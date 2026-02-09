/**
 * Union Objects Integration Tests
 *
 * Explores type behavior when schemas contain union objects (discriminated unions).
 * MongoDB often stores polymorphic documents, this tests how sluice handles them.
 *
 * KEY FINDINGS:
 * - Sluice does NOT support discriminant narrowing ($match on type does NOT change downstream types)
 * - Union types are flattened - all fields from all variants are accessible via $.path
 * - Array fields with union elements can be unwound and grouped normally
 * - Conditional access via $ifNull, $switch, $cond is the safe pattern for variant-specific fields
 * - Dotted paths in $match filters need explicit typing or use raw MongoDB filters
 */

import { Schema as S } from "@effect/schema";
import { $addFields, $group, $match, $project, $unwind, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

// Discriminated union: different event types with different payloads
const ClickEventPayload = S.Struct({
  type: S.Literal("click"),
  elementId: S.String,
  coordinates: S.Struct({
    x: S.Number,
    y: S.Number,
  }),
});

const PageViewPayload = S.Struct({
  type: S.Literal("pageview"),
  url: S.String,
  referrer: S.NullOr(S.String),
});

const PurchasePayload = S.Struct({
  type: S.Literal("purchase"),
  productId: S.String,
  amount: S.Number,
  currency: S.String,
});

const EventPayload = S.Union(ClickEventPayload, PageViewPayload, PurchasePayload);

const AnalyticsEventSchema = S.Struct({
  _id: ObjectIdSchema,
  userId: S.String,
  timestamp: S.Date,
  payload: EventPayload,
  metadata: S.optional(
    S.Struct({
      userAgent: S.String,
      ip: S.String,
    }),
  ),
});

// Schema with array of union objects
const UserActivitySchema = S.Struct({
  _id: ObjectIdSchema,
  userId: S.String,
  events: S.Array(EventPayload),
  stats: S.Struct({
    totalClicks: S.Number,
    totalPurchases: S.Number,
    totalPageViews: S.Number,
  }),
});

// Schema with nested union (union within union)
const NotificationTargetEmail = S.Struct({
  channel: S.Literal("email"),
  address: S.String,
  verified: S.Boolean,
});

const NotificationTargetSMS = S.Struct({
  channel: S.Literal("sms"),
  phone: S.String,
  countryCode: S.String,
});

const NotificationTargetPush = S.Struct({
  channel: S.Literal("push"),
  deviceToken: S.String,
  platform: S.Literal("ios", "android"),
});

const NotificationTarget = S.Union(
  NotificationTargetEmail,
  NotificationTargetSMS,
  NotificationTargetPush,
);

const NotificationSchema = S.Struct({
  _id: ObjectIdSchema,
  recipientId: S.String,
  targets: S.Array(NotificationTarget),
  message: S.String,
  sent: S.Boolean,
});

const dbRegistry = registry("8.0", {
  analyticsEvents: AnalyticsEventSchema,
  userActivities: UserActivitySchema,
  notifications: NotificationSchema,
});

describe("Union Objects Integration Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    // Seed analytics events
    await dbRegistry(db)
      .analyticsEvents.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          userId: "user1",
          timestamp: new Date("2024-01-15T10:00:00Z"),
          payload: {
            type: "click",
            elementId: "btn-submit",
            coordinates: { x: 100, y: 200 },
          },
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          userId: "user1",
          timestamp: new Date("2024-01-15T10:01:00Z"),
          payload: {
            type: "pageview",
            url: "/products",
            referrer: "/home",
          },
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          userId: "user1",
          timestamp: new Date("2024-01-15T10:02:00Z"),
          payload: {
            type: "purchase",
            productId: "prod-123",
            amount: 99.99,
            currency: "USD",
          },
        },
        {
          _id: new ObjectId("000000000000000000000004"),
          userId: "user2",
          timestamp: new Date("2024-01-15T11:00:00Z"),
          payload: {
            type: "click",
            elementId: "nav-home",
            coordinates: { x: 50, y: 30 },
          },
        },
      ])
      .execute();

    // Seed user activities
    await dbRegistry(db)
      .userActivities.insertMany([
        {
          _id: new ObjectId("000000000000000000000010"),
          userId: "user1",
          events: [
            { type: "click", elementId: "btn-1", coordinates: { x: 10, y: 20 } },
            { type: "pageview", url: "/page1", referrer: null },
            { type: "purchase", productId: "p1", amount: 50, currency: "EUR" },
          ],
          stats: { totalClicks: 1, totalPurchases: 1, totalPageViews: 1 },
        },
      ])
      .execute();

    // Seed notifications
    await dbRegistry(db)
      .notifications.insertMany([
        {
          _id: new ObjectId("000000000000000000000020"),
          recipientId: "user1",
          targets: [
            { channel: "email", address: "user1@example.com", verified: true },
            { channel: "push", deviceToken: "token-123", platform: "ios" },
          ],
          message: "Hello!",
          sent: false,
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  describe("Basic union field access", () => {
    it("should match on discriminant via $expr", async () => {
      // Match using $expr to access nested fields in unions
      const result = await dbRegistry(db)
        .analyticsEvents.aggregate(
          $match($ => ({ $expr: $.eq("$payload.type", "click") })),
          $project(() => ({
            _id: 0,
            userId: 1,
            payloadType: "$payload.type",
          })),
        )
        .toList();

      expect(result).toHaveLength(2);
      expect(result.every(r => r.payloadType === "click")).toBe(true);
    });

    it("should access common fields across union variants via $group", async () => {
      // All variants have 'type' field - accessible via string path
      const result = await dbRegistry(db)
        .analyticsEvents.aggregate(
          $group($ => ({
            _id: "$payload.type",
            count: $.sum(1),
          })),
        )
        .toList();

      expect(result).toHaveLength(3);

      const [first] = [...result].sort((a, b) => a._id.localeCompare(b._id));
      expect(first?._id).toBe("click");
      expect(first?.count).toBe(2);
    });

    it("should handle variant-specific fields with $ifNull", async () => {
      // Use $ifNull to safely access fields that may not exist on all variants
      const result = await dbRegistry(db)
        .analyticsEvents.aggregate(
          $match($ => ({ $expr: $.eq("$payload.type", "purchase") })),
          $addFields($ => ({
            // $ifNull handles missing fields safely
            purchaseAmount: $.ifNull("$payload.amount", 0),
          })),
          $project(() => ({
            _id: 0,
            purchaseAmount: 1,
          })),
        )
        .toList();

      expect(result).toHaveLength(1);
      expect(result[0]?.purchaseAmount).toBe(99.99);
    });
  });

  describe("Array of union objects", () => {
    it("should unwind array of unions", async () => {
      const result = await dbRegistry(db)
        .userActivities.aggregate(
          $match(() => ({ userId: "user1" })),
          $unwind("$events"),
          $project(() => ({
            _id: 0,
            eventType: "$events.type",
          })),
        )
        .toList();

      expect(result).toHaveLength(3);
      expect(result.map(r => r.eventType).sort()).toEqual(["click", "pageview", "purchase"]);
    });

    it("should group by union discriminant after unwind", async () => {
      const result = await dbRegistry(db)
        .userActivities.aggregate(
          $unwind("$events"),
          $group($ => ({
            _id: "$events.type",
            count: $.sum(1),
          })),
        )
        .toList();

      expect(result).toHaveLength(3);
      expect(result.find(r => r._id === "click")?.count).toBe(1);
    });
  });

  describe("Complex union scenarios", () => {
    it("should handle notification targets (nested union)", async () => {
      const result = await dbRegistry(db)
        .notifications.aggregate(
          $unwind("$targets"),
          $group($ => ({
            _id: "$targets.channel",
            count: $.sum(1),
          })),
        )
        .toList();

      expect(result).toHaveLength(2);
      expect(result.find(r => r._id === "email")).toBeDefined();
      expect(result.find(r => r._id === "push")).toBeDefined();
    });

    it("should project union-specific fields based on discriminant match", async () => {
      const result = await dbRegistry(db)
        .notifications.aggregate(
          $unwind("$targets"),
          $match($ => ({ $expr: $.eq("$targets.channel", "email") })),
          $project(() => ({
            _id: 0,
            channel: "$targets.channel",
            // address exists only on email variant but is accessed via string path
            address: "$targets.address",
          })),
        )
        .toList();

      expect(result).toHaveLength(1);
      expect(result[0]?.address).toBe("user1@example.com");
    });

    it("should compute across all union variants using $cond", async () => {
      // Count total events by type across all analytics using conditional aggregation
      const result = await dbRegistry(db)
        .analyticsEvents.aggregate(
          $group($ => ({
            _id: null,
            clicks: $.sum($.cond({ if: $.eq("$payload.type", "click"), then: 1, else: 0 })),
            pageviews: $.sum($.cond({ if: $.eq("$payload.type", "pageview"), then: 1, else: 0 })),
            purchases: $.sum($.cond({ if: $.eq("$payload.type", "purchase"), then: 1, else: 0 })),
          })),
          $project(() => ({
            _id: 0,
            clicks: 1,
            pageviews: 1,
            purchases: 1,
          })),
        )
        .toList();

      expect(result[0]).toEqual({
        clicks: 2,
        pageviews: 1,
        purchases: 1,
      });
    });
  });

  describe("Type inference with unions", () => {
    it("should infer union type in projection", async () => {
      const result = await dbRegistry(db)
        .analyticsEvents.aggregate(
          $project(() => ({
            _id: 0,
            payload: 1,
          })),
        )
        .toList();

      // Type should be the full union
      type PayloadType = (typeof result)[number]["payload"];
      expectType<
        | { type: "click"; elementId: string; coordinates: { x: number; y: number } }
        | { type: "pageview"; url: string; referrer: string | null }
        | { type: "purchase"; productId: string; amount: number; currency: string }
      >(result[0]?.payload ?? (null as never));

      expect(result).toHaveLength(4);
    });

    it("should preserve union in $switch expression", async () => {
      const result = await dbRegistry(db)
        .analyticsEvents.aggregate(
          $addFields($ => ({
            eventCategory: $.switch({
              branches: [
                { case: $.eq("$payload.type", "click"), then: "interaction" },
                { case: $.eq("$payload.type", "pageview"), then: "navigation" },
                { case: $.eq("$payload.type", "purchase"), then: "conversion" },
              ],
              default: "unknown",
            }),
          })),
          $project(() => ({
            _id: 0,
            eventCategory: 1,
          })),
        )
        .toList();

      expect(result.map(r => r.eventCategory).sort()).toEqual([
        "conversion",
        "interaction",
        "interaction",
        "navigation",
      ]);
    });
  });
});
