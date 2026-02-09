/**
 * Documentation Examples Validation
 *
 * This file exists ONLY to prove that all code examples used in the
 * documentation site compile correctly against the real Sluice API.
 *
 * These are compile-time tests — they validate types, not runtime behavior.
 * Each section corresponds to a documentation page.
 */
import { Schema as S } from "@effect/schema";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, it } from "vitest";

import {
  $addFields,
  $facet,
  $group,
  $limit,
  $lookup,
  $match,
  $project,
  $set,
  $sort,
  $sortByCount,
  $unset,
  $unwind,
  migrate,
  registry,
} from "../src/index.js";
import { setup, teardown } from "./utils/setup.js";

// ============================================================
// Schemas used across documentation examples
// ============================================================

const UserSchema = S.Struct({
  _id: S.String,
  name: S.String,
  email: S.String,
  age: S.Number,
  department: S.String,
  createdAt: S.Date,
});

const OrderSchema = S.Struct({
  _id: S.String,
  userId: S.String,
  amount: S.Number,
  items: S.Array(
    S.Struct({
      productId: S.String,
      name: S.String,
      price: S.Number,
      quantity: S.Number,
      category: S.String,
    }),
  ),
  status: S.Literal("pending", "paid", "shipped", "delivered"),
  createdAt: S.Date,
});

const ObjectIdSchema = S.instanceOf(ObjectId);

const MonsterSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  score: S.Number,
  active: S.Boolean,
  level: S.Number,
  hp: S.Number,
  attack: S.Number,
  defense: S.Number,
  deletedAt: S.NullOr(S.Date),
  legacyScore: S.NullOr(S.Number),
  status: S.Literal("draft", "published", "archived"),
  metadata: S.Struct({
    version: S.String,
    counts: S.Struct({
      views: S.Number,
      likes: S.Number,
    }),
  }),
  tags: S.Array(S.String),
  scores: S.Array(S.Number),
  items: S.Array(
    S.Struct({
      name: S.String,
      price: S.Number,
      quantity: S.Number,
      rarity: S.Literal("common", "rare", "epic", "legendary"),
      discounts: S.Array(
        S.Struct({
          code: S.String,
          percent: S.Number,
        }),
      ),
    }),
  ),
});

const EventSchema = S.Struct({
  _id: S.String,
  timestamp: S.Date,
  payload: S.Union(
    S.Struct({ type: S.Literal("click"), elementId: S.String }),
    S.Struct({ type: S.Literal("purchase"), amount: S.Number }),
    S.Struct({ type: S.Literal("pageview"), url: S.String }),
  ),
});

// ============================================================
// Registry setup
// ============================================================

const dbRegistry = registry("8.0", {
  users: UserSchema,
  orders: OrderSchema,
  monsters: MonsterSchema,
  events: EventSchema,
});

describe("Documentation Examples — Type & Runtime Validation", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
  });

  afterAll(() => teardown());

  // ========================================
  // intro.md — Quick Example
  // ========================================
  describe("intro.md examples", () => {
    it("quick example — $match → $group → $sort", async () => {
      const bound = dbRegistry(db);

      const result = await bound.users
        .aggregate(
          $match($ => ({ age: { $gte: 18 } })),
          $group($ => ({
            _id: "$department",
            avgAge: $.avg("$age"),
            headcount: $.sum(1),
          })),
          $sort({ headcount: -1 }),
        )
        .toList();

      expectType<{ _id: string; avgAge: number | null; headcount: number }[]>(result);
    });
  });

  // ========================================
  // quick-start.md examples
  // ========================================
  describe("quick-start.md examples", () => {
    it("top spenders pipeline", async () => {
      const bound = dbRegistry(db);

      const topSpenders = await bound.orders
        .aggregate(
          $group($ => ({
            _id: "$userId",
            totalSpent: $.sum("$amount"),
            orderCount: $.sum(1),
          })),
          $match($ => ({ totalSpent: { $gt: 100 } })),
          $sort({ totalSpent: -1 }),
          $project($ => ({
            userId: "$_id",
            totalSpent: 1,
            orderCount: 1,
            _id: 0,
          })),
        )
        .toList();

      expectType<{ userId: string; totalSpent: number; orderCount: number }[]>(topSpenders);
    });
  });

  // ========================================
  // advanced-typings.md — Pipeline Type Flow
  // ========================================
  describe("advanced-typings.md examples", () => {
    it("5-stage category report pipeline", async () => {
      const bound = dbRegistry(db);

      const categoryReport = await bound.orders
        .aggregate(
          $match($ => ({ status: "paid" })),
          $unwind("$items"),
          $group($ => ({
            _id: "$items.category",
            revenue: $.sum($.multiply("$items.price", "$items.quantity")),
            unitsSold: $.sum("$items.quantity"),
            orderCount: $.sum(1),
          })),
          $addFields($ => ({
            avgRevenuePerOrder: $.divide("$revenue", "$orderCount"),
          })),
          $sort({ revenue: -1 }),
        )
        .toList();

      expectType<
        {
          _id: string;
          revenue: number;
          unitsSold: number;
          orderCount: number;
          avgRevenuePerOrder: number;
        }[]
      >(categoryReport);
    });

    it("conditional expressions: $cond, $switch, $ifNull", async () => {
      const bound = dbRegistry(db);

      await bound.monsters
        .aggregate(
          $addFields($ => ({
            tier: $.cond({
              if: $.gte("$score", 1000),
              then: "premium",
              else: "standard",
            }),
            bracket: $.cond({
              if: $.lt("$score", 30),
              then: "low",
              else: $.cond({
                if: $.lt("$score", 70),
                then: "mid",
                else: "high",
              }),
            }),
            rating: $.switch({
              branches: [
                { case: $.lte("$score", 20), then: "poor" },
                { case: $.lte("$score", 50), then: "average" },
                { case: $.lte("$score", 80), then: "good" },
              ],
              default: "excellent",
            }),
            safeScore: $.ifNull("$legacyScore", 0),
          })),
        )
        .toList();
    });

    it("$map — transform array elements", async () => {
      const bound = dbRegistry(db);

      await bound.monsters
        .aggregate(
          $project($ => ({
            name: "$name",
            itemNames: $.map({
              input: "$items",
              as: "item",
              in: "$$item.name",
            }),
            itemTotals: $.map({
              input: "$items",
              as: "item",
              in: $ => $.multiply("$$item.price", "$$item.quantity"),
            }),
          })),
        )
        .toList();
    });

    it("$filter — type-safe array filtering", async () => {
      const bound = dbRegistry(db);

      await bound.monsters
        .aggregate(
          $project($ => ({
            name: "$name",
            expensiveItems: $.filter({
              input: "$items",
              as: "item",
              cond: $ => $.gte("$$item.price", 100),
            }),
            epicItems: $.filter({
              input: "$items",
              as: "item",
              cond: $ => $.in("$$item.rarity", ["epic", "legendary"]),
            }),
          })),
        )
        .toList();
    });

    it("$reduce — fold arrays with type tracking", async () => {
      const bound = dbRegistry(db);

      await bound.monsters
        .aggregate(
          $project($ => ({
            name: "$name",
            totalQuantity: $.reduce({
              input: "$items",
              initialValue: 0,
              in: $ => $.add("$$value", "$$this.quantity"),
            }),
            allRarities: $.reduce({
              input: $.map({ input: "$items", as: "item", in: "$$item.rarity" }),
              initialValue: [] as string[],
              in: $ => $.concatArrays("$$value", ["$$this"]),
            }),
          })),
        )
        .toList();
    });

    it("$facet — multi-branch analysis", async () => {
      const bound = dbRegistry(db);

      const analysis = await bound.monsters
        .aggregate(
          $facet($ => ({
            byLevel: $.pipe(
              $addFields($ => ({
                bracket: $.cond({
                  if: $.lt("$level", 10),
                  then: "low",
                  else: $.cond({
                    if: $.lt("$level", 30),
                    then: "mid",
                    else: "high",
                  }),
                }),
              })),
              $group($ => ({
                _id: "$bracket",
                count: $.sum(1),
                avgAttack: $.avg("$attack"),
              })),
            ),
            byTags: $.pipe($unwind("$tags"), $sortByCount("$tags")),
            stats: $.pipe(
              $group($ => ({
                _id: null,
                totalMonsters: $.sum(1),
                avgLevel: $.avg("$level"),
                maxHp: $.max("$hp"),
              })),
            ),
          })),
        )
        .toList();

      // Validate we can access each facet branch
      const first = analysis[0];
      if (first) {
        expectType<{ _id: string; count: number; avgAttack: number | null }[]>(first.byLevel);
        expectType<{ _id: string; count: number }[]>(first.byTags);
      }
    });

    it("accumulator expressions in $group", async () => {
      const bound = dbRegistry(db);

      await bound.monsters
        .aggregate(
          $group($ => ({
            _id: "$status",
            headcount: $.sum(1),
            avgScore: $.avg("$score"),
            maxScore: $.max("$score"),
            minScore: $.min("$score"),
            newestName: $.last("$name"),
            oldestName: $.first("$name"),
            allNames: $.push("$name"),
            uniqueTags: $.addToSet("$name"),
            employees: $.push({
              name: "$name",
              score: "$score",
              level: "$level",
            }),
            totalAdjusted: $.sum($.add("$score", $.ifNull("$legacyScore", 0))),
          })),
        )
        .toList();
    });

    it("$lookup with sub-pipeline", async () => {
      const bound = dbRegistry(db);

      await bound.users
        .aggregate(
          $lookup({
            from: bound.orders,
            localField: "_id",
            foreignField: "userId",
            as: "recentOrders",
            pipeline: $ =>
              $.pipe(
                $match($ => ({
                  status: { $in: ["paid", "shipped"] },
                })),
                $sort({ createdAt: -1 }),
                $limit(5),
              ),
          }),
        )
        .toList();
    });

    it("deeply nested schema — dot-path access", async () => {
      const bound = dbRegistry(db);

      await bound.monsters
        .aggregate(
          $project($ => ({
            version: "$metadata.version",
            viewCount: "$metadata.counts.views",
            firstTag: $.arrayElemAt("$tags", 0),
            safeScore: $.ifNull("$legacyScore", 0),
            itemNames: $.map({
              input: "$items",
              as: "item",
              in: "$$item.name",
            }),
          })),
        )
        .toList();
    });

    it("$project — inclusion, exclusion, reshaping", async () => {
      const bound = dbRegistry(db);

      await bound.monsters
        .aggregate(
          $project($ => ({
            name: 1,
            score: 1,
            _id: 0,
            scoreName: "$name",
            doubleScore: $.multiply("$score", 2),
            isActive: $.eq("$active", true),
            tagCount: $.size("$tags"),
            topScore: $.max("$scores"),
            filteredScores: $.filter({
              input: "$scores",
              cond: $ => $.gte("$$this", 80),
            }),
          })),
        )
        .toList();
    });

    it("union types — conditional access", async () => {
      const bound = dbRegistry(db);

      const summary = await bound.events
        .aggregate(
          $group($ => ({
            _id: "$payload.type",
            count: $.sum(1),
            totalRevenue: $.sum(
              $.cond({
                if: $.eq("$payload.type", "purchase"),
                then: "$payload.amount",
                else: 0,
              }),
            ),
          })),
          $sort({ count: -1 }),
        )
        .toList();

      expectType<{ _id: string; count: number; totalRevenue: number }[]>(summary);
    });
  });

  // ========================================
  // advanced-typings.md — Type Narrowing Tricks
  // ========================================
  describe("advanced-typings.md — narrowing tricks", () => {
    it("$.switch produces union of literal types", async () => {
      const bound = dbRegistry(db);

      const result = await bound.monsters
        .aggregate(
          $project($ => ({
            _id: 1,
            ageBand: $.switch({
              branches: [
                { case: $.lt("$score", 18), then: "minor" },
                { case: $.lt("$score", 65), then: "adult" },
              ],
              default: "senior",
            }),
          })),
        )
        .toList();

      expectType<{ _id: ObjectId; ageBand: "minor" | "adult" | "senior" }[]>(result);
    });

    it("$.cond produces union of branch types", async () => {
      const bound = dbRegistry(db);

      const result = await bound.monsters
        .aggregate(
          $addFields($ => ({
            tier: $.cond({
              if: $.gte("$score", 1000),
              then: "premium",
              else: "standard",
            }),
          })),
        )
        .toList();

      // tier is "premium" | "standard", not string
      if (result[0]) {
        expectType<"premium" | "standard">(result[0].tier);
      }
    });

    it("$.concat produces template literal types", async () => {
      const bound = dbRegistry(db);

      const result = await bound.monsters
        .aggregate(
          $project($ => ({
            greeting: $.concat("Hello, ", "$name"),
          })),
        )
        .toList();

      // Result type should be template literal `Hello, ${string}`
      if (result[0]) {
        expectType<`Hello, ${string}`>(result[0].greeting);
      }
    });

    it("$.mergeObjects with overlapping keys", async () => {
      const bound = dbRegistry(db);

      await bound.monsters
        .aggregate(
          $addFields($ => ({
            merged: $.mergeObjects("$metadata", { extra: "value" as const }),
          })),
        )
        .toList();
    });

    it("$sortArray preserves element types", async () => {
      const bound = dbRegistry(db);

      await bound.monsters
        .aggregate(
          $project($ => ({
            sortedScores: $.sortArray({ input: "$scores", sortBy: 1 }),
            sortedItems: $.sortArray({ input: "$items", sortBy: { price: -1 } }),
          })),
        )
        .toList();
    });
  });

  // ========================================
  // advanced-typings.md — Autocomplete
  // ========================================
  describe("advanced-typings.md — autocomplete constraints", () => {
    it("$.multiply only accepts numeric fields", async () => {
      const bound = dbRegistry(db);

      await bound.monsters
        .aggregate(
          $project($ => ({
            doubled: $.multiply("$score", 2),
          })),
        )
        .toList();
    });

    it("$.concat only accepts string fields", async () => {
      const bound = dbRegistry(db);

      await bound.monsters
        .aggregate(
          $project($ => ({
            greeting: $.concat("Hello, ", "$name"),
          })),
        )
        .toList();
    });
  });

  // ========================================
  // crud.md — CRUD Operations
  // ========================================
  describe("crud.md examples", () => {
    it("insertOne and findOne", async () => {
      const bound = dbRegistry(db);
      const id = new ObjectId().toHexString();

      await bound.users.insertOne({
        _id: id,
        name: "Test User",
        email: "test@example.com",
        age: 25,
        department: "Testing",
        createdAt: new Date(),
      }).execute();

      const found = await bound.users.findOne($ => ({ _id: id })).toOne();
      expectType<{ _id: string; name: string; email: string; age: number; department: string; createdAt: Date } | null>(found);

      await bound.users.deleteOne($ => ({ _id: id })).execute();
    });

    it("updateOne with $set and $inc", async () => {
      const bound = dbRegistry(db);
      const id = new ObjectId().toHexString();

      await bound.users.insertOne({
        _id: id, name: "Update Test", email: "u@x.com", age: 20, department: "QA", createdAt: new Date(),
      }).execute();

      await bound.users.updateOne(
        $ => ({ _id: id }),
        { $set: { name: "Updated" }, $inc: { age: 1 } },
      ).execute();

      await bound.users.deleteOne($ => ({ _id: id })).execute();
    });

    it("countDocuments and distinct", async () => {
      const bound = dbRegistry(db);

      const count = await bound.users.countDocuments().execute();
      expectType<number>(count);

      const names = await bound.users.distinct("name").execute();
      expectType<string[]>(names);
    });

    it("findOneAndDelete returns typed doc or null", async () => {
      const bound = dbRegistry(db);
      const id = new ObjectId().toHexString();

      await bound.users.insertOne({
        _id: id, name: "Delete Me", email: "d@x.com", age: 1, department: "None", createdAt: new Date(),
      }).execute();

      const deleted = await bound.users.findOneAndDelete($ => ({ _id: id })).execute();
      expectType<{ _id: string; name: string; email: string; age: number; department: string; createdAt: Date } | null>(deleted);
    });
  });

  // ========================================
  // migration.md — Migration Tool
  // ========================================
  describe("migration.md examples", () => {
    it("migrate<OldType, NewType> — valid migration compiles", () => {
      type OldUser = {
        _id: string;
        name: string;
        age: number;
        legacyField: string;
      };

      type NewUser = {
        _id: string;
        name: string;
        age: number;
        email: string;
      };

      const m = migrate<OldUser, NewUser>();

      // Valid: add email, remove legacyField
      const _migration = m.pipe(
        $set({ email: "unknown@example.com" }),
        $unset("legacyField"),
      );

      // Valid: Using $addFields with expression
      const _migration2 = m.pipe(
        $addFields($ => ({ email: $.concat("$name", "@migrated.com") })),
        $unset("legacyField"),
      );
    });
  });

  // ========================================
  // examples.md — E-commerce
  // ========================================
  describe("examples.md — e-commerce", () => {
    it("top-selling products pipeline", async () => {
      const bound = dbRegistry(db);

      const topProducts = await bound.orders
        .aggregate(
          $match($ => ({ status: "paid" })),
          $unwind("$items"),
          $group($ => ({
            _id: "$items.productId",
            productName: $.first("$items.name"),
            totalRevenue: $.sum($.multiply("$items.price", "$items.quantity")),
            totalSold: $.sum("$items.quantity"),
            orderCount: $.sum(1),
          })),
          $sort({ totalRevenue: -1 }),
          $limit(10),
        )
        .toList();

      expectType<{
        _id: string;
        productName: string;
        totalRevenue: number;
        totalSold: number;
        orderCount: number;
      }[]>(topProducts);
    });

    it("customer lifetime value with $lookup", async () => {
      const bound = dbRegistry(db);

      await bound.orders
        .aggregate(
          $match($ => ({ status: "paid" })),
          $group($ => ({
            _id: "$userId",
            totalSpent: $.sum("$amount"),
            orderCount: $.sum(1),
            avgOrderValue: $.avg("$amount"),
            firstOrder: $.min("$createdAt"),
            lastOrder: $.max("$createdAt"),
          })),
          $lookup({
            from: bound.users,
            localField: "_id",
            foreignField: "_id",
            as: "userInfo",
          }),
          $unwind("$userInfo"),
          $project($ => ({
            customerId: "$_id",
            customerName: "$userInfo.name",
            totalSpent: $.include,
            orderCount: $.include,
            avgOrderValue: $.include,
            customerSince: "$firstOrder",
            lastOrder: $.include,
            _id: $.exclude,
          })),
          $sort({ totalSpent: -1 }),
        )
        .toList();
    });

    it("$facet dashboard", async () => {
      const bound = dbRegistry(db);

      const analysis = await bound.monsters
        .aggregate(
          $facet($ => ({
            byLevel: $.pipe(
              $group($ => ({
                _id: "$level",
                count: $.sum(1),
              })),
              $sort({ count: -1 }),
            ),
            topTags: $.pipe($unwind("$tags"), $sortByCount("$tags"), $limit(10)),
            stats: $.pipe(
              $group($ => ({
                _id: null,
                totalMonsters: $.sum(1),
                avgLevel: $.avg("$level"),
              })),
            ),
          })),
        )
        .toList();

      if (analysis[0]) {
        expectType<{ _id: number; count: number }[]>(analysis[0].byLevel);
        expectType<{ _id: string; count: number }[]>(analysis[0].topTags);
      }
    });
  });

  // ========================================
  // Homepage hero example
  // ========================================
  describe("homepage examples", () => {
    it("hero pipeline example", async () => {
      const bound = dbRegistry(db);

      const report = await bound.orders
        .aggregate(
          $match($ => ({ status: "paid" })),
          $group($ => ({
            _id: "$userId",
            totalSpent: $.sum("$amount"),
            orderCount: $.sum(1),
          })),
          $project($ => ({
            userId: "$_id",
            totalSpent: 1,
            orderCount: 1,
            avgOrder: $.divide("$totalSpent", "$orderCount"),
            _id: 0,
          })),
          $sort({ totalSpent: -1 }),
        )
        .toList();

      expectType<
        { userId: string; totalSpent: number; orderCount: number; avgOrder: number }[]
      >(report);
    });
  });
});
