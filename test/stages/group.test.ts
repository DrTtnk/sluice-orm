// Runtime equivalent of group.test-d.ts
import { Schema as S } from "@effect/schema";
import { $group, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const SaleSchema = S.Struct({
  _id: ObjectIdSchema,
  item: S.String,
  price: S.Number,
  quantity: S.Number,
  date: S.Date,
});

const OrderSchema = S.Struct({
  _id: ObjectIdSchema,
  customerId: S.String,
  total: S.Number,
  status: S.Literal("pending", "shipped", "delivered"),
});

type Sale = typeof SaleSchema.Type;
type Order = typeof OrderSchema.Type;

const dbRegistry = registry("8.0", {
  sales: SaleSchema,
  orders: OrderSchema,
});

describe("Group Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .sales.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          item: "Widget",
          price: 10,
          quantity: 5,
          date: new Date("2024-01-01"),
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          item: "Gadget",
          price: 20,
          quantity: 3,
          date: new Date("2024-01-02"),
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          item: "Widget",
          price: 10,
          quantity: 2,
          date: new Date("2024-01-03"),
        },
      ])
      .execute();

    await dbRegistry(db)
      .orders.insertMany([
        {
          _id: new ObjectId("000000000000000000000004"),
          customerId: "c1",
          total: 100,
          status: "shipped",
        },
        {
          _id: new ObjectId("000000000000000000000005"),
          customerId: "c1",
          total: 200,
          status: "delivered",
        },
        {
          _id: new ObjectId("000000000000000000000006"),
          customerId: "c2",
          total: 150,
          status: "pending",
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should group by null", async () => {
    const ResultSchema = S.Struct({
      _id: S.Null,
      count: S.Number,
    });

    const results = await dbRegistry(db)
      .sales.aggregate(
        $group($ => ({
          _id: null,
          count: $.sum(1),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should group by field", async () => {
    const ResultSchema = S.Struct({
      _id: S.String,
      count: S.Number,
    });

    const results = await dbRegistry(db)
      .sales.aggregate(
        $group($ => ({
          _id: "$item",
          count: $.sum(1),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should group with multiple accumulators", async () => {
    const ResultSchema = S.Struct({
      _id: S.Null,
      totalRevenue: S.Number,
      avgRevenue: S.NullOr(S.Number),
      orderCount: S.Number,
    });

    const results = await dbRegistry(db)
      .orders.aggregate(
        $group($ => ({
          _id: null,
          totalRevenue: $.sum("$total"),
          avgRevenue: $.avg("$total"),
          orderCount: $.sum(1),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should group with $push accumulator", async () => {
    const ResultSchema = S.Struct({
      _id: S.String,
      totals: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .orders.aggregate(
        $group($ => ({
          _id: "$customerId",
          totals: $.push("$total"),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should group with $addToSet accumulator", async () => {
    const ResultSchema = S.Struct({
      _id: S.String,
      statuses: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .orders.aggregate(
        $group($ => ({
          _id: "$customerId",
          statuses: $.addToSet("$status"),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
