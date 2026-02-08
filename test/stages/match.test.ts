// Runtime equivalent of match.test-d.ts
import { Schema as S } from "@effect/schema";
import { $match, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const ArticleSchema = S.Struct({
  _id: ObjectIdSchema,
  author: S.String,
  score: S.Number,
  views: S.Number,
  title: S.String,
  tags: S.Array(S.String),
});

const MonthlyBudgetSchema = S.Struct({
  _id: ObjectIdSchema,
  month: S.String,
  budget: S.Number,
  spent: S.Number,
});

const OrderWithStatusSchema = S.Struct({
  _id: ObjectIdSchema,
  orderId: S.String,
  status: S.Literal("pending", "shipped", "delivered"),
  priority: S.Literal(1, 2, 3),
});

const dbRegistry = registry("8.0", {
  articles: ArticleSchema,
  budgets: MonthlyBudgetSchema,
  orders: OrderWithStatusSchema,
});

describe("Match Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .articles.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          author: "dave",
          score: 85,
          views: 500,
          title: "MongoDB Basics",
          tags: ["mongodb", "database"],
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          author: "alice",
          score: 92,
          views: 1200,
          title: "Advanced Queries",
          tags: ["mongodb", "aggregation", "advanced"],
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          author: "bob",
          score: 60,
          views: 300,
          title: "Intro to NoSQL",
          tags: ["nosql"],
        },
      ])
      .execute();

    await dbRegistry(db)
      .budgets.insertMany([
        {
          _id: new ObjectId("000000000000000000000004"),
          month: "Jan",
          budget: 1000,
          spent: 800,
        },
        {
          _id: new ObjectId("000000000000000000000005"),
          month: "Feb",
          budget: 1000,
          spent: 1200,
        },
      ])
      .execute();

    await dbRegistry(db)
      .orders.insertMany([
        {
          _id: new ObjectId("000000000000000000000006"),
          orderId: "ORD1",
          status: "shipped",
          priority: 1,
        },
        {
          _id: new ObjectId("000000000000000000000007"),
          orderId: "ORD2",
          status: "pending",
          priority: 2,
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should match equality filter", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate($match($ => ({ author: "dave" })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match comparison operator", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate($match($ => ({ score: { $gt: 70 } })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $or logical operator", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate(
        $match($ => ({
          $or: [
            {
              score: {
                $gt: 70,
                $lt: 90,
              },
            },
            { views: { $gte: 1000 } },
          ],
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $nor logical operator", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate(
        $match($ => ({
          $nor: [{ author: "dave" }, { views: { $gte: 1000 } }],
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $and logical operator", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate($match($ => ({ $and: [{ author: "dave" }, { score: { $gt: 50 } }] })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $expr with builder", async () => {
    const ResultSchema = MonthlyBudgetSchema;

    const results = await dbRegistry(db)
      .budgets.aggregate($match($ => ({ $expr: $.gt("$spent", "$budget") })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $expr with nested builder expressions", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate($match($ => ({ $expr: $.and($.gt("$score", 50), $.lt("$views", 1000)) })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $mod operator", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate($match($ => ({ score: { $mod: [2, 0] } })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $in operator", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate($match($ => ({ author: { $in: ["dave", "alice", "bob"] } })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $nin operator", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate($match($ => ({ author: { $nin: ["admin", "system"] } })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $exists operator", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate($match($ => ({ author: { $exists: true } })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $regex operator", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate($match($ => ({ author: { $regex: /^d/i } })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $all for array fields", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate($match($ => ({ tags: { $all: ["mongodb", "aggregation"] } })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $size for array fields", async () => {
    const ResultSchema = ArticleSchema;

    const results = await dbRegistry(db)
      .articles.aggregate($match($ => ({ tags: { $size: 3 } })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match union type", async () => {
    const ResultSchema = OrderWithStatusSchema;

    const results = await dbRegistry(db)
      .orders.aggregate($match($ => ({ status: "shipped" })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match numeric union type", async () => {
    const ResultSchema = OrderWithStatusSchema;

    const results = await dbRegistry(db)
      .orders.aggregate($match($ => ({ priority: { $in: [1, 2] } })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
