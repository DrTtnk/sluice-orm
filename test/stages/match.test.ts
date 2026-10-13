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

const NestedDocumentSchema = S.Struct({
  _id: ObjectIdSchema,
  user: S.Struct({
    name: S.String,
    age: S.Number,
  }),
  scores: S.Array(S.Number),
  metadata: S.Struct({
    createdAt: S.Date,
    tags: S.Array(S.String),
  }),
});

const ComplexDocumentSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  orders: S.Array(
    S.Struct({
      orderId: S.String,
      amount: S.Number,
      status: S.Literal("pending", "shipped", "delivered"),
    }),
  ),
  reviews: S.Array(
    S.Struct({
      rating: S.Number,
      comment: S.String,
      verified: S.Boolean,
    }),
  ),
});

const dbRegistry = registry("8.0", {
  articles: ArticleSchema,
  budgets: MonthlyBudgetSchema,
  orders: OrderWithStatusSchema,
  nestedDocs: NestedDocumentSchema,
  complexDocs: ComplexDocumentSchema,
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

    await dbRegistry(db)
      .nestedDocs.insertMany([
        {
          _id: new ObjectId("000000000000000000000008"),
          user: { name: "John Doe", age: 30 },
          scores: [85, 90, 88],
          metadata: { createdAt: new Date("2023-01-01"), tags: ["active", "premium"] },
        },
        {
          _id: new ObjectId("000000000000000000000009"),
          user: { name: "Jane Smith", age: 25 },
          scores: [92, 95],
          metadata: { createdAt: new Date("2023-02-01"), tags: ["active"] },
        },
        {
          _id: new ObjectId("00000000000000000000000a"),
          user: { name: "Bob Johnson", age: 35 },
          scores: [78, 82, 80, 85],
          metadata: { createdAt: new Date("2023-03-01"), tags: ["inactive"] },
        },
      ])
      .execute();

    await dbRegistry(db)
      .complexDocs.insertMany([
        {
          _id: new ObjectId("00000000000000000000000b"),
          name: "Alice Cooper",
          orders: [
            { orderId: "ORD001", amount: 150, status: "delivered" },
            { orderId: "ORD002", amount: 75, status: "shipped" },
          ],
          reviews: [
            { rating: 5, comment: "Excellent service!", verified: true },
            { rating: 4, comment: "Good product", verified: true },
          ],
        },
        {
          _id: new ObjectId("00000000000000000000000c"),
          name: "Bob Wilson",
          orders: [{ orderId: "ORD003", amount: 200, status: "pending" }],
          reviews: [{ rating: 3, comment: "Average experience", verified: false }],
        },
        {
          _id: new ObjectId("00000000000000000000000d"),
          name: "Charlie Brown",
          orders: [
            { orderId: "ORD004", amount: 50, status: "delivered" },
            { orderId: "ORD005", amount: 120, status: "shipped" },
            { orderId: "ORD006", amount: 300, status: "pending" },
          ],
          reviews: [
            { rating: 5, comment: "Outstanding!", verified: true },
            { rating: 2, comment: "Poor quality", verified: true },
          ],
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

  it("should match dotted path in nested object", async () => {
    const ResultSchema = NestedDocumentSchema;

    const results = await dbRegistry(db)
      .nestedDocs.aggregate(
        $match(() => ({ "user.name": "John Doe" })), // Type assertion needed due to current type limitations
      )
      .toList();

    expect(results.length).toBe(1);
    expect(results[0]?.user.name).toBe("John Doe");
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match dotted path in array element", async () => {
    const ResultSchema = NestedDocumentSchema;

    const results = await dbRegistry(db)
      .nestedDocs.aggregate(
        $match(() => ({ "scores.0": { $gte: 85 } })), // Type assertion needed due to current type limitations
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match dotted path in deeply nested object", async () => {
    const ResultSchema = NestedDocumentSchema;

    const results = await dbRegistry(db)
      .nestedDocs.aggregate(
        $match(() => ({ "metadata.tags": "active" })), // Type assertion needed due to current type limitations
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $elemMatch on array of objects", async () => {
    const ResultSchema = ComplexDocumentSchema;

    const results = await dbRegistry(db)
      .complexDocs.aggregate(
        $match($ => ({
          orders: { $elemMatch: { status: "delivered", amount: { $gt: 100 } } },
        })),
      )
      .toList();

    expect(results.length).toBe(1); // Only Alice Cooper has a delivered order > $100
    expect(results[0]?.name).toBe("Alice Cooper");
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $elemMatch with complex conditions", async () => {
    const ResultSchema = ComplexDocumentSchema;

    const results = await dbRegistry(db)
      .complexDocs.aggregate(
        $match($ => ({
          reviews: { $elemMatch: { rating: { $gte: 4 }, verified: true } },
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0); // Alice and Charlie have verified reviews with rating >= 4
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $elemMatch with nested object conditions", async () => {
    const ResultSchema = ComplexDocumentSchema;

    const results = await dbRegistry(db)
      .complexDocs.aggregate(
        $match($ => ({
          orders: { $elemMatch: { amount: { $lt: 100 }, status: "shipped" } },
        })),
      )
      .toList();

    expect(results.length).toBe(1); // Only Alice has a shipped order < $100
    expect(results[0]?.name).toBe("Alice Cooper");
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should match $elemMatch on primitive arrays", async () => {
    const ResultSchema = NestedDocumentSchema;

    const results = await dbRegistry(db)
      .nestedDocs.aggregate(
        $match($ => ({
          scores: { $elemMatch: { $gt: 90 } },
        })),
      )
      .toList();

    expect(results.length).toBe(1); // Only Jane Smith has scores > 90
    expect(results[0]?.user.name).toBe("Jane Smith");
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
