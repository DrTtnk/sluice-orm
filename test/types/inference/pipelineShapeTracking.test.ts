/**
 * Pipeline shape tracking runtime tests - verify types flow correctly through stages
 */
import { Schema as S } from "@effect/schema";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  $addFields,
  $count,
  $facet,
  $group,
  $limit,
  $lookup,
  $match,
  $project,
  $sort,
  $unwind,
  registry,
} from "../../../src/sluice.js";
import { ObjectIdSchema } from "../../utils/common-schemas.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";

// ============================================
// SCHEMAS
// ============================================

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  email: S.String,
  role: S.Literal("admin", "user", "guest"),
  department: S.String,
  salary: S.Number,
  tags: S.Array(S.String),
  skills: S.Array(
    S.Struct({
      name: S.String,
      level: S.Number,
    }),
  ),
  metadata: S.Struct({
    createdAt: S.Date,
    score: S.Number,
    flags: S.Array(S.Boolean),
  }),
});

const OrderSchema = S.Struct({
  _id: ObjectIdSchema,
  userId: ObjectIdSchema,
  total: S.Number,
  items: S.Array(
    S.Struct({
      product: S.String,
      quantity: S.Number,
      price: S.Number,
    }),
  ),
  status: S.Literal("pending", "shipped", "delivered"),
  createdAt: S.Date,
});

const dbRegistry = registry("8.0", {
  users: UserSchema,
  orders: OrderSchema,
});

describe("Pipeline Shape Tracking Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .users.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          name: "Alice",
          age: 30,
          email: "alice@example.com",
          role: "admin",
          department: "Engineering",
          salary: 120000,
          tags: ["senior", "tech"],
          skills: [
            {
              name: "TypeScript",
              level: 9,
            },
            {
              name: "Python",
              level: 7,
            },
          ],
          metadata: {
            createdAt: new Date("2020-01-15"),
            score: 95.5,
            flags: [true, false, true],
          },
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          name: "Bob",
          age: 25,
          email: "bob@example.com",
          role: "user",
          department: "Engineering",
          salary: 80000,
          tags: ["junior", "backend"],
          skills: [
            {
              name: "Java",
              level: 6,
            },
          ],
          metadata: {
            createdAt: new Date("2021-03-20"),
            score: 78.0,
            flags: [false, false],
          },
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          name: "Charlie",
          age: 35,
          email: "charlie@example.com",
          role: "admin",
          department: "Sales",
          salary: 110000,
          tags: ["manager"],
          skills: [
            {
              name: "Communication",
              level: 10,
            },
          ],
          metadata: {
            createdAt: new Date("2019-07-10"),
            score: 88.0,
            flags: [true],
          },
        },
        {
          _id: new ObjectId("000000000000000000000004"),
          name: "Diana",
          age: 28,
          email: "diana@example.com",
          role: "user",
          department: "Marketing",
          salary: 75000,
          tags: ["creative", "design"],
          skills: [
            {
              name: "Photoshop",
              level: 8,
            },
            {
              name: "Illustrator",
              level: 7,
            },
          ],
          metadata: {
            createdAt: new Date("2020-11-05"),
            score: 82.5,
            flags: [false, true],
          },
        },
        {
          _id: new ObjectId("000000000000000000000005"),
          name: "Eve",
          age: 22,
          email: "eve@example.com",
          role: "guest",
          department: "Intern",
          salary: 40000,
          tags: ["intern"],
          skills: [],
          metadata: {
            createdAt: new Date("2023-01-01"),
            score: 65.0,
            flags: [],
          },
        },
      ])
      .execute();

    await dbRegistry(db)
      .orders.insertMany([
        {
          _id: new ObjectId("000000000000000000000006"),
          userId: new ObjectId("000000000000000000000001"),
          total: 299.99,
          items: [
            {
              product: "Laptop",
              quantity: 1,
              price: 299.99,
            },
          ],
          status: "delivered",
          createdAt: new Date("2023-06-01"),
        },
        {
          _id: new ObjectId("000000000000000000000007"),
          userId: new ObjectId("000000000000000000000001"),
          total: 49.99,
          items: [
            {
              product: "Mouse",
              quantity: 1,
              price: 49.99,
            },
          ],
          status: "shipped",
          createdAt: new Date("2023-08-15"),
        },
        {
          _id: new ObjectId("000000000000000000000008"),
          userId: new ObjectId("000000000000000000000002"),
          total: 149.99,
          items: [
            {
              product: "Keyboard",
              quantity: 1,
              price: 149.99,
            },
          ],
          status: "delivered",
          createdAt: new Date("2023-07-20"),
        },
        {
          _id: new ObjectId("000000000000000000000009"),
          userId: new ObjectId("000000000000000000000003"),
          total: 599.99,
          items: [
            {
              product: "Monitor",
              quantity: 1,
              price: 599.99,
            },
          ],
          status: "pending",
          createdAt: new Date("2023-09-10"),
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  // ============================================
  // TEST 1: Basic Match
  // ============================================
  it("basic match preserves shape", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      age: S.Number,
      email: S.String,
      role: S.Literal("admin", "user", "guest"),
      department: S.String,
      salary: S.Number,
      tags: S.Array(S.String),
      skills: S.Array(
        S.Struct({
          name: S.String,
          level: S.Number,
        }),
      ),
      metadata: S.Struct({
        createdAt: S.Date,
        score: S.Number,
        flags: S.Array(S.Boolean),
      }),
    });

    const results = await dbRegistry(db)
      .users.aggregate($match($ => ({ role: "admin" })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 2: Basic Project
  // ============================================
  it("basic project completely replaces shape", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      fullName: S.String,
      isAdult: S.Boolean,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          _id: 1,
          fullName: "$name",
          isAdult: $.gte("$age", 18),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 3: Basic AddFields
  // ============================================
  it("basic addFields merges new fields into existing shape", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      age: S.Number,
      email: S.String,
      role: S.Literal("admin", "user", "guest"),
      department: S.String,
      salary: S.Number,
      tags: S.Array(S.String),
      skills: S.Array(
        S.Struct({
          name: S.String,
          level: S.Number,
        }),
      ),
      metadata: S.Struct({
        createdAt: S.Date,
        score: S.Number,
        flags: S.Array(S.Boolean),
      }),
      isAdult: S.Boolean,
      lowerName: S.String,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({
          isAdult: $.gte("$age", 18),
          lowerName: $.toLower("$name"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 4: Simple Group By Field
  // ============================================
  it("simple group by field", async () => {
    const ResultSchema = S.Struct({
      _id: S.Literal("admin", "user", "guest"),
      count: S.Number,
      avgAge: S.NullOr(S.Number),
      totalSalary: S.Number,
      users: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $group($ => ({
          _id: "$role",
          count: $.sum(1),
          avgAge: $.avg("$age"),
          totalSalary: $.sum("$salary"),
          users: $.push("$name"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 5: Group By Compound ID
  // ============================================
  it("group by compound _id", async () => {
    const ResultSchema = S.Struct({
      _id: S.Struct({
        department: S.String,
        role: S.Literal("admin", "user", "guest"),
      }),
      count: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $group($ => ({
          _id: {
            department: "$department",
            role: "$role",
          },
          count: $.sum(1),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 6: Group All (null _id)
  // ============================================
  it("group all (null _id)", async () => {
    const ResultSchema = S.Struct({
      _id: S.Null,
      totalUsers: S.Number,
      maxAge: S.Number,
      minAge: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $group($ => ({
          _id: null,
          totalUsers: $.sum(1),
          maxAge: $.max("$age"),
          minAge: $.min("$age"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 7: Unwind Simple Array
  // ============================================
  it("unwind simple array", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      age: S.Number,
      email: S.String,
      role: S.Literal("admin", "user", "guest"),
      department: S.String,
      salary: S.Number,
      tags: S.String,
      skills: S.Array(
        S.Struct({
          name: S.String,
          level: S.Number,
        }),
      ),
      metadata: S.Struct({
        createdAt: S.Date,
        score: S.Number,
        flags: S.Array(S.Boolean),
      }),
    });

    const results = await dbRegistry(db)
      .users.aggregate($unwind({ path: "$tags" }))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 8: Unwind With Options
  // ============================================
  it("unwind with includeArrayIndex and preserveNullAndEmptyArrays", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      age: S.Number,
      email: S.String,
      role: S.Literal("admin", "user", "guest"),
      department: S.String,
      salary: S.Number,
      tags: S.Array(S.String),
      skills: S.optional(S.NullOr(S.Struct({ name: S.String, level: S.Number }))),
      metadata: S.Struct({
        createdAt: S.Date,
        score: S.Number,
        flags: S.Array(S.Boolean),
      }),
      skillIndex: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $unwind({
          path: "$skills",
          includeArrayIndex: "skillIndex",
          preserveNullAndEmptyArrays: true,
        }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 9: Lookup
  // ============================================
  it("lookup adds foreign collection array", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      age: S.Number,
      email: S.String,
      role: S.Literal("admin", "user", "guest"),
      department: S.String,
      salary: S.Number,
      tags: S.Array(S.String),
      skills: S.Array(
        S.Struct({
          name: S.String,
          level: S.Number,
        }),
      ),
      metadata: S.Struct({
        createdAt: S.Date,
        score: S.Number,
        flags: S.Array(S.Boolean),
      }),
      orders: S.Array(OrderSchema),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $lookup({
          from: dbRegistry(db).orders,
          localField: "_id",
          foreignField: "userId",
          as: "orders",
        }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 10: Complex Pipeline 1 (Match -> Group -> Sort -> Limit)
  // ============================================
  it("complex pipeline: match -> group -> sort -> limit", async () => {
    const ResultSchema = S.Struct({
      _id: S.String,
      avgSalary: S.NullOr(S.Number),
      count: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $match($ => ({ role: { $ne: "guest" } })),
        $group($ => ({
          _id: "$department",
          avgSalary: $.avg("$salary"),
          count: $.sum(1),
        })),
        $sort({ avgSalary: -1 }),
        $limit(10),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 11: Complex Pipeline 2 (Lookup -> Unwind -> Group)
  // ============================================
  it("complex pipeline: lookup -> unwind -> group", async () => {
    const ResultSchema = S.Struct({
      _id: S.instanceOf(ObjectId),
      userName: S.String,
      totalSpent: S.Number,
      orderCount: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $lookup({
          from: dbRegistry(db).orders,
          localField: "_id",
          foreignField: "userId",
          as: "orders",
        }),
        $unwind({ path: "$orders" }),
        $group($ => ({
          _id: "$_id",
          userName: $.first("$name"),
          totalSpent: $.sum("$orders.total"),
          orderCount: $.sum(1),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 12: Facet Multi-Pipeline
  // ============================================
  it("facet creates multi-pipeline with different shapes", async () => {
    const ResultSchema = S.Struct({
      byDepartment: S.Array(
        S.Struct({
          _id: S.String,
          count: S.Number,
        }),
      ),
      topEarners: S.Array(
        S.Struct({
          _id: ObjectIdSchema,
          name: S.String,
          salary: S.Number,
        }),
      ),
      statistics: S.Array(
        S.Struct({
          _id: S.Null,
          avgAge: S.NullOr(S.Number),
          avgSalary: S.NullOr(S.Number),
          total: S.Number,
        }),
      ),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $facet($ => ({
          byDepartment: $.pipe(
            $group($ => ({
              _id: "$department",
              count: $.sum(1),
            })),
          ),
          topEarners: $.pipe(
            $sort({ salary: -1 }),
            $limit(5),
            $project($ => ({
              _id: 1,
              name: 1,
              salary: 1,
            })),
          ),
          statistics: $.pipe(
            $group($ => ({
              _id: null,
              avgAge: $.avg("$age"),
              avgSalary: $.avg("$salary"),
              total: $.sum(1),
            })),
          ),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 13: Count Terminal
  // ============================================
  it("count terminal stage", async () => {
    const ResultSchema = S.Struct({ adminCount: S.Number });

    const results = await dbRegistry(db)
      .users.aggregate(
        $match($ => ({ role: "admin" })),
        $count("adminCount"),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // ============================================
  // TEST 14: Field Reference Type Preservation
  // ============================================
  it("addFields correctly infers field reference types", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      age: S.Number,
      email: S.String,
      role: S.Literal("admin", "user", "guest"),
      department: S.String,
      salary: S.Number,
      tags: S.Array(S.String),
      skills: S.Array(
        S.Struct({
          name: S.String,
          level: S.Number,
        }),
      ),
      metadata: S.Struct({
        createdAt: S.Date,
        score: S.Number,
        flags: S.Array(S.Boolean),
      }),
      copiedName: S.String,
      copiedAge: S.Number,
      copiedRole: S.Literal("admin", "user", "guest"),
      copiedTags: S.Array(S.String),
      copiedMetadata: S.Struct({
        createdAt: S.Date,
        score: S.Number,
        flags: S.Array(S.Boolean),
      }),
      nestedScore: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({
          copiedName: "$name",
          copiedAge: "$age",
          copiedRole: "$role",
          copiedTags: "$tags",
          copiedMetadata: "$metadata",
          nestedScore: "$metadata.score",
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
