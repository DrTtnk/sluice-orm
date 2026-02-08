/**
 * Hardcore runtime and type tests merged
 * Tests edge cases, complex type inference, and stress testing the type system
 * Originally from hardcore.test-d.ts (type-only) + hardcore.test.ts (runtime)
 */
import { Schema as S } from "@effect/schema";
import {
  $addFields,
  $count,
  $facet,
  $group,
  $limit,
  $match,
  $project,
  $setWindowFields,
  $sort,
  $sortByCount,
  $unset,
  registry,
} from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { ComplexMonsterSchema as MonsterSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";
type Monster = typeof MonsterSchema.Type;

const monster = registry("8.0", { monsters: MonsterSchema });

const m1Id = new ObjectId("507f1f77bcf86cd799439011");
const m2Id = new ObjectId("507f1f77bcf86cd799439012");
const m3Id = new ObjectId("507f1f77bcf86cd799439013");

describe("Hardcore Runtime & Type Tests", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;

    // Seed data
    await monster(db)
      .monsters.insertMany([
        {
          _id: m1Id,
          name: "Blob",
          score: 10,
          active: true,
          createdAt: new Date("2023-01-01"),
          deletedAt: null,
          legacyScore: 50,
          status: "draft",
          level: 1,
          metadata: {
            version: "v1",
            flags: [true],
            counts: {
              views: 100,
              likes: 10,
              shares: 0,
            },
            audit: null,
          },
          tags: ["slime", "green"],
          scores: [10, 20, 15],
          items: [
            {
              id: "i1",
              name: "goo",
              price: 10,
              quantity: 5,
              discounts: [],
            },
          ],
          coords: [0, 0],
        },
        {
          _id: m2Id,
          name: "Dragon",
          score: 90,
          active: true,
          createdAt: new Date("2023-01-01"),
          deletedAt: null,
          legacyScore: null,
          status: "published",
          level: 5,
          metadata: {
            version: "v1",
            flags: [true, false],
            counts: {
              views: 5000,
              likes: 1000,
              shares: 500,
            },
            audit: null,
          },
          tags: ["fire", "flying"],
          scores: [90, 95, 88],
          items: [
            {
              id: "i2",
              name: "gold",
              price: 1000,
              quantity: 10,
              discounts: [
                {
                  code: "SAVE10",
                  percent: 10,
                  validUntil: new Date(),
                },
              ],
            },
          ],
          coords: [100, 100],
        },
        {
          _id: m3Id,
          name: "Ghost",
          score: 40,
          active: false,
          createdAt: new Date("2022-01-01"),
          deletedAt: new Date("2023-01-01"),
          legacyScore: 20,
          status: "archived",
          level: 3,
          metadata: {
            version: "v2",
            flags: [],
            counts: {
              views: 50,
              likes: 5,
              shares: 1,
            },
            audit: null,
          },
          tags: ["undead"],
          scores: [40, 35, 42],
          items: [],
          coords: [10, 10],
        },
      ])
      .execute();
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should use switch operator with complex branching", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      score: S.Number,
      active: S.Boolean,
      createdAt: S.Date,
      deletedAt: S.NullOr(S.Date),
      legacyScore: S.NullOr(S.Number),
      description: S.optional(S.String),
      priority: S.optional(S.Number),
      status: S.Literal("draft", "published", "archived"),
      level: S.Literal(1, 2, 3, 4, 5),
      metadata: S.Struct({
        version: S.String,
        flags: S.Array(S.Boolean),
        counts: S.Struct({
          views: S.Number,
          likes: S.Number,
          shares: S.Number,
        }),
        audit: S.NullOr(
          S.Struct({
            lastModifiedBy: S.String,
            lastModifiedAt: S.Date,
          }),
        ),
      }),
      tags: S.Array(S.String),
      scores: S.Array(S.Number),
      items: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
          price: S.Number,
          quantity: S.Number,
          discounts: S.Array(
            S.Struct({
              code: S.String,
              percent: S.Number,
              validUntil: S.Date,
            }),
          ),
        }),
      ),
      coords: S.Tuple(S.Number, S.Number),
      scoreLabel: S.Literal("low", "medium", "high", "excellent"),
    });

    const results = await monster(db)
      .monsters.aggregate(
        $addFields($ => ({
          scoreLabel: $.switch({
            branches: [
              {
                case: $.lte("$score", 20),
                then: "low",
              },
              {
                case: $.lte("$score", 50),
                then: "medium",
              },
              {
                case: $.lte("$score", 80),
                then: "high",
              },
            ],
            default: "excellent",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    expect(results.find(m => m._id.toString() === m1Id.toString())?.scoreLabel).toBe("low");
    expect(results.find(m => m._id.toString() === m2Id.toString())?.scoreLabel).toBe("excellent");
    expect(results.find(m => m._id.toString() === m3Id.toString())?.scoreLabel).toBe("medium");
  });

  it("should project nested fields and use array operators", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      version: S.String,
      viewCount: S.Number,
      likeCount: S.Number,
      firstTag: S.String,
      itemNames: S.Array(S.String),
    });

    const results = await monster(db)
      .monsters.aggregate(
        $project($ => ({
          _id: 1,
          version: "$metadata.version",
          viewCount: "$metadata.counts.views",
          likeCount: "$metadata.counts.likes",
          firstTag: $.arrayElemAt("$tags", 0),
          itemNames: $.map({
            input: "$items",
            in: "$$this.name",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    const m1 = results.find(m => m._id.toString() === m1Id.toString());
    expect(m1).toMatchObject({
      version: "v1",
      viewCount: 100,
      likeCount: 10,
      firstTag: "slime",
      itemNames: ["goo"],
    });
  });

  it("should handle nullable fields with ifNull operator", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      score: S.Number,
      active: S.Boolean,
      createdAt: S.Date,
      deletedAt: S.NullOr(S.Date),
      legacyScore: S.NullOr(S.Number),
      description: S.optional(S.String),
      priority: S.optional(S.Number),
      status: S.Literal("draft", "published", "archived"),
      level: S.Literal(1, 2, 3, 4, 5),
      metadata: S.Struct({
        version: S.String,
        flags: S.Array(S.Boolean),
        counts: S.Struct({
          views: S.Number,
          likes: S.Number,
          shares: S.Number,
        }),
        audit: S.NullOr(
          S.Struct({
            lastModifiedBy: S.String,
            lastModifiedAt: S.Date,
          }),
        ),
      }),
      tags: S.Array(S.String),
      scores: S.Array(S.Number),
      items: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
          price: S.Number,
          quantity: S.Number,
          discounts: S.Array(
            S.Struct({
              code: S.String,
              percent: S.Number,
              validUntil: S.Date,
            }),
          ),
        }),
      ),
      coords: S.Tuple(S.Number, S.Number),
      hasLegacyScore: S.Boolean,
      isDeleted: S.Boolean,
      safeScore: S.Number,
    });

    const results = await monster(db)
      .monsters.aggregate(
        $addFields($ => ({
          hasLegacyScore: $.ne("$legacyScore", null),
          isDeleted: $.ne("$deletedAt", null),
          safeScore: $.ifNull("$legacyScore", 0),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    const m2 = results.find(m => m._id.toString() === m2Id.toString());
    expect(m2?.hasLegacyScore).toBe(false);
    expect(m2?.isDeleted).toBe(false);
    expect(m2?.safeScore).toBe(0);

    const m3 = results.find(m => m._id.toString() === m3Id.toString());
    expect(m3?.hasLegacyScore).toBe(true);
    expect(m3?.isDeleted).toBe(true);
    expect(m3?.safeScore).toBe(20);
  });

  it("should group by complex object with multiple fields", async () => {
    const ResultSchema = S.Struct({
      _id: S.Struct({
        status: S.String,
        level: S.Number,
        year: S.Number,
      }),
      avgScore: S.NullOr(S.Number),
    });

    const results = await monster(db)
      .monsters.aggregate(
        $group($ => ({
          _id: {
            status: "$status",
            level: "$level",
            year: $.year("$createdAt"),
          },
          avgScore: $.avg("$score"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    expect(results).toHaveLength(3);
    expect(results).toContainEqual({
      _id: {
        status: "draft",
        level: 1,
        year: 2023,
      },
      avgScore: 10,
    });
  });

  it("should use $let for local variables in expressions", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      score: S.Number,
      active: S.Boolean,
      createdAt: S.Date,
      deletedAt: S.NullOr(S.Date),
      legacyScore: S.NullOr(S.Number),
      description: S.optional(S.String),
      priority: S.optional(S.Number),
      status: S.Literal("draft", "published", "archived"),
      level: S.Literal(1, 2, 3, 4, 5),
      metadata: S.Struct({
        version: S.String,
        flags: S.Array(S.Boolean),
        counts: S.Struct({
          views: S.Number,
          likes: S.Number,
          shares: S.Number,
        }),
        audit: S.NullOr(
          S.Struct({
            lastModifiedBy: S.String,
            lastModifiedAt: S.Date,
          }),
        ),
      }),
      tags: S.Array(S.String),
      scores: S.Array(S.Number),
      items: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
          price: S.Number,
          quantity: S.Number,
          discounts: S.Array(
            S.Struct({
              code: S.String,
              percent: S.Number,
              validUntil: S.Date,
            }),
          ),
        }),
      ),
      coords: S.Tuple(S.Number, S.Number),
      itemValue: S.Number,
    });

    const results = await monster(db)
      .monsters.aggregate(
        $addFields($ => ({
          itemValue: $.let({
            vars: {
              totalItems: $.size("$items"),
              totalScore: "$score",
            },
            in: $ => $.multiply("$$totalItems", "$$totalScore"),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    const m1 = results.find(m => m._id.toString() === m1Id.toString());
    expect(m1?.itemValue).toBe(10);
  });

  it("should use $reduce to aggregate arrays", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      score: S.Number,
      active: S.Boolean,
      createdAt: S.Date,
      deletedAt: S.NullOr(S.Date),
      legacyScore: S.NullOr(S.Number),
      description: S.optional(S.String),
      priority: S.optional(S.Number),
      status: S.Literal("draft", "published", "archived"),
      level: S.Literal(1, 2, 3, 4, 5),
      metadata: S.Struct({
        version: S.String,
        flags: S.Array(S.Boolean),
        counts: S.Struct({
          views: S.Number,
          likes: S.Number,
          shares: S.Number,
        }),
        audit: S.NullOr(
          S.Struct({
            lastModifiedBy: S.String,
            lastModifiedAt: S.Date,
          }),
        ),
      }),
      tags: S.Array(S.String),
      scores: S.Array(S.Number),
      items: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
          price: S.Number,
          quantity: S.Number,
          discounts: S.Array(
            S.Struct({
              code: S.String,
              percent: S.Number,
              validUntil: S.Date,
            }),
          ),
        }),
      ),
      coords: S.Tuple(S.Number, S.Number),
      totalItemValue: S.Number,
      allTags: S.String,
    });

    const results = await monster(db)
      .monsters.aggregate(
        $addFields($ => ({
          totalItemValue: $.reduce({
            input: "$items",
            initialValue: 0,
            in: $ => $.add("$$value", $.multiply("$$this.price", "$$this.quantity")),
          }),
          allTags: $.reduce({
            input: "$tags",
            initialValue: "",
            in: $ =>
              $.cond({
                if: $.eq("$$value", ""),
                then: "$$this",
                else: $.concat("$$value", ", ", "$$this"),
              }),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    const m1 = results.find(m => m._id.toString() === m1Id.toString());
    expect(m1?.totalItemValue).toBe(50);
    expect(m1?.allTags).toBe("slime, green");
  });

  it("should chain multiple $addFields stages", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      score: S.Number,
      active: S.Boolean,
      createdAt: S.Date,
      deletedAt: S.NullOr(S.Date),
      legacyScore: S.NullOr(S.Number),
      description: S.optional(S.String),
      priority: S.optional(S.Number),
      status: S.Literal("draft", "published", "archived"),
      level: S.Literal(1, 2, 3, 4, 5),
      metadata: S.Struct({
        version: S.String,
        flags: S.Array(S.Boolean),
        counts: S.Struct({
          views: S.Number,
          likes: S.Number,
          shares: S.Number,
        }),
        audit: S.NullOr(
          S.Struct({
            lastModifiedBy: S.String,
            lastModifiedAt: S.Date,
          }),
        ),
      }),
      tags: S.Array(S.String),
      scores: S.Array(S.Number),
      items: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
          price: S.Number,
          quantity: S.Number,
          discounts: S.Array(
            S.Struct({
              code: S.String,
              percent: S.Number,
              validUntil: S.Date,
            }),
          ),
        }),
      ),
      coords: S.Tuple(S.Number, S.Number),
      itemCount: S.Number,
      viewScore: S.Number,
      scorePerItem: S.Number,
      engagement: S.Number,
    });

    const results = await monster(db)
      .monsters.aggregate(
        $match($ => ({ active: true })),
        $addFields($ => ({
          itemCount: $.size("$items"),
          viewScore: "$metadata.counts.views",
        })),
        $addFields($ => ({
          scorePerItem: $.divide("$score", "$itemCount"),
          engagement: $.add("$viewScore", "$metadata.counts.likes"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    const m1 = results.find(m => m._id.toString() === m1Id.toString());
    expect(m1?.itemCount).toBe(1);
    expect(m1?.scorePerItem).toBe(10);
    expect(m1?.engagement).toBe(110);
  });

  it("should use $facet for multiple parallel pipelines", async () => {
    const ResultSchema = S.Struct({
      byStatus: S.Array(
        S.Struct({
          _id: S.String,
          count: S.Number,
        }),
      ),
      topScorers: S.Array(
        S.Struct({
          _id: ObjectIdSchema,
          name: S.String,
          score: S.Number,
        }),
      ),
      recent: S.Array(
        S.Struct({
          _id: ObjectIdSchema,
          name: S.String,
          createdAt: S.Date,
        }),
      ),
    });

    const results = await monster(db)
      .monsters.aggregate(
        $facet($ => ({
          byStatus: $.pipe(
            $group($ => ({
              _id: "$status",
              count: $.sum(1),
            })),
          ),
          topScorers: $.pipe(
            $match($ => ({ score: { $gt: 80 } })),
            $project($ => ({
              _id: 1,
              name: 1,
              score: 1,
            })),
            $limit(5),
          ),
          recent: $.pipe(
            $sort({ createdAt: -1 }),
            $limit(10),
            $project($ => ({
              _id: 1,
              name: 1,
              createdAt: 1,
            })),
          ),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    expect(results.length).toBeGreaterThan(0);
    const facet = results[0];
    expect(facet?.byStatus).toHaveLength(3);
    expect(facet?.topScorers).toHaveLength(1);
    expect(facet?.topScorers[0]?.name).toBe("Dragon");
  });

  it("should use type coercion operators", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      score: S.Number,
      active: S.Boolean,
      createdAt: S.Date,
      deletedAt: S.NullOr(S.Date),
      legacyScore: S.NullOr(S.Number),
      description: S.optional(S.String),
      priority: S.optional(S.Number),
      status: S.Literal("draft", "published", "archived"),
      level: S.Literal(1, 2, 3, 4, 5),
      metadata: S.Struct({
        version: S.String,
        flags: S.Array(S.Boolean),
        counts: S.Struct({
          views: S.Number,
          likes: S.Number,
          shares: S.Number,
        }),
        audit: S.NullOr(
          S.Struct({
            lastModifiedBy: S.String,
            lastModifiedAt: S.Date,
          }),
        ),
      }),
      tags: S.Array(S.String),
      scores: S.Array(S.Number),
      items: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
          price: S.Number,
          quantity: S.Number,
          discounts: S.Array(
            S.Struct({
              code: S.String,
              percent: S.Number,
              validUntil: S.Date,
            }),
          ),
        }),
      ),
      coords: S.Tuple(S.Number, S.Number),
      scoreAsInt: S.Number,
      scoreAsBool: S.Boolean,
    });

    const results = await monster(db)
      .monsters.aggregate(
        $addFields($ => ({
          scoreAsInt: $.toInt("$score"),
          scoreAsBool: $.toBool("$score"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    const m1 = results.find(m => m._id.toString() === m1Id.toString());
    expect(m1?.scoreAsInt).toBe(10);
    expect(m1?.scoreAsBool).toBe(true);
  });

  it("should use $count to count documents", async () => {
    const ResultSchema = S.Struct({ totalPublished: S.Number });

    const results = await monster(db)
      .monsters.aggregate(
        $match($ => ({ status: "published" })),
        $count("totalPublished"),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    expect(results.length).toBeGreaterThan(0);
    expect(results[0]?.totalPublished).toBe(1);
  });

  it("should use $sortByCount with union field", async () => {
    const ResultSchema = S.Struct({
      _id: S.String,
      count: S.Number,
    });

    const results = await monster(db).monsters.aggregate($sortByCount("$status")).toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    expect(results.find(r => r._id === "published")?.count).toBe(1);
  });

  it("should use $unset to remove fields", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      score: S.Number,
      active: S.Boolean,
      createdAt: S.Date,
      deletedAt: S.NullOr(S.Date),
      legacyScore: S.NullOr(S.Number),
      description: S.optional(S.String),
      priority: S.optional(S.Number),
      status: S.Literal("draft", "published", "archived"),
      level: S.Literal(1, 2, 3, 4, 5),
      tags: S.Array(S.String),
      scores: S.Array(S.Number),
      items: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
          price: S.Number,
          quantity: S.Number,
          discounts: S.Array(
            S.Struct({
              code: S.String,
              percent: S.Number,
              validUntil: S.Date,
            }),
          ),
        }),
      ),
      coords: S.Tuple(S.Number, S.Number),
    });

    const results = await monster(db).monsters.aggregate($unset("metadata")).toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);

    const m1 = results.find(m => m._id.toString() === m1Id.toString());
    expect(m1?.name).toBe("Blob");
    expect((m1 as unknown as { metadata?: unknown }).metadata).toBeUndefined();
  });

  it("should handle complex $setWindowFields with obscure window functions", async () => {
    // Define the MatrixUserDocument schema
    const MatrixUserSchema = S.Struct({
      _id: ObjectIdSchema,
      createdAt: S.Date,
      name: S.String,
      email: S.String,
      age: S.Number,
      tags: S.Array(S.String),
      matrixMetadata: S.Array(S.Array(S.Array(S.Struct({ rows: S.Number })))),
      profile: S.Struct({
        bio: S.String,
        avatar: S.String,
      }),
      orders: S.Array(
        S.Struct({
          orderId: ObjectIdSchema,
          total: S.Number,
          items: S.Array(S.String),
        }),
      ),
    });

    const matrixUsers = registry("8.0", { matrixUsers: MatrixUserSchema });

    // Seed test data
    const user1Id = new ObjectId();
    const user2Id = new ObjectId();
    const user3Id = new ObjectId();

    await matrixUsers(db)
      .matrixUsers.insertMany([
        {
          _id: user1Id,
          createdAt: new Date("2023-01-01"),
          name: "Alice Smith",
          email: "alice@example.com",
          age: 25,
          tags: ["premium", "active"],
          matrixMetadata: [[[{ rows: 10 }]], [[{ rows: 15 }]]],
          profile: {
            bio: "Software engineer",
            avatar: "avatar1.png",
          },
          orders: [
            {
              orderId: new ObjectId(),
              total: 150,
              items: ["item1", "item2", "item3"],
            },
            {
              orderId: new ObjectId(),
              total: 250,
              items: ["item4", "item5"],
            },
          ],
        },
        {
          _id: user2Id,
          createdAt: new Date("2023-01-15"),
          name: "Bob Jones",
          email: "bob@gmail.com",
          age: 35,
          tags: ["regular"],
          matrixMetadata: [[[{ rows: 20 }]], [[{ rows: 25 }]]],
          profile: {
            bio: "Data analyst",
            avatar: "avatar2.png",
          },
          orders: [
            {
              orderId: new ObjectId(),
              total: 300,
              items: ["item6", "item7", "item8", "item9"],
            },
          ],
        },
        {
          _id: user3Id,
          createdAt: new Date("2023-02-01"),
          name: "Carol Wilson",
          email: "carol@yahoo.com",
          age: 75,
          tags: ["senior", "loyal"],
          matrixMetadata: [[[{ rows: 30 }]], [[{ rows: 35 }]]],
          profile: {
            bio: "Retired teacher",
            avatar: "avatar3.png",
          },
          orders: [
            {
              orderId: new ObjectId(),
              total: 100,
              items: ["item10"],
            },
          ],
        },
      ])
      .execute();

    // First test the raw MongoDB query to ensure it works
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("matrixUsers")
      .aggregate([
        {
          $setWindowFields: {
            // PARTITION: Instead of a simple field, we create a dynamic
            // partition key using a switch-case on email domains and age buckets.
            partitionBy: {
              $concat: [
                { $substr: ["$email", 0, { $indexOfBytes: ["$email", "@"] }] },
                "-",
                {
                  $switch: {
                    branches: [
                      {
                        case: { $lt: ["$age", 18] },
                        then: "minor",
                      },
                      {
                        case: { $lt: ["$age", 65] },
                        then: "adult",
                      },
                    ],
                    default: "senior",
                  },
                },
              ],
            },

            // SORT: Simple sort by createdAt for derivative compatibility
            sortBy: { createdAt: 1 },

            output: {
              // 1. OBSCURE OPERATOR: $locf (Last Observation Carried Forward)
              // If 'profile.avatar' is null/missing, it grabs the last non-null value
              // within the partition.
              filledAvatar: { $locf: "$profile.avatar" },

              // 2. NESTED PATH VELOCITY: $derivative
              // Calculates the rate of change of the 'rows' value in the first
              // nested matrix element relative to time (unit: week).
              matrixGrowthVelocity: {
                $derivative: {
                  input: {
                    $getField: {
                      field: "rows",
                      input: {
                        $arrayElemAt: [
                          { $arrayElemAt: [{ $arrayElemAt: ["$matrixMetadata", 0] }, 0] },
                          0,
                        ],
                      },
                    },
                  },
                  unit: "week",
                },
                window: { documents: [-1, "current"] },
              },

              // 3. AREA UNDER THE CURVE: $integral
              // Calculates the integral of the order total.
              economicMomentum: {
                $integral: {
                  input: { $avg: "$orders.total" },
                  unit: "day",
                },
                window: {
                  range: [-30, "current"],
                  unit: "day",
                },
              },

              // 4. RANK based on order totals
              itemDensityRank: { $rank: {} },
            },
          },
        },
        // Final stage to clean up the mess we just made
        {
          $project: {
            name: 1,
            partition: {
              $concat: [
                { $substr: ["$email", 0, { $indexOfBytes: ["$email", "@"] }] },
                "-",
                {
                  $switch: {
                    branches: [
                      {
                        case: { $lt: ["$age", 18] },
                        then: "minor",
                      },
                      {
                        case: { $lt: ["$age", 65] },
                        then: "adult",
                      },
                    ],
                    default: "senior",
                  },
                },
              ],
            },
            subDomain: {
              $substr: ["$email", { $add: [{ $indexOfBytes: ["$email", "@"] }, 1] }, -1],
            },
            matrixGrowthVelocity: 1,
            economicMomentum: 1,
            filledAvatar: 1,
            itemDensityRank: 1,
            isHighValue: { $gt: ["$economicMomentum", 1000] },
          },
        },
      ])
      .toArray();

    console.log("Raw MongoDB query results:", JSON.stringify(rawResults, null, 2));

    // Verify that the raw query works and produces expected results
    expect(rawResults).toBeDefined();
    expect(Array.isArray(rawResults)).toBe(true);
    expect(rawResults.length).toBe(3);

    // Check that each result has the expected fields
    for (const result of rawResults) {
      expect(result).toHaveProperty("name");
      expect(result).toHaveProperty("partition");
      expect(result).toHaveProperty("matrixGrowthVelocity");
      expect(result).toHaveProperty("economicMomentum");
      expect(result).toHaveProperty("filledAvatar");
      expect(result).toHaveProperty("itemDensityRank");
      expect(result).toHaveProperty("isHighValue");
      expect(typeof result.isHighValue).toBe("boolean");
    }

    // Define expected result schema
    const _WindowResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      partition: S.String,
      subDomain: S.String,
      matrixGrowthVelocity: S.NullOr(S.Number),
      economicMomentum: S.Number,
      filledAvatar: S.String,
      itemDensityRank: S.Number,
      isHighValue: S.Boolean,
    });

    const WindowResultSchema = S.Array(_WindowResultSchema);

    // Validate the schema matches
    const parsedResults = S.decodeUnknownSync(WindowResultSchema)(rawResults);
    expect(parsedResults).toHaveLength(3);

    const sluiceResults = await matrixUsers(db)
      .matrixUsers.aggregate(
        $setWindowFields($ => ({
          partitionBy: $.concat(
            $.substr("$email", 0, $.indexOfBytes("$email", "@")),
            "-",
            $.switch({
              branches: [
                {
                  case: $.lt("$age", 18),
                  then: "minor",
                },
                {
                  case: $.lt("$age", 65),
                  then: "adult",
                },
              ],
              default: "senior",
            }),
          ),
          sortBy: { createdAt: 1 },
          output: {
            filledAvatar: $.locf("$profile.avatar"),
            matrixGrowthVelocity: $.derivative({
              input: $.getField({
                field: "rows",
                input: $.arrayElemAt($.arrayElemAt($.arrayElemAt("$matrixMetadata", 0), 0), 0),
              }),
              unit: "week",
              window: { documents: [-1, "current"] },
            }),
            economicMomentum: $.integral({
              input: $.avg("$orders.total"),
              unit: "day",
              window: {
                range: [-30, "current"],
                unit: "day",
              },
            }),
            itemDensityRank: $.rank(),
          },
        })),
        $project($ => ({
          name: 1,
          partition: $.concat(
            $.substr("$email", 0, $.indexOfBytes("$email", "@")),
            "-",
            $.switch({
              branches: [
                {
                  case: $.lt("$age", 18),
                  then: "minor",
                },
                {
                  case: $.lt("$age", 65),
                  then: "adult",
                },
              ],
              default: "senior",
            }),
          ),
          subDomain: $.substr("$email", $.add($.indexOfBytes("$email", "@"), 1), -1),
          matrixGrowthVelocity: 1,
          economicMomentum: 1,
          filledAvatar: 1,
          itemDensityRank: 1,
          isHighValue: $.gt("$economicMomentum", 1000),
        })),
      )
      .toList();

    assertSync(WindowResultSchema, sluiceResults);
    expect(sluiceResults).toHaveLength(3);
    expect(sluiceResults).toEqual(parsedResults);
  });
});
