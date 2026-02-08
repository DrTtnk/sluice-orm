// Runtime equivalent of facet.test-d.ts
import { Schema as S } from "@effect/schema";
import { $count, $facet, $group, $limit, $match, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  department: S.String,
  active: S.Boolean,
  tags: S.Array(S.String),
});

const ArticleSchema = S.Struct({
  _id: ObjectIdSchema,
  title: S.String,
  author: S.String,
  score: S.Number,
  category: S.String,
  date: S.Date,
});

type User = typeof UserSchema.Type;
type Article = typeof ArticleSchema.Type;

const dbRegistry = registry("8.0", {
  users: UserSchema,
  articles: ArticleSchema,
});

describe("Facet Runtime Tests", () => {
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
          department: "Engineering",
          active: true,
          tags: ["tech"],
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          name: "Bob",
          age: 25,
          department: "Sales",
          active: false,
          tags: ["sales"],
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          name: "Charlie",
          age: 17,
          department: "Engineering",
          active: true,
          tags: ["intern"],
        },
      ])
      .execute();

    await dbRegistry(db)
      .articles.insertMany([
        {
          _id: new ObjectId("000000000000000000000004"),
          title: "Article 1",
          author: "Alice",
          score: 85,
          category: "Tech",
          date: new Date("2024-01-01"),
        },
        {
          _id: new ObjectId("000000000000000000000005"),
          title: "Article 2",
          author: "Bob",
          score: 90,
          category: "Business",
          date: new Date("2024-01-02"),
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should facet with multiple pipelines", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({
      activeUsers: S.Array(UserSchema),
      byDepartment: S.Array(
        S.Struct({
          _id: S.String,
          count: S.Number,
        }),
      ),
    });

    // 2 - Perform aggregation
    const results = await dbRegistry(db)
      .users.aggregate(
        $facet($ => ({
          activeUsers: $.pipe(
            $match($ => ({ active: true })),
            $limit(10),
          ),
          byDepartment: $.pipe(
            $group($ => ({
              _id: "$department",
              count: $.sum(1),
            })),
          ),
        })),
      )
      .toList();

    // 3 - Expect non-empty results
    expect(results.length).greaterThan(0);

    // 4 - Runtime validation
    assertSync(S.Array(ResultSchema), results);

    // 5 - Type assertion
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should facet with count pipeline", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({
      total: S.Array(S.Struct({ count: S.Number })),
      adults: S.Array(UserSchema),
    });

    // 2 - Perform aggregation
    const results = await dbRegistry(db)
      .users.aggregate(
        $facet($ => ({
          total: $.pipe($count("count")),
          adults: $.pipe(
            $match($ => ({ age: { $gte: 18 } })),
            $limit(5),
          ),
        })),
      )
      .toList();

    // 3 - Expect non-empty results
    expect(results.length).greaterThan(0);

    // 4 - Runtime validation
    assertSync(S.Array(ResultSchema), results);

    // 5 - Type assertion
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
