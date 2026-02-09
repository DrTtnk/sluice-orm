/**
 * Comprehensive Field Validation Tests - Runtime
 *
 * Runtime validation counterpart to fieldValidation.test-d.ts
 * Tests that invalid field references fail at runtime with proper error messages.
 */
import { Schema as S } from "@effect/schema";
import { $addFields, $group, $limit, $match, $project, $sort, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  email: S.String,
  active: S.Boolean,
  role: S.Literal("admin", "user", "guest"),
  address: S.Struct({
    city: S.String,
    zip: S.Number,
    country: S.optional(S.String),
  }),
  scores: S.Array(S.Number),
  tags: S.Array(S.String),
  friends: S.Array(
    S.Struct({
      id: S.String,
      name: S.String,
    }),
  ),
  metadata: S.Struct({
    created: S.Date,
    lastLogin: S.Date,
    visits: S.Number,
    profile: S.Struct({
      bio: S.String,
      avatar: S.optional(S.String),
      social: S.Struct({
        twitter: S.optional(S.String),
        github: S.optional(S.String),
      }),
    }),
  }),
});

const dbRegistry = registry("8.0", { users: UserSchema });

describe("Field Validation - Runtime", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .users.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          name: "Alice Smith",
          age: 30,
          email: "alice@example.com",
          active: true,
          role: "admin",
          address: {
            city: "New York",
            zip: 10001,
            country: "USA",
          },
          scores: [85, 92, 78, 95],
          tags: ["developer", "team-lead", "mentor"],
          friends: [
            {
              id: "user2",
              name: "Bob Jones",
            },
            {
              id: "user3",
              name: "Carol White",
            },
          ],
          metadata: {
            created: new Date("2023-01-15"),
            lastLogin: new Date("2025-01-20"),
            visits: 245,
            profile: {
              bio: "Senior software engineer with 10 years experience",
              avatar: "avatar1.jpg",
              social: {
                twitter: "@alice_codes",
                github: "alice-dev",
              },
            },
          },
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          name: "Bob Jones",
          age: 25,
          email: "bob@example.com",
          active: true,
          role: "user",
          address: {
            city: "San Francisco",
            zip: 94102,
            country: "USA",
          },
          scores: [88, 90, 85],
          tags: ["developer", "frontend"],
          friends: [
            {
              id: "user1",
              name: "Alice Smith",
            },
          ],
          metadata: {
            created: new Date("2023-06-20"),
            lastLogin: new Date("2025-01-25"),
            visits: 180,
            profile: {
              bio: "Frontend specialist",
              social: { github: "bob-frontend" },
            },
          },
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          name: "Carol White",
          age: 28,
          email: "carol@example.com",
          active: false,
          role: "guest",
          address: {
            city: "Austin",
            zip: 73301,
          },
          scores: [75, 80, 82, 78, 85],
          tags: ["designer", "ux"],
          friends: [],
          metadata: {
            created: new Date("2023-03-10"),
            lastLogin: new Date("2024-12-15"),
            visits: 50,
            profile: {
              bio: "UX/UI Designer",
              avatar: "avatar3.jpg",
              social: { twitter: "@carol_design" },
            },
          },
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  // ============================================
  // VALID CASES - Sanity checks
  // ============================================

  it("should project all valid simple fields", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      userName: S.String,
      userAge: S.Number,
      userEmail: S.String,
      isActive: S.Boolean,
      userRole: S.Literal("admin", "user", "guest"),
      userScores: S.Array(S.Number),
      userTags: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          _id: 1,
          userName: "$name",
          userAge: "$age",
          userEmail: "$email",
          isActive: "$active",
          userRole: "$role",
          userScores: "$scores",
          userTags: "$tags",
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should project all valid nested fields", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      city: S.String,
      zip: S.Number,
      country: S.optional(S.String),
      created: S.Date,
      lastLogin: S.Date,
      visits: S.Number,
      bio: S.String,
      avatar: S.optional(S.String),
      twitter: S.optional(S.String),
      github: S.optional(S.String),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          city: "$address.city",
          zip: "$address.zip",
          country: "$address.country",
          created: "$metadata.created",
          lastLogin: "$metadata.lastLogin",
          visits: "$metadata.visits",
          bio: "$metadata.profile.bio",
          avatar: "$metadata.profile.avatar",
          twitter: "$metadata.profile.social.twitter",
          github: "$metadata.profile.social.github",
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(2);
    const first = results[0];
    const second = results[1];
    const third = results[2];
    expect(first).toBeDefined();
    expect(second).toBeDefined();
    expect(third).toBeDefined();
    expect(first?.avatar).toBeDefined();
    expect(second?.avatar).toBeUndefined();
    expect(second?.twitter).toBeUndefined();
    expect(third?.country).toBeUndefined();
    expect(third?.github).toBeUndefined();
  });

  it("should use valid operators with correct types", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      doubled: S.Number,
      summed: S.Number,
      upper: S.String,
      lower: S.String,
      tagCount: S.Number,
      scoreCount: S.Number,
      firstTag: S.NullOr(S.String),
      lastScore: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          doubled: $.multiply("$age", 2),
          summed: $.add("$age", "$metadata.visits"),
          upper: $.toUpper("$name"),
          lower: $.toLower("$email"),
          tagCount: $.size("$tags"),
          scoreCount: $.size("$scores"),
          firstTag: $.first("$tags"),
          lastScore: $.last("$scores"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.doubled).toBe(60); // Alice age 30 * 2
    expect(first?.upper).toBe("ALICE SMITH");
    expect(first?.tagCount).toBe(3);
  });

  it("should track pipeline shape after $group", async () => {
    const ResultSchema = S.Struct({
      _id: S.Literal("admin", "user", "guest"),
      avgAge: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $group($ => ({
          _id: "$role",
          avgAge: $.avg("$age"),
        })),
        $sort({ _id: 1 }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
  });

  it("should track pipeline shape after $group and $project", async () => {
    const ResultSchema = S.Struct({
      _id: S.Literal("admin", "user", "guest"),
      role: S.Literal("admin", "user", "guest"),
      average: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $group($ => ({
          _id: "$role",
          avgAge: $.avg("$age"),
        })),
        $sort({ _id: 1 }),
        $project($ => ({
          role: "$_id",
          average: "$avgAge",
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
  });

  it("should track pipeline shape after $project with inclusion", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      age: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          _id: 1,
          name: 1,
          age: 1,
        })),
        $sort({
          name: 1,
          age: -1,
        }),
        $limit(10),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.name).toBe("Alice Smith");
  });

  it("should handle valid $match with existing field", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($match($ => ({ active: true })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.every(r => r.active)).toBe(true);
  });

  it("should handle valid $match with $expr", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($match($ => ({ $expr: $.gt("$age", 26) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.every(r => r.age > 26)).toBe(true);
  });

  it("should handle valid $group with accumulator", async () => {
    const ResultSchema = S.Struct({
      _id: S.Literal("admin", "user", "guest"),
      total: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $group($ => ({
          _id: "$role",
          total: $.sum("$age"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
  });

  it("should handle valid $sort on existing field", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($sort({ age: 1 }))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(2);
    const first = results[0];
    const second = results[1];
    const third = results[2];
    expect(first).toBeDefined();
    expect(second).toBeDefined();
    expect(third).toBeDefined();
    expect(first?.age).toBe(25);
    expect(second?.age).toBe(28);
    expect(third?.age).toBe(30);
  });

  it("should handle valid $sort with -1 direction", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($sort({ age: -1 }))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(2);
    const first = results[0];
    const second = results[1];
    const third = results[2];
    expect(first).toBeDefined();
    expect(second).toBeDefined();
    expect(third).toBeDefined();
    expect(first?.age).toBe(30);
    expect(second?.age).toBe(28);
    expect(third?.age).toBe(25);
  });

  it("should handle $addFields with valid field reference", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      age: S.Number,
      email: S.String,
      active: S.Boolean,
      role: S.Literal("admin", "user", "guest"),
      address: S.Struct({
        city: S.String,
        zip: S.Number,
        country: S.optional(S.String),
      }),
      scores: S.Array(S.Number),
      tags: S.Array(S.String),
      friends: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
        }),
      ),
      metadata: S.Struct({
        created: S.Date,
        lastLogin: S.Date,
        visits: S.Number,
        profile: S.Struct({
          bio: S.String,
          avatar: S.optional(S.String),
          social: S.Struct({
            twitter: S.optional(S.String),
            github: S.optional(S.String),
          }),
        }),
      }),
      ageDouble: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate($addFields($ => ({ ageDouble: $.multiply("$age", 2) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.ageDouble).toBe(60);
  });

  it("should handle $project with nested field arithmetic", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      visitDoubled: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate($project($ => ({ visitDoubled: $.multiply("$metadata.visits", 2) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?._id).toBeInstanceOf(ObjectId);
    expect(first?.visitDoubled).toBe(490);
  });

  it("should handle $project with string operators on nested fields", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      bioUpper: S.String,
    });

    const results = await dbRegistry(db)
      .users.aggregate($project($ => ({ bioUpper: $.toUpper("$metadata.profile.bio") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.bioUpper).toContain("SENIOR");
  });

  it("should handle $project with array field reference", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      allTags: S.Array(S.String),
      allScores: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          allTags: "$tags",
          allScores: "$scores",
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(Array.isArray(first?.allTags)).toBe(true);
  });

  it("should handle $project with optional nested fields", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      country: S.optional(S.String),
      avatar: S.optional(S.String),
      twitter: S.optional(S.String),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          country: "$address.country",
          avatar: "$metadata.profile.avatar",
          twitter: "$metadata.profile.social.twitter",
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(2);
    const first = results[0];
    const second = results[1];
    const third = results[2];
    expect(first).toBeDefined();
    expect(second).toBeDefined();
    expect(third).toBeDefined();
    expect(first?.country).toBe("USA");
    expect(second?.avatar).toBeUndefined();
    expect(second?.twitter).toBeUndefined();
    expect(third?.country).toBeUndefined();
  });

  it("should handle $group with nested field in _id", async () => {
    const ResultSchema = S.Struct({
      _id: S.String,
      count: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $group($ => ({
          _id: "$address.city",
          count: $.sum(1),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
  });

  it("should handle $group with multiple accumulators", async () => {
    const ResultSchema = S.Struct({
      _id: S.Literal("admin", "user", "guest"),
      avgAge: S.NullOr(S.Number),
      totalVisits: S.Number,
      userCount: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $group($ => ({
          _id: "$role",
          avgAge: $.avg("$age"),
          totalVisits: $.sum("$metadata.visits"),
          userCount: $.sum(1),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
  });

  it("should handle $match with nested field equality", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($match($ => ({ $expr: $.eq("$address.city", "New York") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.address.city).toBe("New York");
  });

  it("should handle $match with comparison operators", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($match($ => ({ age: { $gte: 28 } })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.every(r => r.age >= 28)).toBe(true);
  });

  it("should handle $match with $in operator", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($match($ => ({ role: { $in: ["admin", "user"] } })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.every(r => r.role === "admin" || r.role === "user")).toBe(true);
  });

  it("should handle $project with multiple operators", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      nameLower: S.String,
      ageDoubled: S.Number,
      tagCount: S.Number,
      cityUpper: S.String,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          nameLower: $.toLower("$name"),
          ageDoubled: $.multiply("$age", 2),
          tagCount: $.size("$tags"),
          cityUpper: $.toUpper("$address.city"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.nameLower).toBe("alice smith");
    expect(first?.cityUpper).toBe("NEW YORK");
  });

  it("should handle $project with $.add on multiple fields", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      totalActivity: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate($project($ => ({ totalActivity: $.add("$age", "$metadata.visits") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.totalActivity).toBe(275); // 30 + 245
  });

  it("should handle $project with $.concat on string fields", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      fullContact: S.String,
    });

    const results = await dbRegistry(db)
      .users.aggregate($project($ => ({ fullContact: $.concat("$name", " - ", "$email") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.fullContact).toBe("Alice Smith - alice@example.com");
  });

  it("should handle $project with $.size on array fields", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      tagCount: S.Number,
      scoreCount: S.Number,
      friendCount: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          tagCount: $.size("$tags"),
          scoreCount: $.size("$scores"),
          friendCount: $.size("$friends"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.tagCount).toBe(3);
    expect(first?.scoreCount).toBe(4);
  });

  it("should handle $project with $.first and $.last on arrays", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      firstTag: S.NullOr(S.String),
      lastTag: S.NullOr(S.String),
      firstScore: S.NullOr(S.Number),
      lastScore: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          firstTag: $.first("$tags"),
          lastTag: $.last("$tags"),
          firstScore: $.first("$scores"),
          lastScore: $.last("$scores"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.firstTag).toBe("developer");
    expect(first?.lastScore).toBe(95);
  });

  it("should handle complex pipeline with multiple stages", async () => {
    const ResultSchema = S.Struct({
      _id: S.Literal("admin", "user", "guest"),
      role: S.Literal("admin", "user", "guest"),
      avgAge: S.NullOr(S.Number),
      totalUsers: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $match($ => ({ active: true })),
        $group($ => ({
          _id: "$role",
          avgAge: $.avg("$age"),
          totalUsers: $.sum(1),
        })),
        $sort({ _id: 1 }),
        $project($ => ({
          role: "$_id",
          avgAge: "$avgAge",
          totalUsers: "$totalUsers",
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
  });

  it("should handle $addFields with complex expression", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      age: S.Number,
      email: S.String,
      active: S.Boolean,
      role: S.Literal("admin", "user", "guest"),
      address: S.Struct({
        city: S.String,
        zip: S.Number,
        country: S.optional(S.String),
      }),
      scores: S.Array(S.Number),
      tags: S.Array(S.String),
      friends: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
        }),
      ),
      metadata: S.Struct({
        created: S.Date,
        lastLogin: S.Date,
        visits: S.Number,
        profile: S.Struct({
          bio: S.String,
          avatar: S.optional(S.String),
          social: S.Struct({
            twitter: S.optional(S.String),
            github: S.optional(S.String),
          }),
        }),
      }),
      activityScore: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({ activityScore: $.add($.multiply("$age", 2), "$metadata.visits") })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.activityScore).toBe(305); // (30 * 2) + 245
  });

  it("should handle $group with max and min accumulators", async () => {
    const ResultSchema = S.Struct({
      _id: S.Literal("admin", "user", "guest"),
      maxAge: S.Number,
      minAge: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $group($ => ({
          _id: "$role",
          maxAge: $.max("$age"),
          minAge: $.min("$age"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
  });

  it("should handle $sort with multiple fields", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate(
        $sort({
          active: -1,
          age: 1,
        }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.active).toBe(true);
    expect(first?.age).toBe(25);
  });

  it("should handle $limit after $sort", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($sort({ age: -1 }), $limit(2))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(1);
    const first = results[0];
    const second = results[1];
    expect(first).toBeDefined();
    expect(second).toBeDefined();
    expect(first?.age).toBe(30);
    expect(second?.age).toBe(28);
  });

  it("should handle $project with inclusion after $addFields", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      doubled: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({ doubled: $.multiply("$age", 2) })),
        $project($ => ({
          name: 1,
          doubled: 1,
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.doubled).toBe(60);
  });

  it("should handle $match with $expr using nested fields", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($match($ => ({ $expr: $.gt("$metadata.visits", 100) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.every(r => r.metadata.visits > 100)).toBe(true);
  });

  it("should handle $match with $expr using eq", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($match($ => ({ $expr: $.eq("$active", true) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.every(r => r.active)).toBe(true);
  });

  it("should handle $project with nested object destructuring", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      city: S.String,
      zip: S.Number,
      bio: S.String,
      visits: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          city: "$address.city",
          zip: "$address.zip",
          bio: "$metadata.profile.bio",
          visits: "$metadata.visits",
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.city).toBe("New York");
  });

  it("should handle $group with deeply nested field in accumulator", async () => {
    const ResultSchema = S.Struct({
      _id: S.String,
      totalVisits: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $group($ => ({
          _id: "$address.city",
          totalVisits: $.sum("$metadata.visits"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
  });

  it("should handle pipeline with $match, $addFields, $project, $sort", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      userName: S.String,
      userAge: S.Number,
      score: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $match($ => ({ active: true })),
        $addFields($ => ({ score: $.add("$age", "$metadata.visits") })),
        $project($ => ({
          userName: "$name",
          userAge: "$age",
          score: "$score",
        })),
        $sort({ score: -1 }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(1);
    const first = results[0];
    const second = results[1];
    expect(first).toBeDefined();
    expect(second).toBeDefined();
    expect(first?.score).toBeDefined();
    expect(second?.score).toBeDefined();
    expect(first?.score).toBeGreaterThan(second?.score ?? -Infinity);
  });

  it("should handle $project with all array operators", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      tagCount: S.Number,
      firstTag: S.NullOr(S.String),
      lastTag: S.NullOr(S.String),
      scoreCount: S.Number,
      firstScore: S.NullOr(S.Number),
      lastScore: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $project($ => ({
          tagCount: $.size("$tags"),
          firstTag: $.first("$tags"),
          lastTag: $.last("$tags"),
          scoreCount: $.size("$scores"),
          firstScore: $.first("$scores"),
          lastScore: $.last("$scores"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.tagCount).toBe(3);
    expect(first?.firstTag).toBe("developer");
  });

  it("should handle $addFields with string operator on nested field", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      age: S.Number,
      email: S.String,
      active: S.Boolean,
      role: S.Literal("admin", "user", "guest"),
      address: S.Struct({
        city: S.String,
        zip: S.Number,
        country: S.optional(S.String),
      }),
      scores: S.Array(S.Number),
      tags: S.Array(S.String),
      friends: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
        }),
      ),
      metadata: S.Struct({
        created: S.Date,
        lastLogin: S.Date,
        visits: S.Number,
        profile: S.Struct({
          bio: S.String,
          avatar: S.optional(S.String),
          social: S.Struct({
            twitter: S.optional(S.String),
            github: S.optional(S.String),
          }),
        }),
      }),
      cityUppercase: S.String,
    });

    const results = await dbRegistry(db)
      .users.aggregate($addFields($ => ({ cityUppercase: $.toUpper("$address.city") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first).toBeDefined();
    expect(first?.cityUppercase).toBe("NEW YORK");
  });

  it("should validate end-to-end complex real-world pattern", async () => {
    const ResultSchema = S.Struct({
      _id: S.Literal("admin", "user", "guest"),
      userRole: S.Literal("admin", "user", "guest"),
      activeCount: S.Number,
      avgAge: S.NullOr(S.Number),
      totalVisits: S.Number,
      avgScoreCount: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({ scoreCount: $.size("$scores") })),
        $group($ => ({
          _id: "$role",
          activeCount: $.sum(1),
          avgAge: $.avg("$age"),
          totalVisits: $.sum("$metadata.visits"),
          avgScoreCount: $.avg("$scoreCount"),
        })),
        $project($ => ({
          userRole: "$_id",
          activeCount: "$activeCount",
          avgAge: "$avgAge",
          totalVisits: "$totalVisits",
          avgScoreCount: "$avgScoreCount",
        })),
        $sort({ totalVisits: -1 }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
  });
});
