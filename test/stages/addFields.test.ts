// Runtime equivalent of addFields.test-d.ts
import { Schema as S } from "@effect/schema";
import { $addFields, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

// ============================================
// TEST DOCUMENT TYPES AND SCHEMAS
// ============================================

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  firstName: S.String,
  lastName: S.String,
  age: S.Number,
  scores: S.Array(S.Number),
  email: S.String,
});

const ScoreSchema = S.Struct({
  _id: ObjectIdSchema,
  student: S.String,
  homework: S.Array(S.Number),
  quiz: S.Array(S.Number),
  extraCredit: S.Number,
});

const VehicleSchema = S.Struct({
  _id: ObjectIdSchema,
  make: S.String,
  model: S.String,
  specs: S.Struct({
    horsepower: S.Number,
    weight: S.Number,
  }),
});

const EventSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  date: S.Date,
});

describe("AddFields Runtime Tests", () => {
  let db: Db;

  const dbRegistry = registry("8.0", {
    users: UserSchema,
    scores: ScoreSchema,
    vehicles: VehicleSchema,
    events: EventSchema,
  });

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    // Seed Users
    await dbRegistry(db)
      .users.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          firstName: "John",
          lastName: "Doe",
          age: 25,
          scores: [85, 90, 88],
          email: "john@example.com",
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          firstName: "Jane",
          lastName: "Smith",
          age: 17,
          scores: [95, 92, 98],
          email: "jane@example.com",
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          firstName: "Bob",
          lastName: "Johnson",
          age: 70,
          scores: [78, 82, 80],
          email: "bob@example.com",
        },
      ])
      .execute();

    // Seed Scores
    await dbRegistry(db)
      .scores.insertMany([
        {
          _id: new ObjectId("000000000000000000000004"),
          student: "John Doe",
          homework: [85, 90],
          quiz: [88, 92],
          extraCredit: 5,
        },
        {
          _id: new ObjectId("000000000000000000000005"),
          student: "Jane Smith",
          homework: [95, 98],
          quiz: [92, 96],
          extraCredit: 3,
        },
      ])
      .execute();

    // Seed Vehicles
    await dbRegistry(db)
      .vehicles.insertMany([
        {
          _id: new ObjectId("000000000000000000000006"),
          make: "Toyota",
          model: "Camry",
          specs: {
            horsepower: 200,
            weight: 3200,
          },
        },
        {
          _id: new ObjectId("000000000000000000000007"),
          make: "Honda",
          model: "Civic",
          specs: {
            horsepower: 180,
            weight: 2900,
          },
        },
      ])
      .execute();

    // Seed Events
    await dbRegistry(db)
      .events.insertMany([
        {
          _id: new ObjectId("000000000000000000000008"),
          name: "Conference",
          date: new Date("2023-10-01"),
        },
        {
          _id: new ObjectId("000000000000000000000009"),
          name: "Workshop",
          date: new Date("2023-11-15"),
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should add computed fields using expression builder", async () => {
    const ResultSchema = S.Struct({
      ...UserSchema.fields,
      fullName: S.String,
      isAdult: S.Boolean,
      hasHighScore: S.Boolean,
      scoreSum: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({
          fullName: $.concat("$firstName", " ", "$lastName"),
          isAdult: $.gte("$age", 18),
          hasHighScore: $.gte($.max("$scores"), 90),
          scoreSum: $.sum("$scores"),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should copy fields using simple field refs", async () => {
    const ResultSchema = S.Struct({
      ...UserSchema.fields,
      nameCopy: S.String,
      ageCopy: S.Number,
      scoresCopy: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({
          nameCopy: "$firstName",
          ageCopy: "$age",
          scoresCopy: "$scores",
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should handle conditional logic with $cond", async () => {
    const ResultSchema = S.Struct({
      ...UserSchema.fields,
      ageGroup: S.String,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({
          ageGroup: $.cond({
            if: $.lt("$age", 18),
            then: "minor",
            else: $.cond({
              if: $.lt("$age", 65),
              then: "adult",
              else: "senior",
            }),
          }),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
