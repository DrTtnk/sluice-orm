import { Schema as S } from "@effect/schema";
import { $addFields, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  email: S.String,
  isActive: S.Boolean,
  createdAt: S.Date,
  tags: S.Array(S.String),
  scores: S.Array(S.Number),
});

const dbRegistry = registry("8.0", { users: UserSchema });

describe("InputValidation Runtime Tests", () => {
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
          isActive: true,
          createdAt: new Date("2024-01-15"),
          tags: ["developer", "senior"],
          scores: [85, 92, 88],
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should validate numeric field operators", async () => {
    const ResultSchema = S.Struct({
      ...UserSchema.fields,
      validAbs: S.Number,
      validAbsLiteral: S.Number,
      validAdd: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({
          validAbs: $.abs("$age"),
          validAbsLiteral: $.abs(42),
          validAdd: $.add("$age", 10, "$age"),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should validate string field operators", async () => {
    const ResultSchema = S.Struct({
      ...UserSchema.fields,
      validConcat: S.String,
      validToLower: S.String,
      validStrLen: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({
          validConcat: $.concat("$name", " - ", "$email"),
          validToLower: $.toLower("hello"),
          validStrLen: $.strLenBytes("$name"),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should validate array field operators", async () => {
    const ResultSchema = S.Struct({
      ...UserSchema.fields,
      validSize: S.Number,
      validSlice: S.Array(S.String),
      validReverse: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({
          validSize: $.size("$tags"),
          validSlice: $.slice("$tags", 2),
          validReverse: $.reverseArray("$scores"),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should validate date field operators", async () => {
    const ResultSchema = S.Struct({
      ...UserSchema.fields,
      validYear: S.Number,
      validDayOfMonth: S.Number,
    });

    const results = await dbRegistry(db)
      .users.aggregate(
        $addFields($ => ({
          validYear: $.year("$createdAt"),
          validDayOfMonth: $.dayOfMonth("$createdAt"),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
