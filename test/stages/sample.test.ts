// Runtime equivalent of sample.test-d.ts
import { Schema as S } from "@effect/schema";
import { $match, $sample, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  email: S.String,
  registered: S.Boolean,
});

type User = typeof UserSchema.Type;

const dbRegistry = registry("8.0", { users: UserSchema });

describe("Sample Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .users.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          name: "Alice",
          email: "alice@example.com",
          registered: true,
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          name: "Bob",
          email: "bob@example.com",
          registered: false,
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          name: "Charlie",
          email: "charlie@example.com",
          registered: true,
        },
        {
          _id: new ObjectId("000000000000000000000004"),
          name: "Diana",
          email: "diana@example.com",
          registered: true,
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should sample basic documents", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($sample({ size: 3 }))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should sample after match", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate(
        $match($ => ({ registered: true })),
        $sample({ size: 5 }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should sample with large size", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($sample({ size: 100 }))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should sample single document", async () => {
    const ResultSchema = UserSchema;

    const results = await dbRegistry(db)
      .users.aggregate($sample({ size: 1 }))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
