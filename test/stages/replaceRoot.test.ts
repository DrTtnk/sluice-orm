// Runtime equivalent of replaceRoot.test-d.ts
import { Schema as S } from "@effect/schema";
import {
  $match,
  $replaceRoot,
  $replaceWith,
  $unwind,
  registry,
} from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const PersonSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  pets: S.Struct({
    dogs: S.optional(S.Number),
    cats: S.optional(S.Number),
    birds: S.optional(S.Number),
    fish: S.optional(S.Number),
  }),
});

const StudentSchema = S.Struct({
  _id: ObjectIdSchema,
  grades: S.Array(
    S.Struct({
      test: S.Number,
      grade: S.Number,
      mean: S.Number,
      std: S.Number,
    }),
  ),
});

const ContactSchema = S.Struct({
  _id: ObjectIdSchema,
  first_name: S.String,
  last_name: S.String,
  city: S.String,
});

const ContactDefaultsSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  email: S.optional(S.String),
  cell: S.optional(S.String),
  home: S.optional(S.String),
});

type Person = typeof PersonSchema.Type;
type Student = typeof StudentSchema.Type;
type Contact = typeof ContactSchema.Type;
type ContactDefaults = typeof ContactDefaultsSchema.Type;

const dbRegistry = registry("8.0", {
  people: PersonSchema,
  students: StudentSchema,
  contacts: ContactSchema,
  contactDefaults: ContactDefaultsSchema,
});

describe("ReplaceRoot Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .people.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          name: "Alice",
          age: 30,
          pets: {
            dogs: 2,
            cats: 1,
          },
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          name: "Bob",
          age: 25,
          pets: { birds: 3 },
        },
      ])
      .execute();

    await dbRegistry(db)
      .students.insertMany([
        {
          _id: new ObjectId("000000000000000000000003"),
          grades: [
            {
              test: 1,
              grade: 85,
              mean: 80,
              std: 5,
            },
            {
              test: 2,
              grade: 92,
              mean: 85,
              std: 6,
            },
          ],
        },
      ])
      .execute();

    await dbRegistry(db)
      .contacts.insertMany([
        {
          _id: new ObjectId("000000000000000000000004"),
          first_name: "John",
          last_name: "Doe",
          city: "NYC",
        },
        {
          _id: new ObjectId("000000000000000000000005"),
          first_name: "Jane",
          last_name: "Smith",
          city: "LA",
        },
      ])
      .execute();

    await dbRegistry(db)
      .contactDefaults.insertMany([
        {
          _id: new ObjectId("000000000000000000000006"),
          name: "Contact1",
          email: "test@example.com",
        },
        {
          _id: new ObjectId("000000000000000000000007"),
          name: "Contact2",
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should replace root with merged pets", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({
      dogs: S.Number,
      cats: S.Number,
      birds: S.Number,
      fish: S.Number,
    });

    // 2 - Perform aggregation
    const results = await dbRegistry(db)
      .people.aggregate(
        $replaceRoot($ => ({
          newRoot: $.mergeObjects(
            $.literal({
              dogs: 0,
              cats: 0,
              birds: 0,
              fish: 0,
            }),
            "$pets",
          ),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should replace root with unwound grades", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({
      test: S.Number,
      grade: S.Number,
      mean: S.Number,
      std: S.Number,
    });

    // 2 - Perform aggregation
    const results = await dbRegistry(db)
      .students.aggregate(
        $unwind("$grades"),
        $match($ => ({ $expr: $.gte("$grades.grade", 90) })),
        $replaceRoot({ newRoot: "$grades" }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should replace root with full name", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({ full_name: S.String });

    // 2 - Perform aggregation
    const results = await dbRegistry(db)
      .contacts.aggregate(
        $replaceRoot($ => ({ newRoot: { full_name: $.concat("$first_name", " ", "$last_name") } })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should replace with pets using $replaceWith", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({
      dogs: S.optional(S.Number),
      cats: S.optional(S.Number),
      birds: S.optional(S.Number),
      fish: S.optional(S.Number),
    });

    // 2 - Perform aggregation
    const results = await dbRegistry(db)
      .people.aggregate(
        $replaceWith($ =>
          $.mergeObjects(
            $.literal({
              dogs: 0,
              cats: 0,
              birds: 0,
              fish: 0,
            }),
            "$pets",
          ),
        ),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
