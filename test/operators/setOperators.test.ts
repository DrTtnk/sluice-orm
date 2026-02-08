// Runtime equivalent of setOperators.test-d.ts
import { Schema as S } from "@effect/schema";
import { $addFields, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const DocumentSchema = S.Struct({
  _id: ObjectIdSchema,
  A: S.Array(S.String),
  B: S.Array(S.String),
  numbers1: S.Array(S.Number),
  numbers2: S.Array(S.Number),
  booleans: S.Array(S.Boolean),
  nested: S.Struct({
    tags: S.Array(S.String),
    flags: S.Array(S.Boolean),
  }),
  permissions: S.Array(S.Literal("read", "write", "admin")),
  roles: S.Array(S.Literal("user", "admin", "moderator")),
});

type Document = typeof DocumentSchema.Type;

const dbRegistry = registry("8.0", { docs: DocumentSchema });

describe("SetOperators Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .docs.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          A: ["a", "b", "c"],
          B: ["b", "c", "d"],
          numbers1: [1, 2, 3],
          numbers2: [2, 3, 4],
          booleans: [true, false],
          nested: {
            tags: ["tag1", "tag2"],
            flags: [true, true],
          },
          permissions: ["read", "write"],
          roles: ["user"],
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should compute set union of string arrays", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      combined: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ combined: $.setUnion("$A", "$B") })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should compute set union of multiple arrays", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      allTags: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ allTags: $.setUnion("$A", "$B", "$nested.tags") })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should compute set intersection", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      common: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ common: $.setIntersection("$A", "$B") })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should compute set difference", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      onlyInA: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ onlyInA: $.setDifference("$A", "$B") })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should check set equality", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      areEqual: S.Boolean,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ areEqual: $.setEquals("$A", "$B") })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should check if set is subset", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      isSubset: S.Boolean,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ isSubset: $.setIsSubset("$A", "$B") })))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should evaluate any/all element true", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      anyTrue: S.Boolean,
      allTrue: S.Boolean,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          anyTrue: $.anyElementTrue("$booleans"),
          allTrue: $.allElementsTrue("$nested.flags"),
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
