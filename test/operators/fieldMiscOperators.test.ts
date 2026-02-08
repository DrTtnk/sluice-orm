import { Schema as S } from "@effect/schema";
import { $addFields, $match, $setWindowFields, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const DocumentSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  data: S.Struct({
    value: S.Number,
    nested: S.Struct({ deep: S.String }),
  }),
  "field.with.dots": S.String,
  "another-field": S.Number,
  metadata: S.Record({
    key: S.String,
    value: S.Object,
  }),
  dynamicField: S.String,
  x: S.Number,
  y: S.Number,
  scores: S.Array(S.Number),
  prices: S.Array(S.Number),
});

const dbRegistry = registry("8.0", { docs: DocumentSchema });

describe("Field Miscellaneous Operators Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .docs.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          name: "Test Doc",
          data: {
            value: 42,
            nested: { deep: "value" },
          },
          "field.with.dots": "dotted value",
          "another-field": 123,
          metadata: {
            key1: { foo: "bar" },
            key2: { baz: 456 },
          },
          dynamicField: "key1",
          x: 10,
          y: 20,
          scores: [85, 92, 88, 90],
          prices: [10.5, 20.3, 15.7],
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          name: "Another Doc",
          data: {
            value: 55,
            nested: { deep: "value-2" },
          },
          "field.with.dots": "dotted value 2",
          "another-field": 456,
          metadata: {
            key1: { foo: "baz" },
            key2: { baz: 789 },
          },
          dynamicField: "key2",
          x: 12,
          y: 25,
          scores: [70, 75, 80],
          prices: [12.1, 18.6, 21.4],
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          name: "Third Doc",
          data: {
            value: 60,
            nested: { deep: "value-3" },
          },
          "field.with.dots": "dotted value 3",
          "another-field": 789,
          metadata: {
            key1: { foo: "qux" },
            key2: { baz: 1011 },
          },
          dynamicField: "key1",
          x: 14,
          y: 22,
          scores: [88, 90, 92],
          prices: [9.9, 14.2, 19.8],
        },
      ])
      .execute();

    await db.collection("docs").createIndex({ name: "text" });
  });

  afterAll(async () => {
    await teardown();
  });

  // $getField tests
  it("should access field with dots in name", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      dottedValue: S.String,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          dottedValue: $.getField({
            field: "field.with.dots",
            input: "$$ROOT",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should access field with hyphen", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      hyphenValue: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          hyphenValue: $.getField({
            field: "another-field",
            input: "$$ROOT",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should access dynamic field", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      dynamicValue: S.Object,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          dynamicValue: $.getField({
            field: "$dynamicField",
            input: "$metadata",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should access nested field", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      nestedAccess: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          nestedAccess: $.getField({
            field: "value",
            input: "$data",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // $setField tests
  it("should set field with dots", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      modified: S.Object,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          modified: $.setField({
            field: "new.field",
            input: "$data",
            value: "test",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should set nested field", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      withNewField: S.Struct({
        deep: S.String,
        added: S.Literal(123),
      }),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          withNewField: $.setField({
            field: "added",
            input: "$data.nested",
            value: 123,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should set field with computed value", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      computed: S.Struct({
        value: S.Number,
        nested: S.Struct({ deep: S.String }),
        sum: S.Number,
      }),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          computed: $.setField({
            field: "sum",
            input: "$data",
            value: $.add("$x", "$y"),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // $unsetField tests
  it("should remove field from object", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      cleaned: S.Struct({ nested: S.Struct({ deep: S.String }) }),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          cleaned: $.unsetField({
            field: "value",
            input: "$data",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should remove nested field", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      withoutNested: S.Struct({ value: S.Number }),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          withoutNested: $.unsetField({
            field: "nested",
            input: "$data",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // $literal tests
  it("should return literal string without parsing", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      notAField: S.Literal("$name"),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ notAField: $.literal("$name") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should return literal object", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      literalObj: S.Struct({ $gt: S.Literal(10) }),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ literalObj: $.literal({ $gt: 10 }) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should return literal number", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      constantValue: S.Literal(42),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ constantValue: $.literal(42) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should return literal array", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      literalArray: S.Tuple(S.Literal(1), S.Literal(2), S.Literal(3)),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ literalArray: $.literal([1, 2, 3]) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // $rand tests
  it("should generate random value", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      randomValue: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ randomValue: $.rand() })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should generate random in range", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      random100: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ random100: $.multiply($.rand(), 100) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should generate random boolean", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      randomBool: S.Boolean,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ randomBool: $.lt($.rand(), 0.5) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should select random array element", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      randomElement: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          randomElement: $.arrayElemAt("$scores", $.floor($.multiply($.rand(), $.size("$scores")))),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // $meta tests
  it("should access text search score", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      score: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $match($ => ({ $text: { $search: "test" } })),
        $addFields($ => ({ score: $.meta("textScore") })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should access search score", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      searchScore: S.optional(S.Number), // Optional because $meta returns nothing without Atlas Search
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ searchScore: $.meta("searchScore") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should access search highlights", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      highlights: S.optional(
        S.Array(
          S.Struct({
            path: S.String,
            texts: S.Array(
              S.Struct({
                value: S.String,
                type: S.Union(S.Literal("text"), S.Literal("hit")),
              }),
            ),
          }),
        ),
      ), // Optional because $meta returns nothing without Atlas Search
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ highlights: $.meta("searchHighlights") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should access index key", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      indexKey: S.optional(
        S.Record({
          key: S.String,
          value: S.String,
        }),
      ), // Optional because $meta returns nothing without index
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ indexKey: $.meta("indexKey") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // $covariancePop / $covarianceSamp tests (window-only)
  it("should calculate population covariance", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      xyCovariance: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $setWindowFields($ => ({
          sortBy: { _id: 1 },
          output: { xyCovariance: $.covariancePop("$x", "$y") },
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should calculate sample covariance", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      xySampleCov: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $setWindowFields($ => ({
          sortBy: { _id: 1 },
          output: { xySampleCov: $.covarianceSamp("$x", "$y") },
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should calculate covariance by group", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      cov: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $setWindowFields($ => ({
          partitionBy: "$name",
          sortBy: { _id: 1 },
          output: { cov: $.covariancePop("$x", "$y") },
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // Complex operations
  it("should perform complex random sampling with literal", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      maybeSpecial: S.Union(S.Number, S.Literal("SPECIAL")),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          maybeSpecial: $.cond({
            if: $.lt($.rand(), 0.1),
            then: $.literal("SPECIAL"),
            else: $.multiply($.rand(), 100),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should calculate statistical correlation", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      covariance: S.NullOr(S.Number),
      stdDevX: S.NullOr(S.Number),
      stdDevY: S.NullOr(S.Number),
      correlation: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $setWindowFields($ => ({
          sortBy: { _id: 1 },
          output: {
            covariance: $.covariancePop("$x", "$y"),
            stdDevX: $.stdDevPop("$x"),
            stdDevY: $.stdDevPop("$y"),
          },
        })),
        $addFields($ => ({
          correlation: $.divide("$covariance", $.multiply("$stdDevX", "$stdDevY")),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
