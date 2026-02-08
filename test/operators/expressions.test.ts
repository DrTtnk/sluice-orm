import { Schema as S } from "@effect/schema";
import { $project, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { Binary, Decimal128, ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const TestDocSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  score: S.Number,
  date: S.Date,
  active: S.Boolean,
  tags: S.Array(S.String),
  values: S.Array(S.Number),
  binary: S.instanceOf(Binary),
  nested: S.Struct({
    field: S.String,
    count: S.Number,
  }),
});

const dbRegistry = registry("8.0", { test: TestDocSchema });

describe("Expression Operators Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .test.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          name: "Alice",
          age: 30,
          score: 87.5,
          date: new Date("2024-01-15T10:30:45.123Z"),
          active: true,
          tags: ["developer", "senior", "vip"],
          values: [10, 25, 50, 75, 100],
          binary: new Binary(Buffer.from("abc")),
          nested: {
            field: "value",
            count: 42,
          },
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should perform arithmetic operations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      doubled: S.Number,
      halved: S.Number,
      summed: S.Number,
      diff: S.Number,
      remainder: S.Number,
      absolute: S.Number,
      ceiling: S.Number,
      floored: S.Number,
      exponent: S.Number,
      natural_log: S.Number,
      log10: S.Number,
      power: S.Number,
      square_root: S.Number,
      truncated: S.Number,
      rounded: S.Number,
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          doubled: $.multiply("$age", 2),
          halved: $.divide("$score", 2),
          summed: $.add("$age", "$score"),
          diff: $.subtract("$score", "$age"),
          remainder: $.mod("$age", 3),
          absolute: $.abs($.subtract("$age", "$score")),
          ceiling: $.ceil("$score"),
          floored: $.floor("$score"),
          exponent: $.exp(1),
          natural_log: $.ln("$age"),
          log10: $.log10("$score"),
          power: $.pow("$age", 2),
          square_root: $.sqrt("$score"),
          truncated: $.trunc("$score"),
          rounded: $.round("$score", 2),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should perform date arithmetic with $add and $subtract", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      datePlusMillis: S.Date,
      dateMinusMillis: S.Date,
      dateDifference: S.Number,
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          datePlusMillis: $.add("$date", 86_400_000),
          dateMinusMillis: $.subtract("$date", 3_600_000),
          dateDifference: $.subtract("$date", new Date("2024-01-15T00:00:00.000Z")),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should perform comparison operations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      isEqual: S.Boolean,
      isNotEqual: S.Boolean,
      isGreater: S.Boolean,
      isGreaterOrEqual: S.Boolean,
      isLess: S.Boolean,
      isLessOrEqual: S.Boolean,
      comparison: S.Union(S.Literal(-1), S.Literal(0), S.Literal(1)),
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          isEqual: $.eq("$age", 30),
          isNotEqual: $.ne("$age", 0),
          isGreater: $.gt("$score", 50),
          isGreaterOrEqual: $.gte("$score", 50),
          isLess: $.lt("$age", 18),
          isLessOrEqual: $.lte("$age", 65),
          comparison: $.cmp("$age", "$score"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should perform boolean operations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      andResult: S.Boolean,
      orResult: S.Boolean,
      notResult: S.Boolean,
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          andResult: $.and($.gt("$age", 18), $.eq("$active", true)),
          orResult: $.or($.lt("$age", 18), $.gt("$age", 65)),
          notResult: $.not("$active"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should perform string operations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      upper: S.String,
      lower: S.String,
      concatenated: S.String,
      substring: S.String,
      length: S.Number,
      lengthCp: S.Number,
      caseCompare: S.Literal(-1, 0, 1),
      trimmed: S.String,
      split: S.Array(S.String),
      replaced: S.String,
      regexMatched: S.Boolean,
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          upper: $.toUpper("$name"),
          lower: $.toLower("$name"),
          concatenated: $.concat("$name", " - ", "test"),
          substring: $.substr("$name", 0, 5),
          length: $.strLenBytes("$name"),
          lengthCp: $.strLenCP("$name"),
          caseCompare: $.strcasecmp("$name", "ALICE"),
          trimmed: $.trim({ input: "$name" }),
          split: $.split("$name", " "),
          replaced: $.replaceAll({
            input: "$name",
            find: "a",
            replacement: "A",
          }),
          regexMatched: $.regexMatch({
            input: "$name",
            regex: /^A/,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should perform array operations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      arraySize: S.Number,
      firstElement: S.String,
      lastElement: S.String,
      inArray: S.Boolean,
      concatArrays: S.Array(S.String),
      sliced: S.Array(S.String),
      reversed: S.Array(S.String),
      isArray: S.Boolean,
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          arraySize: $.size("$tags"),
          firstElement: $.arrayElemAt("$tags", 0),
          lastElement: $.arrayElemAt("$tags", -1),
          inArray: $.in("vip", "$tags"),
          concatArrays: $.concatArrays("$tags", "$tags"),
          sliced: $.slice("$tags", 0, 2),
          reversed: $.reverseArray("$tags"),
          isArray: $.isArray("$tags"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should perform array aggregation operations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      arraySum: S.Number,
      arrayAvg: S.NullOr(S.Number),
      minOfTwo: S.Number,
      maxOfTwo: S.Number,
      filtered: S.Array(S.Number),
      mapped: S.Array(S.Number),
      reduced: S.Number,
      concat: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          arraySum: $.sum("$values"),
          arrayAvg: $.avg("$values"),
          minOfTwo: $.min("$age", "$score"),
          maxOfTwo: $.max("$age", "$score"),
          filtered: $.filter({
            input: "$values",
            cond: $ => $.gt("$$this", 50),
          }),
          mapped: $.map({
            input: "$values",
            in: $ => $.multiply("$$this", 2),
          }),
          reduced: $.reduce({
            input: "$values",
            initialValue: 0,
            in: $ => $.add("$$value", "$$this"),
          }),
          concat: $.reduce({
            input: "$tags",
            initialValue: [] as string[],
            in: $ => $.setUnion("$$value", ["$$this"]),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should perform date operations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      year: S.Number,
      month: S.Number,
      dayOfMonth: S.Number,
      dayOfWeek: S.Number,
      dayOfYear: S.Number,
      hour: S.Number,
      minute: S.Number,
      second: S.Number,
      millisecond: S.Number,
      isoWeek: S.Number,
      isoWeekYear: S.Number,
      dateStr: S.String,
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          year: $.year("$date"),
          month: $.month("$date"),
          dayOfMonth: $.dayOfMonth("$date"),
          dayOfWeek: $.dayOfWeek("$date"),
          dayOfYear: $.dayOfYear("$date"),
          hour: $.hour("$date"),
          minute: $.minute("$date"),
          second: $.second("$date"),
          millisecond: $.millisecond("$date"),
          isoWeek: $.isoWeek("$date"),
          isoWeekYear: $.isoWeekYear("$date"),
          dateStr: $.dateToString({
            format: "%Y-%m-%d",
            date: "$date",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should perform conditional operations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      conditional: S.Union(S.Literal("adult"), S.Literal("minor")),
      switched: S.Union(
        S.Literal("child"),
        S.Literal("teenager"),
        S.Literal("adult"),
        S.Literal("senior"),
      ),
      ifNull: S.String,
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          conditional: $.cond({
            if: $.gte("$age", 18),
            then: "adult",
            else: "minor",
          }),
          switched: $.switch({
            branches: [
              {
                case: $.lt("$age", 13),
                then: "child",
              },
              {
                case: $.lt("$age", 20),
                then: "teenager",
              },
              {
                case: $.lt("$age", 60),
                then: "adult",
              },
            ],
            default: "senior",
          }),
          ifNull: $.ifNull("$name", "Unknown"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should perform type conversion operations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      toStr: S.String,
      toInt: S.Number,
      toDouble: S.Number,
      toDecimal: S.instanceOf(Decimal128),
      toLong: S.Number,
      toBool: S.Boolean,
      toDate: S.Date,
      toObjectId: S.Any,
      typeOf: S.String,
      isNumber: S.Boolean,
      binarySize: S.Number,
      bsonSize: S.Number,
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          toStr: $.toString("$age"),
          toInt: $.toInt("$score"),
          toDouble: $.toDouble("$age"),
          toDecimal: $.toDecimal("$score"),
          toLong: $.toLong("$age"),
          toBool: $.toBool("$active"),
          toDate: $.toDate("$date"),
          toObjectId: $.toObjectId("$_id"),
          typeOf: $.type("$name"),
          isNumber: $.isNumber("$score"),
          binarySize: $.binarySize("$binary"),
          bsonSize: $.bsonSize("$$ROOT"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should perform object operations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      mergedObjects: S.Struct({
        field: S.String,
        count: S.Number,
      }),
      mergedOverride: S.Struct({
        a: S.Number,
        b: S.String,
        c: S.Number,
      }),
      asArray: S.Array(
        S.Struct({
          k: S.String,
          v: S.Union(S.String, S.Number),
        }),
      ),
      setField: S.Struct({
        field: S.String,
        count: S.Number,
        newField: S.String,
      }),
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          mergedObjects: $.mergeObjects("$nested"),
          mergedOverride: $.mergeObjects({ a: 1, b: "old" }, null, { b: "new", c: 2 }),
          asArray: $.objectToArray("$nested"),
          setField: $.setField({
            field: "newField",
            input: "$nested",
            value: "newValue",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should perform trigonometry operations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      sine: S.Number,
      cosine: S.Number,
      tangent: S.Number,
      arcSine: S.Number,
      arcCosine: S.Number,
      arcTangent: S.Number,
      arcTangent2: S.Number,
      arcHyperbolicSine: S.Number,
      arcHyperbolicCosine: S.Number,
      arcHyperbolicTangent: S.Number,
      hypSine: S.Number,
      hypCosine: S.Number,
      hypTangent: S.Number,
      degreesToRad: S.Number,
      radiansToDeg: S.Number,
    });

    const results = await dbRegistry(db)
      .test.aggregate(
        $project($ => ({
          _id: 1,
          sine: $.sin("$score"),
          cosine: $.cos("$score"),
          tangent: $.tan("$score"),
          arcSine: $.asin(0.5),
          arcCosine: $.acos(0.5),
          arcTangent: $.atan(1),
          arcTangent2: $.atan2(1, 1),
          arcHyperbolicSine: $.asinh(1),
          arcHyperbolicCosine: $.acosh(2),
          arcHyperbolicTangent: $.atanh(0.5),
          hypSine: $.sinh("$score"),
          hypCosine: $.cosh("$score"),
          hypTangent: $.tanh("$score"),
          degreesToRad: $.degreesToRadians(180),
          radiansToDeg: $.radiansToDegrees(3.14159),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });
});
