/**
 * MongoDB returns null from most numeric operators when an operand is null or missing.
 * The result type must contain `null` exactly when an operand can be null or missing.
 * @see https://www.mongodb.com/docs/v8.0/reference/operator/aggregation/abs/
 */
import { Schema as S } from "@effect/schema";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";
import { expectType } from "tsd";

import { $group, $project, $setWindowFields, collection } from "../../../src/sluice.js";

const ObjectIdSchema = S.instanceOf(_ObjectId);

const TestSchema = S.Struct({
  _id: ObjectIdSchema,
  num: S.Number,
  other: S.Number,
  nullableNum: S.Union(S.Number, S.Null),
  optionalNum: S.optional(S.Number),
  date: S.Date,
  nullableDate: S.Union(S.Date, S.Null),
});

const mockDb = {} as Db;
const docs = collection("docs", TestSchema, mockDb.collection("docs"));

const numeric = docs
  .aggregate(
    $project($ => ({
      absOk: $.abs("$num"),
      absNull: $.abs("$nullableNum"),
      absOptional: $.abs("$optionalNum"),
      ceilNull: $.ceil("$nullableNum"),
      floorNull: $.floor("$nullableNum"),
      expNull: $.exp("$nullableNum"),
      lnNull: $.ln("$nullableNum"),
      log10Null: $.log10("$nullableNum"),
      sqrtOk: $.sqrt("$num"),
      sqrtNull: $.sqrt("$nullableNum"),
      roundOk: $.round("$num"),
      roundNull: $.round("$nullableNum"),
      roundPlaceNull: $.round("$num", "$nullableNum"),
      truncNull: $.trunc("$nullableNum"),
      sinNull: $.sin("$nullableNum"),
      degreesNull: $.degreesToRadians("$nullableNum"),
      bitNotNull: $.bitNot("$nullableNum"),
      divideOk: $.divide("$num", "$other"),
      divideNull: $.divide("$num", "$nullableNum"),
      modNull: $.mod("$nullableNum", 2),
      powNull: $.pow("$num", "$nullableNum"),
      logNull: $.log("$nullableNum", 10),
      atan2Null: $.atan2("$num", "$nullableNum"),
      multiplyOk: $.multiply("$num", "$other", 2),
      multiplyNull: $.multiply("$num", "$nullableNum"),
      bitAndNull: $.bitAnd("$num", "$nullableNum"),
      addOk: $.add("$num", 1),
      addNull: $.add("$num", "$nullableNum"),
      addOptional: $.add("$num", "$optionalNum"),
      subtractNull: $.subtract("$num", "$nullableNum"),
      dateAddOk: $.add("$date", 1_000),
      dateAddNull: $.add("$nullableDate", 1_000),
      dateAddNullAmount: $.add("$date", "$nullableNum"),
      dateSubtractNull: $.subtract("$nullableDate", 1_000),
      dateDiffOk: $.subtract("$date", "$date"),
      dateDiffNull: $.subtract("$date", "$nullableDate"),
    })),
  )
  .toList();

type NumericDoc = Awaited<typeof numeric>[number];

expectType<number>({} as NumericDoc["absOk"]);
expectType<number | null>({} as NumericDoc["absNull"]);
expectType<number | null>({} as NumericDoc["absOptional"]);
expectType<number | null>({} as NumericDoc["ceilNull"]);
expectType<number | null>({} as NumericDoc["floorNull"]);
expectType<number | null>({} as NumericDoc["expNull"]);
expectType<number | null>({} as NumericDoc["lnNull"]);
expectType<number | null>({} as NumericDoc["log10Null"]);
expectType<number>({} as NumericDoc["sqrtOk"]);
expectType<number | null>({} as NumericDoc["sqrtNull"]);
expectType<number>({} as NumericDoc["roundOk"]);
expectType<number | null>({} as NumericDoc["roundNull"]);
expectType<number | null>({} as NumericDoc["roundPlaceNull"]);
expectType<number | null>({} as NumericDoc["truncNull"]);
expectType<number | null>({} as NumericDoc["sinNull"]);
expectType<number | null>({} as NumericDoc["degreesNull"]);
expectType<number | null>({} as NumericDoc["bitNotNull"]);
expectType<number>({} as NumericDoc["divideOk"]);
expectType<number | null>({} as NumericDoc["divideNull"]);
expectType<number | null>({} as NumericDoc["modNull"]);
expectType<number | null>({} as NumericDoc["powNull"]);
expectType<number | null>({} as NumericDoc["logNull"]);
expectType<number | null>({} as NumericDoc["atan2Null"]);
expectType<number>({} as NumericDoc["multiplyOk"]);
expectType<number | null>({} as NumericDoc["multiplyNull"]);
expectType<number | null>({} as NumericDoc["bitAndNull"]);
expectType<number>({} as NumericDoc["addOk"]);
expectType<number | null>({} as NumericDoc["addNull"]);
expectType<number | null>({} as NumericDoc["addOptional"]);
expectType<number | null>({} as NumericDoc["subtractNull"]);
expectType<Date>({} as NumericDoc["dateAddOk"]);
expectType<Date | null>({} as NumericDoc["dateAddNull"]);
expectType<Date | null>({} as NumericDoc["dateAddNullAmount"]);
expectType<Date | null>({} as NumericDoc["dateSubtractNull"]);
expectType<number>({} as NumericDoc["dateDiffOk"]);
expectType<number | null>({} as NumericDoc["dateDiffNull"]);

// ============================================
// String and date operators
// ============================================

const StringSchema = S.Struct({
  _id: ObjectIdSchema,
  str: S.String,
  other: S.String,
  nullableStr: S.Union(S.String, S.Null),
  optionalStr: S.optional(S.String),
  nullableNum: S.Union(S.Number, S.Null),
  date: S.Date,
  nullableDate: S.Union(S.Date, S.Null),
});

const strings = collection("strings", StringSchema, mockDb.collection("strings"));

const stringResults = strings
  .aggregate(
    $project($ => ({
      concatOk: $.concat("$str", "x"),
      concatNull: $.concat("$nullableStr", "x"),
      concatOptional: $.concat("x", "$optionalStr"),
      indexBytesOk: $.indexOfBytes("$str", "x"),
      indexBytesNull: $.indexOfBytes("$nullableStr", "x"),
      indexCPOk: $.indexOfCP("$str", "x"),
      indexCPNull: $.indexOfCP("$nullableStr", "x"),
      trimOk: $.trim({ input: "$str" }),
      trimNull: $.trim({ input: "$nullableStr" }),
      ltrimNull: $.ltrim({ input: "$nullableStr" }),
      rtrimNull: $.rtrim({ input: "$optionalStr" }),
      replaceOneOk: $.replaceOne({ input: "$str", find: "a", replacement: "b" }),
      replaceOneNullInput: $.replaceOne({ input: "$nullableStr", find: "a", replacement: "b" }),
      replaceOneNullFind: $.replaceOne({ input: "$str", find: "$nullableStr", replacement: "b" }),
      replaceAllNullReplacement: $.replaceAll({ input: "$str", find: "a", replacement: "$nullableStr" }),
      regexFound: $.regexFind({ input: "$str", regex: "a(b)?" }),
      isNumberOk: $.isNumber("$nullableNum"),
      dateTruncOk: $.dateTrunc({ date: "$date", unit: "day" }),
      dateTruncNull: $.dateTrunc({ date: "$nullableDate", unit: "day" }),
      dateToStringOk: $.dateToString({ date: "$date" }),
      dateToStringNull: $.dateToString({ date: "$nullableDate" }),
      dateToStringOnNull: $.dateToString({ date: "$nullableDate", onNull: 0 }),
      dateFromStringOk: $.dateFromString({ dateString: "$str" }),
      dateFromStringNull: $.dateFromString({ dateString: "$nullableStr" }),
      dateFromStringOnNull: $.dateFromString({ dateString: "$nullableStr", onNull: 0 }),
      dateFromStringOnError: $.dateFromString({ dateString: "$str", onError: 0 }),
    })),
  )
  .toList();

type StringDoc = Awaited<typeof stringResults>[number];

expectType<`${string}x`>({} as StringDoc["concatOk"]);
expectType<string | null>({} as StringDoc["concatNull"]);
expectType<string | null>({} as StringDoc["concatOptional"]);
expectType<number>({} as StringDoc["indexBytesOk"]);
expectType<number | null>({} as StringDoc["indexBytesNull"]);
expectType<number>({} as StringDoc["indexCPOk"]);
expectType<number | null>({} as StringDoc["indexCPNull"]);
expectType<string>({} as StringDoc["trimOk"]);
expectType<string | null>({} as StringDoc["trimNull"]);
expectType<string | null>({} as StringDoc["ltrimNull"]);
expectType<string | null>({} as StringDoc["rtrimNull"]);
expectType<string>({} as StringDoc["replaceOneOk"]);
expectType<string | null>({} as StringDoc["replaceOneNullInput"]);
expectType<string | null>({} as StringDoc["replaceOneNullFind"]);
expectType<string | null>({} as StringDoc["replaceAllNullReplacement"]);
// A capture group that does not take part in the match is null
expectType<(string | null)[]>({} as NonNullable<StringDoc["regexFound"]>["captures"]);
expectType<boolean>({} as StringDoc["isNumberOk"]);
expectType<Date>({} as StringDoc["dateTruncOk"]);
expectType<Date | null>({} as StringDoc["dateTruncNull"]);
expectType<string>({} as StringDoc["dateToStringOk"]);
expectType<string | null>({} as StringDoc["dateToStringNull"]);
expectType<string | number>({} as StringDoc["dateToStringOnNull"]);
expectType<Date>({} as StringDoc["dateFromStringOk"]);
expectType<Date | null>({} as StringDoc["dateFromStringNull"]);
expectType<Date | number>({} as StringDoc["dateFromStringOnNull"]);
expectType<Date | number>({} as StringDoc["dateFromStringOnError"]);

// ============================================
// Accumulators and window operators
// ============================================

const AccumulatorSchema = S.Struct({
  _id: ObjectIdSchema,
  group: S.String,
  num: S.Number,
  optionalNum: S.optional(S.Number),
  nullableNum: S.Union(S.Number, S.Null),
});

const measurements = collection("measurements", AccumulatorSchema, mockDb.collection("measurements"));

// A missing or null value gives null, never undefined. A required field never gives null.
const grouped = measurements
  .aggregate(
    $group($ => ({
      _id: "$group",
      firstOk: $.first("$num"),
      firstOptional: $.first("$optionalNum"),
      lastNullable: $.last("$nullableNum"),
      minOk: $.min("$num"),
      minOptional: $.min("$optionalNum"),
      maxOptional: $.max("$optionalNum"),
      topOk: $.top({ output: "$num", sortBy: { num: 1 } }),
      topOptional: $.top({ output: "$optionalNum", sortBy: { num: 1 } }),
      bottomOptional: $.bottom({ output: "$optionalNum", sortBy: { num: 1 } }),
      stdDevPopOk: $.stdDevPop("$num"),
      // A group of one document gives null
      stdDevSampOk: $.stdDevSamp("$num"),
    })),
  )
  .toList();

type GroupedDoc = Awaited<typeof grouped>[number];

expectType<number>({} as GroupedDoc["firstOk"]);
expectType<number | null>({} as GroupedDoc["firstOptional"]);
expectType<number | null>({} as GroupedDoc["lastNullable"]);
expectType<number>({} as GroupedDoc["minOk"]);
expectType<number | null>({} as GroupedDoc["minOptional"]);
expectType<number | null>({} as GroupedDoc["maxOptional"]);
expectType<number>({} as GroupedDoc["topOk"]);
expectType<number | null>({} as GroupedDoc["topOptional"]);
expectType<number | null>({} as GroupedDoc["bottomOptional"]);
expectType<number>({} as GroupedDoc["stdDevPopOk"]);
expectType<number | null>({} as GroupedDoc["stdDevSampOk"]);

const windowed = measurements
  .aggregate(
    $setWindowFields($ => ({
      sortBy: { num: 1 },
      output: {
        locfOk: $.locf("$num"),
        locfOptional: $.locf("$optionalNum"),
        linearFillNullable: $.linearFill("$nullableNum"),
        derivativeValue: $.derivative({ input: "$num", unit: "hour" }),
        integralValue: $.integral({ input: "$num", unit: "hour" }),
        movingAverage: $.expMovingAvg({ input: "$num", N: 3 }),
      },
    })),
  )
  .toList();

type WindowedDoc = Awaited<typeof windowed>[number];

expectType<number>({} as WindowedDoc["locfOk"]);
expectType<number | null>({} as WindowedDoc["locfOptional"]);
expectType<number | null>({} as WindowedDoc["linearFillNullable"]);
expectType<number | null>({} as WindowedDoc["derivativeValue"]);
expectType<number>({} as WindowedDoc["integralValue"]);
expectType<number>({} as WindowedDoc["movingAverage"]);
