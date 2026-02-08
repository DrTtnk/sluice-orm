import { Schema as S } from "@effect/schema";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";
import { expectType } from "tsd";

import {
  $group,
  $match,
  $project,
  $setWindowFields,
  $unwind,
  collection,
} from "../../../src/sluice.js";

const ObjectIdSchema = S.instanceOf(_ObjectId);

const TestSchema = S.Struct({
  _id: ObjectIdSchema,
  score: S.Number,
  date: S.Date,
  maybeDate: S.Union(S.Date, S.Null),
  tags: S.Array(S.String),
  nested: S.Struct({
    score: S.Number,
  }),
});

type TestDoc = typeof TestSchema.Type;

const mockDb = {} as Db;
const docs = collection("docs", TestSchema, mockDb.collection("docs"));

// $add/$subtract date arithmetic
const dateArithmetic = docs
  .aggregate(
    $project($ => ({
      datePlus: $.add("$date", 1_000),
      dateMinus: $.subtract("$date", 2_000),
      dateDiff: $.subtract("$date", "$date"),
      numericDiff: $.subtract("$score", 1),
    })),
  )
  .toList();

type DateArithmeticDoc = Awaited<typeof dateArithmetic>[number];
expectType<Date>({} as DateArithmeticDoc["datePlus"]);
expectType<Date>({} as DateArithmeticDoc["dateMinus"]);
expectType<number>({} as DateArithmeticDoc["dateDiff"]);
expectType<number>({} as DateArithmeticDoc["numericDiff"]);

// Null-aware conversions
const conversions = docs
  .aggregate(
    $project($ => ({
      nullableToString: $.toString("$maybeDate"),
      nonNullToString: $.toString("$date"),
      sampled: $.sampleRate(0.5),
    })),
  )
  .toList();

type ConversionDoc = Awaited<typeof conversions>[number];
expectType<string | null>({} as ConversionDoc["nullableToString"]);
expectType<string>({} as ConversionDoc["nonNullToString"]);
expectType<boolean>({} as ConversionDoc["sampled"]);

// SortBy spec for top/bottom accumulators
const validSortBy = docs
  .aggregate(
    $group($ => ({
      _id: "$_id",
      topScore: $.top({
        output: "$score",
        sortBy: { score: -1, "nested.score": 1 },
      }),
    })),
  )
  .toList();

type ValidSortByDoc = Awaited<typeof validSortBy>[number];
expectType<number>({} as ValidSortByDoc["topScore"]);

// $unwind preserveNullAndEmptyArrays
const unwindPreserve = docs
  .aggregate($unwind({ path: "$tags", preserveNullAndEmptyArrays: true }))
  .toList();

type UnwindPreserveDoc = Awaited<typeof unwindPreserve>[number];
expectType<string | null | undefined>({} as UnwindPreserveDoc["tags"]);

const unwindStrict = docs.aggregate($unwind("$tags")).toList();

type UnwindStrictDoc = Awaited<typeof unwindStrict>[number];
expectType<string>({} as UnwindStrictDoc["tags"]);

// $nor in $match
const matchNor = docs.aggregate($match(() => ({ $nor: [{ score: { $gt: 10 } }] }))).toList();
expectType<number>({} as Awaited<typeof matchNor>[number]["score"]);

// $percentRank in $setWindowFields
const percentRank = docs
  .aggregate(
    $setWindowFields($ => ({
      sortBy: { score: 1 },
      output: { percentRank: { $percentRank: {} } },
    })),
  )
  .toList();

type PercentRankDoc = Awaited<typeof percentRank>[number];
expectType<number>({} as PercentRankDoc["percentRank"]);

// keep reference to TestDoc to ensure it is used
expectType<TestDoc>({} as TestDoc);
