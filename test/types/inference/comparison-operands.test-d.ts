/**
 * Comparison operators accept two operands when their types overlap.
 * `string | number` against `string | null` is allowed, because the runtime value can match.
 * Types that can never match are rejected (see test/types/failing/comparison-operands.test-d.ts).
 * @see https://www.mongodb.com/docs/v8.0/reference/operator/aggregation/eq/
 */
import { Schema as S } from "@effect/schema";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";
import { expectType } from "tsd";

import { $project, collection } from "../../../src/sluice.js";

const ObjectIdSchema = S.instanceOf(_ObjectId);

const TestSchema = S.Struct({
  _id: ObjectIdSchema,
  str: S.String,
  nullableStr: S.Union(S.String, S.Null),
  num: S.Number,
  nullableNum: S.Union(S.Number, S.Null),
  optionalNum: S.optional(S.Number),
  stringOrNumber: S.Union(S.String, S.Number),
  flag: S.Boolean,
  date: S.Date,
  nullableDate: S.Union(S.Date, S.Null),
  tags: S.Array(S.String),
});

const mockDb = {} as Db;
const docs = collection("docs", TestSchema, mockDb.collection("docs"));

const results = docs
  .aggregate(
    $project($ => ({
      sameType: $.eq("$num", "$num"),
      fieldAgainstLiteral: $.eq("$num", 5),
      literalAgainstField: $.eq(5, "$num"),
      stringLiteral: $.eq("$str", "abc"),
      nullableAgainstNull: $.eq("$nullableNum", null),
      nullableAgainstField: $.eq("$num", "$nullableNum"),
      fieldAgainstNullable: $.ne("$nullableNum", "$num"),
      overlappingUnions: $.eq("$stringOrNumber", "$nullableStr"),
      unionAgainstMember: $.eq("$stringOrNumber", "$num"),
      booleans: $.eq("$flag", true),
      dates: $.gt("$date", "$nullableDate"),
      dateAgainstDate: $.lt("$date", new Date(0)),
      arrays: $.eq("$tags", ["a", "b"]),
      strings: $.gte("$str", "a"),
      lessOrEqual: $.lte("$nullableNum", 10),
      compareNumbers: $.cmp("$num", 250),
      compareNullable: $.cmp("$nullableNum", "$num"),
      compareStrings: $.cmp("$str", "$nullableStr"),
    })),
  )
  .toList();

type Doc = Awaited<typeof results>[number];

expectType<boolean>({} as Doc["sameType"]);
expectType<boolean>({} as Doc["fieldAgainstLiteral"]);
expectType<boolean>({} as Doc["literalAgainstField"]);
expectType<boolean>({} as Doc["stringLiteral"]);
expectType<boolean>({} as Doc["nullableAgainstNull"]);
expectType<boolean>({} as Doc["nullableAgainstField"]);
expectType<boolean>({} as Doc["fieldAgainstNullable"]);
expectType<boolean>({} as Doc["overlappingUnions"]);
expectType<boolean>({} as Doc["unionAgainstMember"]);
expectType<boolean>({} as Doc["booleans"]);
expectType<boolean>({} as Doc["dates"]);
expectType<boolean>({} as Doc["dateAgainstDate"]);
expectType<boolean>({} as Doc["arrays"]);
expectType<boolean>({} as Doc["strings"]);
expectType<boolean>({} as Doc["lessOrEqual"]);
expectType<-1 | 0 | 1>({} as Doc["compareNumbers"]);
expectType<-1 | 0 | 1>({} as Doc["compareNullable"]);
expectType<-1 | 0 | 1>({} as Doc["compareStrings"]);
