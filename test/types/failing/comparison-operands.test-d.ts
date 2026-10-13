/**
 * Comparison operands whose types can never match are rejected.
 * The matching positive tests are in test/types/inference/comparison-operands.test-d.ts.
 */
import { Schema as S } from "@effect/schema";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";

import { $project, collection } from "../../../src/sluice.js";

const ObjectIdSchema = S.instanceOf(_ObjectId);

const TestSchema = S.Struct({
  _id: ObjectIdSchema,
  str: S.String,
  num: S.Number,
  nullableNum: S.Union(S.Number, S.Null),
  optionalNum: S.optional(S.Number),
  flag: S.Boolean,
  date: S.Date,
  tags: S.Array(S.String),
});

const mockDb = {} as Db;
const docs = collection("docs", TestSchema, mockDb.collection("docs"));

docs.aggregate(
  $project($ => ({
    // A field that is never null can not equal null
    // @ts-expect-error - number against null
    neverNull: $.eq("$num", null),
    // @ts-expect-error - string against number
    stringAgainstNumber: $.eq("$str", "$num"),
    // @ts-expect-error - number field against a string literal
    numberAgainstLiteral: $.eq("$num", "abc"),
    // @ts-expect-error - string field against a number literal
    stringAgainstNumberLiteral: $.gt("$str", 5),
    // @ts-expect-error - boolean against number
    booleanAgainstNumber: $.eq("$flag", 1),
    // @ts-expect-error - date against number
    dateAgainstNumber: $.lt("$date", 5),
    // A missing field is not equal to null in an aggregation, so a field that can only be missing does not match null
    // @ts-expect-error - optional number against null
    optionalAgainstNull: $.eq("$optionalNum", null),
    // @ts-expect-error - array against one of its elements
    arrayAgainstElement: $.eq("$tags", "a"),
    // @ts-expect-error - number against string in $cmp
    compareMixed: $.cmp("$num", "$str"),
    // @ts-expect-error - unknown field
    unknownField: $.eq("$missing", 5),
  })),
);
