/**
 * Numeric arguments with a documented range accept good number literals and any value typed `number`.
 * The matching negative tests are in test/types/failing/numeric-literals.test-d.ts.
 * @see https://www.mongodb.com/docs/v8.0/reference/operator/aggregation/limit/
 */
import { Schema as S } from "@effect/schema";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";
import { expectType } from "tsd";

import {
  $bucketAuto,
  $graphLookup,
  $group,
  $limit,
  $project,
  $sample,
  $skip,
  collection,
} from "../../../src/sluice.js";

const ObjectIdSchema = S.instanceOf(_ObjectId);

const TestSchema = S.Struct({
  _id: ObjectIdSchema,
  num: S.Number,
  tags: S.Array(S.String),
  parentId: S.String,
  flags: S.Number,
});

const mockDb = {} as Db;
const docs = collection("docs", TestSchema, mockDb.collection("docs"));

declare const anyNumber: number;

const stages = docs
  .aggregate(
    $limit(1),
    $limit(anyNumber),
    $skip(0),
    $skip(10),
    $skip(anyNumber),
    $sample({ size: 1 }),
    $sample({ size: anyNumber }),
    $bucketAuto({ groupBy: "$num", buckets: 3 }),
  )
  .toList();
expectType<Promise<{ _id: { min: number; max: number }; count: number }[]>>(stages);

docs.aggregate(
  $graphLookup({
    from: docs,
    startWith: "$parentId",
    connectFromField: "parentId",
    connectToField: "_id",
    as: "ancestors",
    maxDepth: 0,
  }),
);

const expressions = docs
  .aggregate(
    $project($ => ({
      rangeUp: $.range(0, 10),
      rangeStep: $.range(0, 10, 2),
      rangeDown: $.range(10, 0, -2),
      rangeDynamic: $.range(0, "$num", "$flags"),
      firstThree: $.firstN({ input: "$tags", n: 3 }),
      lastDynamic: $.lastN({ input: "$tags", n: "$num" }),
      maxOne: $.maxN({ input: "$tags", n: 1 }),
      bits: $.bitAnd("$flags", 255),
      bitNot: $.bitNot(5),
    })),
  )
  .toList();

type ExpressionDoc = Awaited<typeof expressions>[number];
expectType<number[]>({} as ExpressionDoc["rangeUp"]);
expectType<number>({} as ExpressionDoc["bits"]);

docs.aggregate(
  $group($ => ({
    _id: null,
    topTwo: $.topN({ output: "$num", sortBy: { num: -1 }, n: 2 }),
    bottomOne: $.bottomN({ output: "$num", sortBy: { num: -1 }, n: 1 }),
  })),
);
