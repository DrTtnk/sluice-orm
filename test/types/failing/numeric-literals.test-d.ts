/**
 * A number literal outside the documented range of an argument is rejected at compile time.
 * A value typed `number` is accepted, because only a literal shows the mistake.
 */
import { Schema as S } from "@effect/schema";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";

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

// $limit needs a positive integer
// @ts-expect-error - zero is not a positive integer
docs.aggregate($limit(0));
// @ts-expect-error - negative
docs.aggregate($limit(-1));
// @ts-expect-error - fractional
docs.aggregate($limit(1.5));

// $skip needs a non-negative integer
// @ts-expect-error - negative
docs.aggregate($skip(-1));
// @ts-expect-error - fractional
docs.aggregate($skip(0.5));

// $sample needs a positive integer size
// @ts-expect-error - zero is not a positive integer
docs.aggregate($sample({ size: 0 }));
// @ts-expect-error - negative
docs.aggregate($sample({ size: -1 }));
// @ts-expect-error - fractional
docs.aggregate($sample({ size: 2.5 }));

// $bucketAuto needs a positive integer number of buckets
// @ts-expect-error - zero is not a positive integer
docs.aggregate($bucketAuto({ groupBy: "$num", buckets: 0 }));
// @ts-expect-error - fractional
docs.aggregate($bucketAuto({ groupBy: "$num", buckets: 2.5 }));

// $graphLookup needs a non-negative integer maxDepth
docs.aggregate(
  $graphLookup({
    from: docs,
    startWith: "$parentId",
    connectFromField: "parentId",
    connectToField: "_id",
    as: "ancestors",
    // @ts-expect-error - negative
    maxDepth: -1,
  }),
);

docs.aggregate(
  $project($ => ({
    // @ts-expect-error - $range step 0 never ends
    zeroStep: $.range(0, 10, 0),
    // @ts-expect-error - $range start must be an integer
    fractionalStart: $.range(0.5, 10),
    // @ts-expect-error - n must be a positive integer
    firstZero: $.firstN({ input: "$tags", n: 0 }),
    // @ts-expect-error - n must be a positive integer
    lastNegative: $.lastN({ input: "$tags", n: -1 }),
    // @ts-expect-error - n must be an integer
    maxFractional: $.maxN({ input: "$tags", n: 1.5 }),
    // @ts-expect-error - n must be a positive integer
    minZero: $.minN({ input: "$tags", n: 0 }),
    // @ts-expect-error - bitwise operands must be integers
    bitAndFractional: $.bitAnd("$flags", 1.5),
    // @ts-expect-error - bitwise operands must be integers
    bitNotFractional: $.bitNot(0.5),
  })),
);

docs.aggregate(
  $group($ => ({
    _id: null,
    // @ts-expect-error - n must be a positive integer
    topZero: $.topN({ output: "$num", sortBy: { num: -1 }, n: 0 }),
    // @ts-expect-error - n must be a positive integer
    bottomNegative: $.bottomN({ output: "$num", sortBy: { num: -1 }, n: -2 }),
  })),
);
