import { Schema as S } from "@effect/schema";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";
import { expectType } from "tsd";

import {
  $bucket,
  $changeStream,
  $collStats,
  $documents,
  $geoNear,
  $lookup,
  $match,
  $setWindowFields,
  $sort,
  $unwind,
  collection,
} from "../../../src/sluice.js";
import type { SimplifyWritable } from "../../../src/type-utils.js";

const ObjectIdSchema = S.instanceOf(_ObjectId);

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  tags: S.Array(S.String),
  location: S.Struct({
    type: S.Literal("Point"),
    coordinates: S.Tuple(S.Number, S.Number),
  }),
});

type User = SimplifyWritable<typeof UserSchema.Type>;

const OrderSchema = S.Struct({
  _id: ObjectIdSchema,
  userId: S.String,
  amount: S.Number,
});

type Order = SimplifyWritable<typeof OrderSchema.Type>;

const mockDb = {} as Db;
const users = collection("users", UserSchema, mockDb.collection("users"));
const orders = collection("orders", OrderSchema, mockDb.collection("orders"));

// $sort should allow $meta sort values
const sortWithMeta = users.aggregate($sort({ name: { $meta: "textScore" } })).toList();
expectType<User>({} as Awaited<typeof sortWithMeta>[number]);

// $lookup pipeline array should be typed
const lookupWithPipelineArray = users
  .aggregate(
    $lookup({
      from: orders,
      let: { uid: "$_id" },
      pipeline: $ => $.pipe($match($ => ({ $expr: $.eq("$userId", "$$uid") }))),
      as: "orders",
    }),
  )
  .toList();

type LookupDoc = Awaited<typeof lookupWithPipelineArray>[number];
expectType<Order[]>({} as LookupDoc["orders"]);

// $documents should preserve literal doc types
const docsStage = $documents([
  { a: 1, b: "x" },
  { a: 2, b: "y" },
]);
expectType<{ a: 1; b: "x" } | { a: 2; b: "y" }>({} as typeof docsStage._current);

// $collStats count should be required when requested
const collStatsWithCount = users.aggregate($collStats({ count: {} })).toList();
expectType<number>({} as Awaited<typeof collStatsWithCount>[number]["count"]);

// $geoNear should add distanceField/includeLocs output
const geoNear = users
  .aggregate(
    $geoNear({
      near: { type: "Point", coordinates: [0, 0] },
      distanceField: "distance",
      includeLocs: "loc",
    }),
  )
  .toList();

type GeoNearDoc = Awaited<typeof geoNear>[number];
expectType<number>({} as GeoNearDoc["distance"]);
expectType<{ type: "Point"; coordinates: [number, number] }>({} as GeoNearDoc["loc"]);

// $setWindowFields avg should be non-null with non-null input
const windowAvg = users
  .aggregate(
    $setWindowFields($ => ({
      sortBy: { age: 1 },
      output: {
        avgAge: { $avg: "$age" },
      },
    })),
  )
  .toList();

type WindowAvgDoc = Awaited<typeof windowAvg>[number];
expectType<number>({} as WindowAvgDoc["avgAge"]);

// $bucket boundaries should be strictly increasing
void users
  .aggregate(
    $bucket({
      groupBy: "$age",
      // @ts-expect-error boundaries must be increasing
      boundaries: [0, 10, 5],
      output: $ => ({ count: $.sum(1) }),
    }),
  )
  .toList();

// $changeStream should be callable
const changeStream = users.aggregate($changeStream({})).toList();
expectType<Awaited<typeof changeStream>[number]>({} as Awaited<typeof changeStream>[number]);

// $unwind with preserveNullAndEmptyArrays should reflect nullable element
const unwindPreserve = users
  .aggregate($unwind({ path: "$tags", preserveNullAndEmptyArrays: true }))
  .toList();
expectType<string | null | undefined>({} as Awaited<typeof unwindPreserve>[number]["tags"]);
