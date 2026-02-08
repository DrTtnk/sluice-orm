/**
 * Additional type inference tests covering gaps identified in review:
 * - $replaceRoot / $replaceWith output typing
 * - $graphLookup output typing
 * - $sample type preservation
 * - $redact type preservation
 * - Expression operator error detection (negative tests)
 * - CRUD new operations type inference
 */
import { Schema as S } from "@effect/schema";
import type { ObjectId } from "bson";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";
import { expectType } from "tsd";

import {
  $addFields,
  $graphLookup,
  $project,
  $redact,
  $replaceRoot,
  $replaceWith,
  $sample,
  $sort,
  $unset,
  collection,
} from "../../../src/sluice.js";
import type { SimplifyWritable } from "../../../src/type-utils.js";

const ObjectIdSchema = S.instanceOf(_ObjectId);

// ============================================
// SCHEMAS
// ============================================

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  profile: S.Struct({
    bio: S.String,
    avatar: S.String,
  }),
  tags: S.Array(S.String),
});

const CategorySchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  parentId: S.NullOr(ObjectIdSchema),
  level: S.Number,
});

const mockDb = {} as Db;
const users = collection("users", UserSchema, mockDb.collection("users"));
const categories = collection("categories", CategorySchema, mockDb.collection("categories"));

type User = SimplifyWritable<typeof UserSchema.Type>;
type Category = SimplifyWritable<typeof CategorySchema.Type>;

// ============================================
// $replaceRoot - output should be the newRoot type
// ============================================

// Field ref: replaces root with nested object
const rr1 = users.aggregate($replaceRoot({ newRoot: "$profile" })).toList();
type RR1 = Awaited<typeof rr1>[number];
expectType<{ bio: string; avatar: string }>({} as RR1);

// Callback with builder expression
const rr2 = users
  .aggregate(
    $replaceRoot($ => ({
      newRoot: $.mergeObjects("$profile", { extraField: "$name" }),
    })),
  )
  .toList();

// ============================================
// $replaceWith - output should be the expression type
// ============================================

const rw1 = users.aggregate($replaceWith("$profile")).toList();
type RW1 = Awaited<typeof rw1>[number];
expectType<{ bio: string; avatar: string }>({} as RW1);

// ============================================
// $sample - type should be preserved
// ============================================

const sample1 = users.aggregate($sample({ size: 5 })).toList();
type Sample1 = Awaited<typeof sample1>[number];
expectType<User>({} as Sample1);

// ============================================
// $redact - type should be preserved
// ============================================

const redact1 = users
  .aggregate($redact($ => $.cond({ if: $.eq("$age", 18), then: "$$KEEP", else: "$$PRUNE" })))
  .toList();
type Redact1 = Awaited<typeof redact1>[number];
expectType<User>({} as Redact1);

// ============================================
// $graphLookup - output adds array field
// ============================================

const gl1 = categories
  .aggregate(
    $graphLookup({
      from: categories,
      startWith: "$parentId",
      connectFromField: "parentId",
      connectToField: "_id",
      as: "ancestors",
      maxDepth: 10,
    }),
  )
  .toList();

type GL1 = Awaited<typeof gl1>[number];
expectType<Category[]>({} as GL1["ancestors"]);
expectType<string>({} as GL1["name"]); // original fields preserved

// $graphLookup with depthField
const gl2 = categories
  .aggregate(
    $graphLookup({
      from: categories,
      startWith: "$parentId",
      connectFromField: "parentId",
      connectToField: "_id",
      as: "ancestors",
      depthField: "depth",
    }),
  )
  .toList();

type GL2 = Awaited<typeof gl2>[number];
expectType<number>({} as GL2["ancestors"][0]["depth"]); // depthField adds number field

// ============================================
// $unset - removes fields from type
// ============================================

const unset1 = users.aggregate($unset("tags", "profile")).toList();
type Unset1 = Awaited<typeof unset1>[number];
expectType<string>({} as Unset1["name"]);
expectType<number>({} as Unset1["age"]);
// @ts-expect-error - tags should be removed
type _Fail1 = Unset1["tags"];
// @ts-expect-error - profile should be removed
type _Fail2 = Unset1["profile"];

// ============================================
// Expression operator error detection (negative tests)
// ============================================

// $.multiply with string field (should error)
const exprFail1 = users
  .aggregate(
    $project($ => ({
      _id: 1,
      // @ts-expect-error - cannot multiply a string field
      bad: $.multiply("$name", 2),
    })),
  )
  .toList();

// $.concat with numeric field (should error)
const exprFail2 = users
  .aggregate(
    $project($ => ({
      _id: 1,
      // @ts-expect-error - cannot concat a numeric field
      bad: $.concat(["$age", " years"]),
    })),
  )
  .toList();

// $.subtract with string fields (should error)
const exprFail3 = users
  .aggregate(
    $project($ => ({
      _id: 1,
      // @ts-expect-error - cannot subtract strings
      bad: $.subtract("$name", "$name"),
    })),
  )
  .toList();

// Invalid sort key after $project
const sortFail = users
  .aggregate(
    $project($ => ({ _id: 1, name: 1 })),
    // @ts-expect-error - age is no longer in the type after project
    $sort({ age: -1 }),
  )
  .toList();

// ============================================
// $addFields / $set type extends: new fields visible downstream
// ============================================

const af1 = users
  .aggregate(
    $addFields($ => ({
      computed: $.multiply("$age", 2),
    })),
    $project($ => ({
      _id: 1,
      name: 1,
      computed: 1,
    })),
  )
  .toList();

type AF1 = Awaited<typeof af1>[number];
expectType<{ _id: ObjectId; name: string; computed: number }>({} as AF1);

// ============================================
// CRUD type inference for new methods
// ============================================

// countDocuments returns number
const countResult = users.countDocuments().execute();
expectType<Promise<number>>(countResult);

// estimatedDocumentCount returns number
const estResult = users.estimatedDocumentCount().execute();
expectType<Promise<number>>(estResult);

// distinct returns array of the field type
const distinctNames = users.distinct("name").execute();
expectType<Promise<string[]>>(distinctNames);

const distinctAges = users.distinct("age").execute();
expectType<Promise<number[]>>(distinctAges);

// findOneAndDelete returns T | null
const foadResult = users.findOneAndDelete($ => ({ name: "test" })).execute();
expectType<Promise<User | null>>(foadResult);

// findOneAndReplace returns T | null
const foarResult = users.findOneAndReplace($ => ({ name: "test" }), {} as User).execute();
expectType<Promise<User | null>>(foarResult);

// findOneAndUpdate returns T | null
const foauResult = users.findOneAndUpdate($ => ({ name: "test" }), { $set: { age: 25 } }).execute();
expectType<Promise<User | null>>(foauResult);
