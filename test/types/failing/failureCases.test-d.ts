/**
 * Failure case tests - verify the type system catches errors
 */
import { Schema as S } from "@effect/schema";
import { ObjectId } from "bson";
import type { Db } from "mongodb";
import { expectType } from "tsd";

import {
  $addFields,
  $bucket,
  $group,
  $lookup,
  $match,
  $merge,
  $out,
  $project,
  $setWindowFields,
  $sort,
  $unwind,
  type Agg,
  type CallbackOnlyError,
  collection,
} from "../../../src/sluice.js";
import type { SimplifyWritable } from "../../../src/type-utils.js";

const ObjectIdSchema = S.instanceOf(ObjectId);

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  email: S.String,
  role: S.Literal("admin", "user"),
  active: S.Boolean,
  tags: S.Array(S.String),
  metadata: S.Struct({
    created: S.Date,
    score: S.Number,
  }),
});

const OrderSchema = S.Struct({
  _id: ObjectIdSchema,
  userId: ObjectIdSchema,
  total: S.Number,
});

const EventSchema = S.Struct({
  _id: ObjectIdSchema,
  createdAt: S.Date,
  value: S.Number,
  group: S.Literal("a", "b"),
});

type User = SimplifyWritable<typeof UserSchema.Type>;
type Order = SimplifyWritable<typeof OrderSchema.Type>;
type Event = SimplifyWritable<typeof EventSchema.Type>;

const mockDb = {} as Db;

const users = collection("users", UserSchema, mockDb.collection("users"));
const orders = collection("orders", OrderSchema, mockDb.collection("orders"));
const events = collection("events", EventSchema, mockDb.collection("events"));

// ============================================
// INVALID COLLECTION TARGETS
// ============================================

const invalidMergeIntoString = users
  .aggregate(
    // @ts-expect-error: $merge.into must be a collection ref
    $merge({ into: "users" }),
  )
  .toList();

const invalidOutString = users
  .aggregate(
    // @ts-expect-error: $out requires a collection ref
    $out("users"),
  )
  .toList();

const invalidMergeIntoMismatched = users
  .aggregate(
    // @ts-expect-error: collection type mismatch
    $merge({ into: orders }),
  )
  .toList();

const invalidOutMismatched = users
  .aggregate(
    // @ts-expect-error: collection type mismatch
    $out(orders),
  )
  .toList();

// ============================================
// INVALID FIELD REFERENCES
// ============================================

// Invalid field name in match - caught by callback signature
// The callback approach means these all fail in different ways

// Invalid field reference in project - using unknown field
// FIXED: The type system now correctly rejects invalid field refs
const invalidProjectField = users
  .aggregate(
    $project($ => ({
      _id: 1,
      // @ts-expect-error: notAField doesn't exist on User
      invalid: "$notAField",
    })),
  )
  .toList();

// Invalid $type alias
const invalidTypeAlias = users
  .aggregate(
    $match($ => ({
      // @ts-expect-error: $type must be a BSON type alias
      age: { $type: "notAType" },
    })),
  )
  .toList();

// $elemMatch on non-array field
const invalidElemMatchOnScalar = users
  .aggregate(
    $match($ => ({
      // @ts-expect-error: $elemMatch only allowed on array fields
      age: $.elemMatch($ => ({ $gt: 18 })),
    })),
  )
  .toList();

// ============================================
// TYPE MISMATCHES VIA OPERATORS
// ============================================

// Using string field with numeric operator
const invalidNumericOp = users
  .aggregate(
    $project($ => ({
      _id: 1,
      // @ts-expect-error: $name is string, not NumericArg
      invalid: $.add("$name", 1),
    })),
  )
  .toList();

// Using number field with string operator
// FIXED: toUpper now correctly rejects non-string fields
const invalidStringOp = users
  .aggregate(
    $project($ => ({
      _id: 1,
      // @ts-expect-error: $age is number, not StringArg
      invalid: $.toUpper("$age"),
    })),
  )
  .toList();

// Using non-array field with array operator
const invalidArrayOp = users
  .aggregate(
    $project($ => ({
      _id: 1,
      // @ts-expect-error: $name is string, not ArrayArg
      invalid: $.size("$name"),
    })),
  )
  .toList();

// Using non-array input with map
const invalidMapInput = users
  .aggregate(
    $project($ => ({
      _id: 1,
      badMap: $.map({
        // @ts-expect-error: $name is string, not ArrayArg
        input: "$name",
        in: "$$this",
      }),
    })),
  )
  .toList();

// Using non-array input with filter
const invalidFilterInput = users
  .aggregate(
    $project($ => ({
      _id: 1,
      badFilter: $.filter({
        // @ts-expect-error: $name is string, not ArrayArg
        input: "$name",
        cond: $ => true,
      }),
    })),
  )
  .toList();

// Using non-array input with reduce
const invalidReduceInput = users
  .aggregate(
    $project($ => ({
      _id: 1,
      badReduce: $.reduce({
        // @ts-expect-error: $name is string, not ArrayArg
        input: "$name",
        initialValue: "",
        in: $ => "$$this",
      }),
    })),
  )
  .toList();

// Invalid set operators on non-array
const invalidSetUnionInput = users
  .aggregate(
    $project($ => ({
      _id: 1,
      // @ts-expect-error: $name is string, not ArrayArg
      badSetUnion: $.setUnion("$name", "$tags"),
    })),
  )
  .toList();

// Invalid string operator input
const invalidRegexMatchInput = users
  .aggregate(
    $project($ => ({
      _id: 1,
      badRegex: $.regexMatch({
        // @ts-expect-error: $age is number, not StringArg
        input: "$age",
        regex: "^[0-9]+$",
      }),
    })),
  )
  .toList();

// Invalid date operator input
const invalidDateAddInput = users
  .aggregate(
    $project($ => ({
      _id: 1,
      badDate: $.dateAdd({
        // @ts-expect-error: $name is string, not DateArg
        startDate: "$name",
        unit: "day",
        amount: 1,
      }),
    })),
  )
  .toList();

// Invalid lookup field paths
const invalidLookupLocalField = users
  .aggregate(
    $lookup({
      from: orders,
      // @ts-expect-error: notAField does not exist on User
      localField: "notAField",
      foreignField: "userId",
      as: "orders",
    }),
  )
  .toList();

// ============================================
// INVALID WINDOW OUTPUTS
// ============================================

const invalidWindowOutput = users
  .aggregate(
    $setWindowFields($ => ({
      partitionBy: "$role",
      sortBy: { age: 1 },
      output: {
        // @ts-expect-error: output must be a window operator spec
        priceVsAvg: "$age",
      },
    })),
  )
  .toList();

// Missing window bounds for $derivative
const invalidDerivativeWithoutWindow = events
  .aggregate(
    $setWindowFields($ => ({
      partitionBy: "$group",
      sortBy: { createdAt: 1 },
      output: {
        // @ts-expect-error: $derivative requires window bounds
        slope: {
          $derivative: {
            input: "$value",
            unit: "day",
          },
        },
      },
    })),
  )
  .toList();

// Missing unit for range window with date sortBy
const invalidRangeWithoutUnit = events
  .aggregate(
    // @ts-expect-error: range windows on date sortBy require unit
    $setWindowFields($ => ({
      partitionBy: "$group",
      sortBy: { createdAt: 1 },
      output: {
        rollingMin: {
          $min: "$value",
          window: { range: ["unbounded", 0] },
        },
      },
    })),
  )
  .toList();

// Rank-based operators require sortBy
const invalidRankWithoutSortBy = events
  .aggregate(
    // @ts-expect-error: sortBy required for $rank
    $setWindowFields($ => ({
      partitionBy: "$group",
      output: {
        rank: { $rank: {} },
        docNum: { $documentNumber: {} },
      },
    })),
  )
  .toList();

// sortBy must use top-level fields only
const invalidWindowSortByNestedField = users
  .aggregate(
    $setWindowFields($ => ({
      partitionBy: "$role",
      // @ts-expect-error: nested field paths are not allowed in sortBy
      sortBy: { "metadata.created": 1 },
      output: { docNum: { $documentNumber: {} } },
    })),
  )
  .toList();

// ============================================
// INVALID SORT KEYS
// ============================================

// Sort key not in document
const invalidSortKey = users
  .aggregate(
    // @ts-expect-error: notAField not in User keys
    $sort({ notAField: 1 }),
  )
  .toList();

// Sort must use 1 or -1
const invalidSortValue = users
  .aggregate(
    // @ts-expect-error: 2 is not valid sort direction
    $sort({ age: 2 }),
  )
  .toList();

// ============================================
// PIPELINE SHAPE TRACKING - After $group
// ============================================

// After $group, original fields should not be accessible
 
const afterGroup = users.aggregate(
  $group($ => ({
    _id: "$role",
    avgAge: $.avg("$age"),
  })),
);

type AfterGroupShape = typeof afterGroup extends Agg<User, infer C> ? C : never;
// These should be the only fields available:
expectType<"admin" | "user">({} as AfterGroupShape["_id"]);
expectType<number>({} as AfterGroupShape["avgAge"]);

// Trying to sort by original field 'name' after group - this should fail
// because 'name' doesn't exist in the grouped shape
const tryingOriginalFieldAfterGroup = afterGroup
  .pipe(
    // @ts-expect-error: name doesn't exist after $group
    $sort({ name: 1 }),
  )
  .toList();

// ============================================
// PIPELINE SHAPE TRACKING - After $project
// ============================================

// After $project with inclusion, excluded fields should not be accessible
 
const afterProject = users.aggregate(
  $project($ => ({
    _id: 1,
    name: 1,
  })),
);

type AfterProjectShape = typeof afterProject extends Agg<User, infer C> ? C : never;
// Only _id and name should exist
expectType<ObjectId>({} as AfterProjectShape["_id"]);
expectType<string>({} as AfterProjectShape["name"]);

// Trying to sort by excluded field 'age' after project - should fail
const tryingExcludedField = afterProject
  .pipe(
    // @ts-expect-error: age doesn't exist after $project
    $sort({ age: 1 }),
  )
  .toList();

// ============================================
// ADDFIELDS TYPE INFERENCE
// ============================================

// addFields should correctly infer field ref types
const addFieldsInference = users
  .aggregate(
    $addFields($ => ({
      copiedName: "$name",
      copiedAge: "$age",
      copiedTags: "$tags",
    })),
  )
  .toList();

type AddFieldsShape = Awaited<typeof addFieldsInference>[number];
expectType<string>({} as AddFieldsShape["copiedName"]);
expectType<number>({} as AddFieldsShape["copiedAge"]);
expectType<string[]>({} as AddFieldsShape["copiedTags"]);

// ============================================
// LOOKUP TYPE SAFETY
// ============================================

// $lookup infers types from Collection - no explicit type params needed
const lookupWithProperRef = users
  .aggregate(
    $lookup({
      from: orders,
      localField: "_id",
      foreignField: "userId",
      as: "userOrders",
    }),
  )
  .toList();

type LookupResultShape = Awaited<typeof lookupWithProperRef>[number];
expectType<Order[]>({} as LookupResultShape["userOrders"]);
// Original fields should still exist
expectType<string>({} as LookupResultShape["name"]);

// ============================================
// UNWIND TYPE NARROWING
// ============================================

const unwoundPipeline = users.aggregate($unwind({ path: "$tags" })).toList();

type UnwoundShape = Awaited<typeof unwoundPipeline>[number];
// After unwind, tags should be string, not string[]
expectType<string>({} as UnwoundShape["tags"]);
// Other fields should remain unchanged
expectType<string>({} as UnwoundShape["name"]);
expectType<number>({} as UnwoundShape["age"]);

// ============================================
// $EXPR IN MATCH
// ============================================

// $expr should correctly propagate context
const matchWithExpr = users.aggregate($match($ => ({ $expr: $.gt("$age", 18) }))).toList();

type MatchExprShape = Awaited<typeof matchWithExpr>[number];
// Match doesn't change shape, should still have User fields
expectType<string>({} as MatchExprShape["name"]);
expectType<number>({} as MatchExprShape["age"]);

// ============================================
// CALLBACK-ONLY ENFORCEMENT
// ============================================

const invalidMapUsage = users
  .aggregate(
    $project($ => ({
      _id: 1,
      badMap: $.map({
        input: "$tags",
        in: "$$this",
      }),
    })),
  )
  .toList();
type InvalidMapShape = Awaited<typeof invalidMapUsage>[number];
expectType<string[]>({} as InvalidMapShape["badMap"]);

const invalidFilterUsage = users
  .aggregate(
    $project($ => ({
      _id: 1,
      badFilter: $.filter({
        input: "$tags",
        cond: true,
      }),
    })),
  )
  .toList();
type InvalidFilterShape = Awaited<typeof invalidFilterUsage>[number];
expectType<CallbackOnlyError<"filter">>({} as InvalidFilterShape["badFilter"]);

const invalidReduceUsage = users
  .aggregate(
    $project($ => ({
      _id: 1,
      badReduce: $.reduce({
        input: "$tags",
        initialValue: 0,
        in: "$$value",
      }),
    })),
  )
  .toList();
type InvalidReduceShape = Awaited<typeof invalidReduceUsage>[number];
expectType<CallbackOnlyError<"reduce">>({} as InvalidReduceShape["badReduce"]);

const invalidBucketOutput = users
  .aggregate(
    $bucket({
      groupBy: "$age",
      boundaries: [0, 18, 65, 120],
      output: $ => ({
        count: $.sum(1), // Valid accumulator expression using builder API
      }),
    }),
  )
  .toList();
type ValidBucketShape = Awaited<typeof invalidBucketOutput>[number];
expectType<{ _id: 0 | 18 | 65 | 120; count: number }>({} as ValidBucketShape);

const invalidLookupPipeline = users
  .aggregate(
    $lookup({
      from: orders,
      localField: "_id",
      foreignField: "userId",
      as: "userOrders",
      pipeline: [{ $match: { userId: { $exists: true } } }],
    }),
  )
  .toList();
type InvalidLookupShape = Awaited<typeof invalidLookupPipeline>[number];
expectType<CallbackOnlyError<"$lookup.pipeline">>({} as InvalidLookupShape);
