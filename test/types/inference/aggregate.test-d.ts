// Type tests for the new callback-based aggregation API
import { Schema as S } from "@effect/schema";
import { ObjectId } from "bson";
import type { Db } from "mongodb";
import { expectType } from "tsd";

import {
  $addFields,
  $count,
  $facet,
  $group,
  $limit,
  $lookup,
  $match,
  $project,
  $skip,
  $sort,
  $unwind,
  collection,
} from "../../../src/sluice.js";
import type { SimplifyWritable } from "../../../src/type-utils.js";

const ObjectIdSchema = S.instanceOf(ObjectId);

// ============================================
// TEST SCHEMAS
// ============================================

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  email: S.String,
  age: S.Number,
  active: S.Boolean,
  department: S.String,
  tags: S.Array(S.String),
  address: S.Struct({
    city: S.String,
    zip: S.Number,
  }),
});

const OrderSchema = S.Struct({
  _id: ObjectIdSchema,
  userId: S.String,
  amount: S.Number,
  status: S.Literal("pending", "completed"),
});

const OrganizationSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  houses: S.Array(
    S.Struct({
      id: S.String,
      name: S.String,
      keychain: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
          keys: S.Array(
            S.Struct({
              id: S.String,
              appName: S.String,
            }),
          ),
        }),
      ),
    }),
  ),
});

const mockDb = {} as Db;

const users = collection("users", UserSchema, mockDb.collection("users"));
const orders = collection("orders", OrderSchema, mockDb.collection("orders"));
const orgs = collection("orgs", OrganizationSchema, mockDb.collection("orgs"));

type User = SimplifyWritable<typeof UserSchema.Type>;
type Order = SimplifyWritable<typeof OrderSchema.Type>;

// ============================================
// TEST: Basic aggregate with single stage
// ============================================

const basic1 = users.aggregate($match($ => ({ active: true }))).toList();

// Type should be preserved after $match
type Basic1Current = Awaited<typeof basic1>[number];
expectType<User>({} as Basic1Current);

// ============================================
// TEST: $match with field ref - Type-safe comparison
// ============================================

const basic2 = users.aggregate($match($ => ({ $expr: $.eq("$active", true) }))).toList();

const basic3 = users.aggregate($match($ => ({ $expr: $.eq("$name", "test") }))).toList();

const basicFail1 = users
  .aggregate(
    $match($ => ({
      // @ts-expect-error - boolean field compared to string literal
      $expr: $.eq("$active", "true"),
    })),
  )
  .toList();

const basicFail2 = users
  .aggregate(
    $match($ => ({
      // @ts-expect-error - string literal not compatible with numeric field
      $expr: $.eq("123", "$age"),
    })),
  )
  .toList();

const basicFail3 = users
  .aggregate(
    $match($ => ({
      // @ts-expect-error - "$nam" is not a valid field
      $expr: $.eq("$nam", "test"),
    })),
  )
  .toList();

type Basic2Current = Awaited<typeof basic2>[number];
expectType<User>({} as Basic2Current);

// ============================================
// TEST: $project transforms type (without _id inclusion)
// ============================================

const project1 = users
  .aggregate(
    $match($ => ({ active: true })),
    $project($ => ({
      _id: 1,
      name: 1,
      department: 1,
      age: 1,
    })),
  )
  .toList();

type Project1Current = Awaited<typeof project1>[number];
expectType<{ _id: ObjectId; name: string; department: string; age: number }>({} as Project1Current);

// ============================================
// TEST: $project with computed fields
// ============================================

const project2 = users
  .aggregate(
    $project($ => ({
      _id: 1,
      name: 1,
      doubled: $.multiply("$age", 2),
    })),
  )
  .toList();

type Project2Current = Awaited<typeof project2>[number];
expectType<{ _id: ObjectId; name: string; doubled: number }>({} as Project2Current);

// ============================================
// TEST: $group transforms type
// ============================================

const group1 = users
  .aggregate(
    $match($ => ({ active: true })),
    $group($ => ({
      _id: "$department",
      count: $.sum(1),
      avgAge: $.avg("$age"),
    })),
  )
  .toList();

type Group1Current = Awaited<typeof group1>[number];
expectType<{ _id: string; count: number; avgAge: number }>({} as Group1Current);

// ============================================
// TEST: $group with compound _id
// Note: compound _id requires literal object syntax
// ============================================

const group2 = users
  .aggregate(
    $group($ => ({
      _id: {
        dept: "$department",
        active: "$active",
      },
      count: $.sum(1),
    })),
  )
  .toList();

// The _id type resolves based on the field refs
type Group2Current = Awaited<typeof group2>[number];

// ============================================
// TEST: $group with null _id (aggregate all)
// ============================================

const group3 = users
  .aggregate(
    $group($ => ({
      _id: null,
      total: $.sum(1),
    })),
  )
  .toList();

type Group3Current = Awaited<typeof group3>[number];
expectType<{ _id: null; total: number }>({} as Group3Current);

// ============================================
// TEST: $addFields preserves and extends type
// Note: $addFields takes expressions - field refs or Ret types from builder
// For computed values, we need to use the builder in a $project or raw field refs
// ============================================

const addFields1 = users
  .aggregate(
    $addFields($ => ({
      nameBackup: "$name", // field ref copies value
      ageBackup: "$age", // field ref copies value
    })),
  )
  .toList();

type AddFields1Current = Awaited<typeof addFields1>[number];
// Should have all User fields + new fields
expectType<string>({} as AddFields1Current["name"]);
expectType<string>({} as AddFields1Current["nameBackup"]);
expectType<number>({} as AddFields1Current["ageBackup"]);

// ============================================
// TEST: $unwind transforms array to element
// ============================================

const unwind1 = users.aggregate($unwind("$tags")).toList();

type Unwind1Current = Awaited<typeof unwind1>[number];
// tags should be string, not string[]
expectType<string>({} as Unwind1Current["tags"]);

// ============================================
// TEST: $unwind with nested array path
// ============================================

const unwind2 = orgs.aggregate($unwind("$houses")).toList();

type Unwind2Current = Awaited<typeof unwind2>[number];
// houses should be unwound to single element
expectType<{
  id: string;
  name: string;
  keychain: { id: string; name: string; keys: { id: string; appName: string }[] }[];
}>({} as Unwind2Current["houses"]);

// ============================================
// TEST: $sort validates against current shape
// ============================================

const sort1 = users
  .aggregate(
    $project($ => ({
      _id: 1,
      name: 1,
      age: 1,
    })),
    $sort({ age: -1 }),
  )
  .toList();

type Sort1Current = Awaited<typeof sort1>[number];
expectType<{ _id: ObjectId; name: string; age: number }>({} as Sort1Current);

// ============================================
// TEST: $limit and $skip preserve type
// ============================================

const limitSkip1 = users.aggregate($sort({ age: -1 }), $skip(10), $limit(5)).toList();

type LimitSkip1Current = Awaited<typeof limitSkip1>[number];
expectType<User>({} as LimitSkip1Current);

// ============================================
// TEST: $count transforms to count doc
// ============================================

const count1 = users
  .aggregate(
    $match($ => ({ active: true })),
    $count("totalActive"),
  )
  .toList();

type Count1Current = Awaited<typeof count1>[number];
expectType<{ totalActive: number }>({} as Count1Current);

// ============================================
// TEST: $lookup with basic fields
// ============================================

const lookup1 = users
  .aggregate(
    $lookup({
      from: orders,
      localField: "_id",
      foreignField: "userId",
      as: "orders",
    }),
  )
  .toList();

type Lookup1Current = Awaited<typeof lookup1>[number];
expectType<Order[]>({} as Lookup1Current["orders"]);

// ============================================
// TEST: $lookup with let (correlated subquery)
// ============================================

// $lookup with let - variables from local doc are accessible as $$varName in pipeline
const lookup2 = users
  .aggregate(
    $lookup({
      from: orders,
      let: {
        localUserId: "$_id", // string from User
        minAge: "$age", // number from User
      },
      pipeline: $ =>
        $.pipe(
          // $$localUserId is typed as string, $$minAge is typed as number
          $match($ => ({
            $expr: $.and(
              $.eq("$userId", "$$localUserId"), // Order.userId == User._id
              $.gt("$amount", "$$minAge"), // Order.amount > User.age (contrived but tests typing)
            ),
          })),
        ),
      as: "matchedOrders",
    }),
  )
  .toList();

type Lookup2Current = Awaited<typeof lookup2>[number];
expectType<
  SimplifyWritable<{
    _id: ObjectId;
    userId: string;
    amount: number;
    status: "completed" | "pending";
  }>[]
>({} as Lookup2Current["matchedOrders"]);

// Test: $$localUserId should be typed as string - using wrong type should error
const lookup3 = users
  .aggregate(
    $lookup({
      from: orders,
      let: { localUserId: "$_id" },
      pipeline: $ =>
        $.pipe(
          $match($ => ({
            // @ts-expect-error - $$localUserId is string, cannot compare with number literal
            $expr: $.eq("$$localUserId", 123),
          })),
        ),
      as: "orders",
    }),
  )
  .toList();

// ============================================
// TEST: $lookup uncorrelated subquery (no let, no localField/foreignField)
// ============================================

// Uncorrelated subquery - just runs pipeline on foreign collection
const lookup4 = users
  .aggregate(
    $lookup({
      from: orders,
      pipeline: $ =>
        $.pipe(
          $match($ => ({ status: "completed" })),
          $project($ => ({
            _id: 1,
            amount: $.multiply("$amount", 100),
          })),
        ),
      as: "allCompletedOrders",
    }),
  )
  .toList();

type Lookup4Current = Awaited<typeof lookup4>[number];
// The pipeline transforms Order to { _id: ObjectId, amount: number }
expectType<{ _id: ObjectId; amount: number }[]>({} as Lookup4Current["allCompletedOrders"]);

// ============================================
// TEST: $facet with multiple sub-pipelines
// ============================================

const facet1 = users
  .aggregate(
    $facet($ => ({
      adults: $.pipe(
        $match($ => ({ age: { $gte: 18 } })),
        $limit(10),
      ),
      byDepartment: $.pipe(
        $group($ => ({
          _id: "$department",
          count: $.sum(1),
        })),
      ),
    })),
  )
  .toList();

type Facet1Current = Awaited<typeof facet1>[number];
expectType<User[]>({} as Facet1Current["adults"]);
expectType<{ _id: string; count: number }[]>({} as Facet1Current["byDepartment"]);

// ============================================
// TEST: Complex multi-stage pipeline
// ============================================

const complex1 = users
  .aggregate(
    $match($ => ({ active: true })),
    $project($ => ({
      _id: 1,
      name: 1,
      department: 1,
      age: 1,
      ageGroup: $.switch({
        branches: [
          {
            case: $.lt("$age", 18),
            then: "minor",
          },
          {
            case: $.lt("$age", 65),
            then: "adult",
          },
        ],
        default: "senior",
      }),
    })),
    $group($ => ({
      _id: {
        dept: "$department",
        ageGroup: "$ageGroup",
      },
      count: $.sum(1),
      avgAge: $.avg("$age"),
    })),
    $sort({ count: -1 }),
    $limit(20),
  )
  .toList();

// ============================================
// TEST: switch literal inference (no as const)
// ============================================

const switchLiterals = users
  .aggregate(
    $project($ => ({
      _id: 1,
      ageBand: $.switch({
        branches: [
          {
            case: $.lt("$age", 18),
            then: "minor",
          },
          {
            case: $.lt("$age", 65),
            then: "adult",
          },
        ],
        default: "senior",
      }),
    })),
  )
  .toList();

type SwitchLiterals = Awaited<typeof switchLiterals>[number];
expectType<"minor" | "adult" | "senior">({} as SwitchLiterals["ageBand"]);

// ============================================
// TEST: Type errors are caught
// ============================================

// Invalid field name is caught
const invalidMatch = users
  .aggregate(
    // @ts-expect-error - "activ" is not a valid field (typo)
    $match($ => ({ activ: true })),
  )
  .toList();

// Cannot unwind non-array fields
const invalidUnwind = users
  .aggregate(
    // $unwind correctly rejects "$name" since it's not an array field
    // @ts-expect-error - "$name" is not an array
    $unwind("$name"),
  )
  .toList();

// Cannot sort by invalid field after project
const invalidSort = users
  .aggregate(
    $project($ => ({
      _id: 1,
      name: 1,
    })),
    // @ts-expect-error - "age" doesn't exist after project
    $sort({ age: 1 }),
  )
  .toList();

// ============================================
// TEST: Pipeline-aware field validation
// ============================================

// Sort validates against projected shape
const validatedSort = users
  .aggregate(
    $project($ => ({
      _id: 1,
      orgId: "$department",
    })),
    $sort({ orgId: 1 }), // orgId exists ✅
  )
  .toList();

// After project, original fields don't exist
const invalidSortAfterProject = users
  .aggregate(
    $project($ => ({
      _id: 1,
      orgId: "$department",
    })),
    // @ts-expect-error - "department" doesn't exist after project
    $sort({ department: 1 }),
  )
  .toList();
