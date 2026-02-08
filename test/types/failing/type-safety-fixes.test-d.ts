 
/**
 * Type tests for all type safety fixes:
 * 1. $not per-field modeling (not top-level)
 * 2. String comparison operators ($gt/$gte/$lt/$lte)
 * 3. $options requires $regex
 * 4. Accumulator bare string rejection
 * 5. Update pipeline stage branding
 * 6. $replaceRoot field ref validation
 */
import { Schema as S } from "@effect/schema";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";

import { update } from "../../../src/crud/updates/stages/index.js";
import {
  $group,
  $match,
  $replaceRoot,
  $sort,
  $unwind,
  collection,
} from "../../../src/sluice.js";
import type { SimplifyWritable } from "../../../src/type-utils.js";

const ObjectIdSchema = S.instanceOf(_ObjectId);

// ============================================
// TEST SCHEMAS
// ============================================

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  email: S.String,
  active: S.Boolean,
  createdAt: S.Date,
  tags: S.Array(S.String),
  profile: S.Struct({
    bio: S.String,
    avatar: S.String,
  }),
  metadata: S.Struct({
    created: S.Date,
    score: S.Number,
  }),
  location: S.Struct({
    type: S.Literal("Point"),
    coordinates: S.Tuple(S.Number, S.Number),
  }),
});

type User = SimplifyWritable<typeof UserSchema.Type>;

const mockDb = {} as Db;
const users = collection("users", UserSchema, mockDb.collection("users"));

// ============================================
// 1. $not PER-FIELD MODELING
// ============================================

// VALID: $not as a per-field operator
const valid_not_numeric = users.aggregate(
  $match(() => ({
    age: { $not: { $gt: 25 } },
  })),
);

const valid_not_string = users.aggregate(
  $match(() => ({
    name: { $not: { $regex: /^A/ } },
  })),
);

const valid_not_boolean = users.aggregate(
  $match(() => ({
    active: { $not: { $eq: true } },
  })),
);

const valid_not_date = users.aggregate(
  $match(() => ({
    createdAt: { $not: { $gt: new Date() } },
  })),
);

// INVALID: $not as a top-level operator (MongoDB rejects this)
const invalid_top_level_not = users.aggregate(
  // @ts-expect-error - $not is NOT a top-level logical operator, use per-field instead
  $match(() => ({
    $not: { age: { $gt: 25 } },
  })),
);

// ============================================
// 2. STRING COMPARISON OPERATORS
// ============================================

// VALID: String comparison operators (now allowed)
const valid_string_gt = users.aggregate(
  $match(() => ({
    name: { $gt: "M" },
  })),
);

const valid_string_gte = users.aggregate(
  $match(() => ({
    name: { $gte: "A", $lte: "Z" },
  })),
);

const valid_string_lt = users.aggregate(
  $match(() => ({
    email: { $lt: "m@example.com" },
  })),
);

const valid_string_lte = users.aggregate(
  $match(() => ({
    email: { $lte: "z@example.com" },
  })),
);

// VALID: $not with string comparison operators
const valid_not_string_gt = users.aggregate(
  $match(() => ({
    name: { $not: { $gt: "M" } },
  })),
);

// ============================================
// 3. $options REQUIRES $regex
// ============================================

// VALID: $options with $regex
const valid_regex_options = users.aggregate(
  $match(() => ({
    name: { $regex: "^test", $options: "i" },
  })),
);

// VALID: $regex without $options
const valid_regex_no_options = users.aggregate(
  $match(() => ({
    name: { $regex: /test/ },
  })),
);

// INVALID: $options without $regex
const invalid_options_without_regex = users.aggregate(
  $match(() => ({
    // @ts-expect-error - $options requires $regex to be present
    name: { $options: "i" },
  })),
);

// ============================================
// 4. ACCUMULATOR BARE STRING REJECTION
// ============================================

// VALID: Accumulator with field ref
const valid_avg = users.aggregate(
  $group($ => ({
    _id: null,
    avgAge: $.avg("$age"),
  })),
);

const valid_sum = users.aggregate(
  $group($ => ({
    _id: null,
    totalScore: $.sum("$metadata.score"),
  })),
);

const valid_max_min = users.aggregate(
  $group($ => ({
    _id: null,
    maxAge: $.max("$age"),
    minAge: $.min("$age"),
  })),
);

// VALID: Accumulator with expression result
const valid_avg_expr = users.aggregate(
  $group($ => ({
    _id: null,
    avgAge: $.avg($.add("$age", 1)),
  })),
);

// VALID: Accumulator with numeric literal
const valid_sum_literal = users.aggregate(
  $group($ => ({
    _id: null,
    count: $.sum(1),
  })),
);

// INVALID: Accumulator with bare string (missing $ prefix)
const invalid_avg_bare = users.aggregate(
  $group($ => ({
    _id: null,
    // @ts-expect-error - Bare string "age" is not valid in accumulator position — did you mean "$age"?
    avgAge: $.avg("age"),
  })),
);

const invalid_sum_bare = users.aggregate(
  $group($ => ({
    _id: null,
    // @ts-expect-error - Bare string "score" is not valid in accumulator position
    totalScore: $.sum("score"),
  })),
);

const invalid_max_bare = users.aggregate(
  $group($ => ({
    _id: null,
    // @ts-expect-error - Bare string "age" is not valid in accumulator position
    maxAge: $.max("age"),
  })),
);

const invalid_min_bare = users.aggregate(
  $group($ => ({
    _id: null,
    // @ts-expect-error - Bare string "age" is not valid in accumulator position
    minAge: $.min("age"),
  })),
);

const invalid_first_bare = users.aggregate(
  $group($ => ({
    _id: null,
    // @ts-expect-error - Bare string "name" is not valid in accumulator position
    firstName: $.first("name"),
  })),
);

const invalid_last_bare = users.aggregate(
  $group($ => ({
    _id: null,
    // @ts-expect-error - Bare string "name" is not valid in accumulator position
    lastName: $.last("name"),
  })),
);

// ============================================
// 5. UPDATE PIPELINE STAGE BRANDING
// ============================================

const u = update<User>();

// VALID: Update-allowed stages in update pipeline
const valid_update_set = u.pipe(u.set({ age: 30 }));
const valid_update_unset = u.pipe(u.unset("active"));
const valid_update_addFields = u.pipe(u.addFields($ => ({ computed: $.add("$age", 1) })));

// INVALID: Non-update stages in update pipeline
// @ts-expect-error - $group is not allowed in update pipelines
const invalid_update_group = u.pipe($group($ => ({ _id: "$name", count: $.sum(1) })));

// @ts-expect-error - $sort is not allowed in update pipelines
const invalid_update_sort = u.pipe($sort({ age: 1 }));

// @ts-expect-error - $unwind is not allowed in update pipelines
const invalid_update_unwind = u.pipe($unwind("$tags"));

// @ts-expect-error - $match is not allowed in update pipelines
const invalid_update_match = u.pipe($match(() => ({ active: true })));

// ============================================
// 6. $replaceRoot FIELD REFERENCE VALIDATION
// ============================================

// VALID: $replaceRoot with valid field ref
const valid_replace_root = users.aggregate($replaceRoot({ newRoot: "$profile" }));

// VALID: $replaceRoot with $$ROOT
const valid_replace_root_system = users.aggregate($replaceRoot("$$ROOT"));

// VALID: $replaceRoot with object expression
const valid_replace_root_object = users.aggregate(
  $replaceRoot({ newRoot: { name: "$name", email: "$email" } }),
);

// VALID: $replaceRoot with builder callback
const valid_replace_root_builder = users.aggregate(
  $replaceRoot($ => ({ newRoot: { info: $.concat("$name", " - ", "$email") } })),
);

// INVALID: $replaceRoot with invalid field ref
const invalid_replace_root = users.aggregate(
  // @ts-expect-error - $nonexistent is not a valid field path
  $replaceRoot("$nonexistent"),
);

const invalid_replace_root_newRoot = users.aggregate(
  // @ts-expect-error - $doesNotExist is not a valid field path
  $replaceRoot({ newRoot: "$doesNotExist" }),
);
