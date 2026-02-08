/**
 * Type Tests: Update Pipeline Validation
 *
 * Validates:
 * - Valid update pipeline operations compile
 * - Invalid field references are caught via builder constraints
 * - Update pipeline stages chain types correctly
 * - $unset removes fields from downstream type
 * - $replaceRoot changes the pipeline output type
 */

import { update } from "../../../src/crud/updates/stages/index.js";

type User = {
  _id: string;
  name: string;
  age: number;
  email: string;
  active: boolean;
  profile: {
    bio: string;
    avatar: string;
  };
  tags: string[];
};

const u = update<User>();

// ============================================
// Valid operations - should compile
// ============================================

// $set with existing field value
const valid1 = u.pipe(u.set($ => ({ age: 25 })));

// $set with computed expression
const valid2 = u.pipe(u.set($ => ({ doubled: $.multiply("$age", 2) })));

// $set with field reference
const valid3 = u.pipe(u.set($ => ({ nameCopy: "$name" })));

// $set adding a new field
const valid4 = u.pipe(u.set($ => ({ newField: "test" })));

// $unset fields
const valid5 = u.pipe(u.unset("tags"));

// Chained stages
const valid6 = u.pipe(
  u.set($ => ({ computed: $.add("$age", 10) })),
  u.unset("tags"),
);

// $replaceRoot with nested field
const valid7 = u.pipe(u.replaceRoot({ newRoot: "$profile" }));

// ============================================
// Invalid operations - should error
// ============================================

// @ts-expect-error - Invalid field reference in expression: $nonexistent not a field
const invalid1 = u.pipe(u.set($ => ({ bad: $.multiply("$nonexistent", 2) })));

// @ts-expect-error - $.multiply requires numeric args, $name is string
const invalid2 = u.pipe(u.set($ => ({ bad: $.multiply("$name", 2) })));

// @ts-expect-error - $.concat requires string args, $age is number
const invalid3 = u.pipe(u.set($ => ({ bad: $.concat(["$age", " years"]) })));

// @ts-expect-error - $replaceRoot now validates field refs: $nonexistent is not a valid field
const invalid_replaceRoot = u.pipe(u.replaceRoot({ newRoot: "$nonexistent" }));
