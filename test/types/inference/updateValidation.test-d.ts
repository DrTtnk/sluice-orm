/**
 * Type-level tests for strict update validation
 * Ensures that update operations are type-safe:
 * - $set only accepts values matching path type
 * - $inc/$mul only accept numeric paths
 * - $push/$addToSet/$pull only accept array paths with correct element types
 *
 * NOTE: Positional operators are supported and type-checked for update paths.
 */

import { Schema as S } from "@effect/schema";
import { type Db, ObjectId } from "mongodb";
import { expectAssignable } from "tsd";

import type {
  AddToSetSpec,
  BitSpec,
  IncSpec,
  MinMaxSpec,
  MulSpec,
  PopSpec,
  PullSpec,
  PushSpec,
} from "../../../src/crud/updates/operators.js";
import type { StrictUpdateSpec } from "../../../src/crud/updates/types.js";
import { registry } from "../../../src/registry.js";

const ObjectIdSchema = S.instanceOf(ObjectId);

// ==========================================
// Test Schema Definition
// ==========================================

type TestDoc = typeof TestSchema.Type;

const TestSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  count: S.Number,
  active: S.Boolean,
  score: S.Number,
  metadata: S.Struct({
    created: S.Date,
    priority: S.Number,
    label: S.String,
  }),
  tags: S.Array(S.String),
  items: S.Array(
    S.Struct({
      id: S.String,
      qty: S.Number,
      active: S.Boolean,
    }),
  ),
  matrix: S.Array(S.Array(S.Number)),
});

// ==========================================
// Test Setup
// ==========================================

const mockDb = {} as Db;
const db = registry("8.0", { test: TestSchema })(mockDb);

// ==========================================
// $set: Type-Safe Value Assignment
// ==========================================

// ✅ Valid: Assign correct types to paths
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $set: {
    name: "Updated",
    count: 42,
    active: true,
    score: 99.5,
    "metadata.priority": 1,
    "metadata.label": "high",
  },
});

// ✅ Valid: Nested object assignment
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $set: {
    metadata: {
      created: new Date(),
      priority: 5,
      label: "test",
    },
  },
});

// ✅ Valid: Array element via numeric index (full type safety)
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $set: {
    "items.0.qty": 10,
    "items.0.active": false,
  },
});

// ✅ Valid: Array root reset with literal empty array
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), { $set: { tags: [] } });

// ✅ Valid: Positional operators with arrayFilters
db.test.updateOne(
  $ => ({ _id: new ObjectId("000000000000000000000001") }),
  { $set: { "items.$[it].qty": 4 } },
  {
    arrayFilters: [
      {
        "it.id": "x",
        "it.active": true,
      },
    ],
  },
);

// Positional operators work with arrayFilters and are validated in update paths.

// ❌ Invalid: Wrong type for string field
// @ts-expect-error - name expects string, got number
db.test.updateOne({ _id: new ObjectId("000000000000000000000001") }, { $set: { name: 123 } });

// ❌ Invalid: Wrong type for number field
// @ts-expect-error - count expects number, got string
db.test.updateOne({ _id: new ObjectId("000000000000000000000001") }, { $set: { count: "five" } });

// ❌ Invalid: Wrong type for boolean field
// @ts-expect-error - active expects boolean, got string
db.test.updateOne({ _id: new ObjectId("000000000000000000000001") }, { $set: { active: "yes" } });

// ❌ Invalid: Wrong type for nested field
db.test.updateOne(
  $ => ({ _id: new ObjectId("000000000000000000000001") }),
  // @ts-expect-error - metadata.priority expects number, got string
  { $set: { "metadata.priority": "high" } },
);

// ❌ Invalid: Wrong type for array element field
db.test.updateOne(
  $ => ({ _id: new ObjectId("000000000000000000000001") }),
  // @ts-expect-error - items.0.qty expects number, got string
  { $set: { "items.0.qty": "ten" } },
);

// ==========================================
// $inc: Numeric Paths Only
// ==========================================

// ✅ Valid: Increment numeric fields
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $inc: {
    count: 1,
    score: 0.5,
  },
});

// ✅ Valid: Increment nested numeric field
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $inc: { "metadata.priority": -1 },
});

// Type test: $inc spec only allows numeric paths
expectAssignable<IncSpec<TestDoc>>({ count: 1 });
expectAssignable<IncSpec<TestDoc>>({ score: -5 });
expectAssignable<IncSpec<TestDoc>>({ "metadata.priority": 10 });

// Non-numeric paths should not be assignable to $inc
type IncPaths = keyof IncSpec<TestDoc>;
expectAssignable<IncPaths>("count" as const);
expectAssignable<IncPaths>("score" as const);
expectAssignable<IncPaths>("metadata.priority" as const);

// ==========================================
// $mul: Numeric Paths Only (like $inc)
// ==========================================

// ✅ Valid: Multiply numeric fields
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $mul: {
    count: 2,
    score: 1.5,
  },
});

// Type test: $mul only allows numeric paths
expectAssignable<MulSpec<TestDoc>>({ count: 2 });
expectAssignable<MulSpec<TestDoc>>({ score: 0.5 });

// ==========================================
// $push: Array Paths with Correct Element Type
// ==========================================

// ✅ Valid: Push string to string array
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $push: { tags: "newTag" },
});

// ✅ Valid: Push with $each modifier
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $push: { tags: { $each: ["a", "b", "c"] } },
});

// ✅ Valid: Push with all modifiers
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $push: {
    tags: {
      $each: ["new"],
      $position: 0,
      $slice: 10,
      $sort: 1,
    },
  },
});

// ✅ Valid: Push object to object array
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $push: {
    items: {
      id: "new",
      qty: 5,
      active: true,
    },
  },
});

// ✅ Valid: Push with $each for objects
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $push: {
    items: {
      $each: [
        {
          id: "a",
          qty: 1,
          active: true,
        },
        {
          id: "b",
          qty: 2,
          active: false,
        },
      ],
    },
  },
});

// Type test: $push only allows array paths
type PushPaths = keyof PushSpec<TestDoc>;

// Array paths should be allowed
expectAssignable<PushPaths>("tags" as const);
expectAssignable<PushPaths>("items" as const);
expectAssignable<PushPaths>("matrix" as const);

// ❌ Invalid: Push wrong element type to string array
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $push: {
    // @ts-expect-error - tags is string[], can't push number
    tags: 123,
  },
});

// ❌ Invalid: Push wrong element type in $each
db.test.updateOne(
  $ => ({ _id: new ObjectId("000000000000000000000001") }),
  // @ts-expect-error - tags is string[], $each must be string[]
  { $push: { tags: { $each: [1, 2, 3] } } },
);

// ❌ Invalid: Push object with wrong shape to items
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $push: {
    items: {
      id: "x",
      // @ts-expect-error - items element requires { id, qty, active }
      wrongField: 5,
    },
  },
});

// ==========================================
// $addToSet: Array Paths with Correct Element Type
// ==========================================

// ✅ Valid: Add string to string array (deduped)
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $addToSet: { tags: "unique" },
});

// ✅ Valid: Add with $each
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $addToSet: { tags: { $each: ["a", "b"] } },
});

// ✅ Valid: Add object to object array
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $addToSet: {
    items: {
      id: "unique",
      qty: 1,
      active: true,
    },
  },
});

// Type test: $addToSet only allows array paths
expectAssignable<AddToSetSpec<TestDoc>>({ tags: "x" });
expectAssignable<AddToSetSpec<TestDoc>>({
  items: {
    id: "x",
    qty: 1,
    active: true,
  },
});

// ❌ Invalid: Add wrong type
db.test.updateOne(
  $ => ({ _id: new ObjectId("000000000000000000000001") }),
  // @ts-expect-error - tags is string[], can't add number
  { $addToSet: { tags: 42 } },
);

// ==========================================
// $pull: Array Paths with Filter Support
// ==========================================

// ✅ Valid: Pull exact element from string array
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $pull: { tags: "removeMe" },
});

// ✅ Valid: Pull with filter on object array
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $pull: { items: { qty: { $lt: 5 } } },
});

// ✅ Valid: Pull with complex filter
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $pull: { items: { $and: [{ qty: 10 }, { active: false }] } },
});

// Type test: $pull only allows array paths
expectAssignable<PullSpec<TestDoc>>({ tags: "x" });
expectAssignable<PullSpec<TestDoc>>({ items: { id: "x" } });

// ==========================================
// $pop: Array Paths with 1 or -1
// ==========================================

// ✅ Valid: Pop first element
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), { $pop: { tags: -1 } });

// ✅ Valid: Pop last element
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), { $pop: { items: 1 } });

// Type test: $pop only allows 1 or -1
expectAssignable<PopSpec<TestDoc>>({ tags: 1 });
expectAssignable<PopSpec<TestDoc>>({ tags: -1 });
expectAssignable<PopSpec<TestDoc>>({ items: 1 });

// ==========================================
// $min/$max: Type-Safe Comparisons
// ==========================================

// ✅ Valid: Min/Max on numeric fields
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $min: { count: 0 },
  $max: { score: 100 },
});

// ✅ Valid: Min/Max on Date field
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $min: { "metadata.created": new Date() },
});

// ✅ Valid: Min/Max on string field (lexicographic)
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $max: { name: "zzz" },
});

// Type test: Values must match path type
expectAssignable<MinMaxSpec<TestDoc>>({ count: 5 });
expectAssignable<MinMaxSpec<TestDoc>>({ name: "test" });
expectAssignable<MinMaxSpec<TestDoc>>({ "metadata.created": new Date() });

// ==========================================
// $bit: Integer Paths with Bitwise Ops
// ==========================================

// ✅ Valid: Bitwise operations on numeric fields
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $bit: { count: { and: 0b1111 } },
});
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $bit: { count: { or: 0b0001 } },
});
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $bit: { count: { xor: 0b1010 } },
});

// Type test: $bit allows numeric paths
expectAssignable<BitSpec<TestDoc>>({ count: { and: 1 } });
expectAssignable<BitSpec<TestDoc>>({ score: { or: 2 } });

// ==========================================
// Combined Update Operations
// ==========================================

// ✅ Valid: Multiple operators in one update
db.test.updateOne($ => ({ _id: new ObjectId("000000000000000000000001") }), {
  $set: {
    name: "Updated",
    "metadata.label": "modified",
  },
  $inc: {
    count: 1,
    score: 10,
  },
  $push: { tags: "processed" },
  $currentDate: { "metadata.created": true },
});

// ==========================================
// Full StrictUpdateSpec Type Tests
// ==========================================

type FullSpec = StrictUpdateSpec<TestDoc>;

// Valid complete spec
const validSpec: FullSpec = {
  $set: { name: "test" },
  $inc: { count: 1 },
  $push: { tags: "new" },
  $pull: { tags: "old" },
  $pop: { items: 1 },
  $min: { score: 0 },
  $max: { score: 100 },
  $bit: { count: { and: 0xff } },
  $unset: { "metadata.label": "" },
  $rename: { name: "displayName" },
};

expectAssignable<FullSpec>(validSpec);
