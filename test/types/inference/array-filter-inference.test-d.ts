/**
 * Test file to demonstrate array filter type inference
 */

import { Schema as S } from "@effect/schema";
import { ObjectId } from "bson";

import type { CrudCollection } from "../../../src/crud.js";

const ObjectIdSchema = S.instanceOf(ObjectId);

const TestSchema = S.Struct({
  _id: ObjectIdSchema,
  tags: S.Array(S.String),
  orders: S.Array(
    S.Struct({
      orderId: ObjectIdSchema,
      total: S.Number,
      items: S.Array(S.String),
    }),
  ),
});

type TestDoc = typeof TestSchema.Type;

declare const coll: CrudCollection<TestDoc>;

// ✓ Type inference works - arrayFilters should infer { i: string }
coll.updateOne(
  $ => ({ _id: new ObjectId() }),
  { $set: { "tags.$[i]": "new-tag" } },
  {
    arrayFilters: [
      // ✓ Correctly inferred: i should be string
      { i: { $type: "string" } },
    ],
  },
);

// ✓ Multiple IDs - arrayFilters should infer { i: string, j: Order }
coll.updateOne(
  $ => ({ _id: new ObjectId() }),
  {
    $set: {
      "tags.$[i]": "new-tag",
      "orders.$[j].total": 500,
    },
  },
  {
    arrayFilters: [
      // ✓ i: string
      { i: { $type: "string" } },
      // ✓ j: Order (can query on nested fields)
      { "j.total": { $lt: 100 } },
      { "j.orderId": { $exists: true } },
    ],
  },
);

// ✓ No array filters needed - should allow omitting the option
coll.updateOne(
  $ => ({ _id: new ObjectId() }),
  { $set: { "orders.0.total": 100 } },
  // No arrayFilters - this is fine!
);

// ✓ Empty update should work
coll.updateOne($ => ({ _id: new ObjectId() }), { $inc: { "orders.0.total": 10 } });

// Test: Wrong ID should error
coll.updateOne(
  $ => ({ _id: new ObjectId() }),
  { $set: { "tags.$[i]": "new-tag" } },
  {
    // @ts-expect-error - 'wrongId' is not used in the update spec
    arrayFilters: [{ wrongId: { $type: "string" } }],
  },
);

// Test: Nested path typing works correctly
coll.updateOne(
  $ => ({ _id: new ObjectId() }),
  { $set: { "orders.$[orderFilter].items": ["item1", "item2"] } },
  {
    arrayFilters: [
      // Can query on root element or nested paths
      { orderFilter: { $exists: true } },
      { "orderFilter.total": { $gte: 100 } },
      { "orderFilter.orderId": { $exists: true } },
    ],
  },
);

export {};
