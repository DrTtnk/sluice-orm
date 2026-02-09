/**
 * Match & Sort type safety regression tests
 *
 * Verifies:
 * - Match comparison operators reject cross-type values
 * - Match comparison operators reject field refs (only valid in $expr)
 * - Sort rejects non-comparable paths (objects, arrays of objects)
 * - Sort allows comparable paths (numbers, strings, dates, booleans, ObjectId, _id)
 */
import { Schema as S } from "@effect/schema";
import { ObjectId } from "bson";

import { $group, $match, $sort, collection } from "../../../src/sluice.js";

const ObjectIdSchema = S.instanceOf(ObjectId);

const TestSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  active: S.Boolean,
  createdAt: S.Date,
  tags: S.Array(S.String),
  profile: S.Struct({ bio: S.String, avatar: S.String }),
  scores: S.Array(S.Number),
});

declare const db: import("mongodb").Db;
const col = collection("test", TestSchema, db.collection("test"));

// =============================================
// Match: cross-type comparison rejection
// =============================================

col.aggregate(
  // @ts-expect-error $gt on numeric field rejects string
  $match(() => ({ age: { $gt: "hello" } })),
);

col.aggregate(
  // @ts-expect-error $lt on numeric field rejects boolean
  $match(() => ({ age: { $lt: true } })),
);

col.aggregate(
  // @ts-expect-error $gte on string field rejects number
  $match(() => ({ name: { $gte: 42 } })),
);

col.aggregate(
  // @ts-expect-error $lte on string field rejects Date
  $match(() => ({ name: { $lte: new Date() } })),
);

col.aggregate(
  // @ts-expect-error $in on numeric field rejects string array
  $match(() => ({ age: { $in: ["a", "b"] } })),
);

// =============================================
// Match: valid usage (should compile)
// =============================================

col.aggregate($match(() => ({ age: { $gt: 18 } })));

col.aggregate($match(() => ({ name: { $eq: "Alice" } })));

col.aggregate($match(() => ({ createdAt: { $gte: new Date() } })));

col.aggregate($match(() => ({ active: { $eq: true } })));

col.aggregate($match(() => ({ age: { $in: [1, 2, 3] } })));

col.aggregate($match(() => ({ name: { $regex: /test/i } })));

// =============================================
// Sort: non-comparable paths rejected
// =============================================

col.aggregate(
  // @ts-expect-error sorting on object path "profile" is not comparable
  $sort({ profile: 1 }),
);

// =============================================
// Sort: valid comparable paths
// =============================================

col.aggregate($sort({ name: 1 }));
col.aggregate($sort({ age: -1 }));
col.aggregate($sort({ active: 1 }));
col.aggregate($sort({ createdAt: -1 }));
col.aggregate($sort({ _id: 1 }));
col.aggregate($sort({ "profile.bio": 1 }));

// Sort on _id after $group with compound _id (should always work)
col.aggregate(
  $group($ => ({
    _id: { name: "$name", age: "$age" },
    count: $.sum(1),
  })),
  $sort({ _id: -1 }),
);
