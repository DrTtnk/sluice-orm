/**
 * Type tests for migration tool and CRUD improvements
 */
import type { CollationOptions, Document, ObjectId } from "mongodb";
import { expectType } from "tsd";

import type { BulkWriteOp, CountOptions, FindOneAndOptions } from "../../../src/crud.js";
import { migrate } from "../../../src/migrate.js";
import { $addFields, $group, $match, $set, $sort, $unset } from "../../../src/sluice-stages.js";

// ============================================
// TEST SCHEMAS
// ============================================

type OldUser = {
  _id: ObjectId;
  name: string;
  age: number;
  legacyField: string;
};

type NewUser = {
  _id: ObjectId;
  name: string;
  age: number;
  email: string;
};

// ============================================
// MIGRATION TOOL — valid cases
// ============================================

const m = migrate<OldUser, NewUser>();

// VALID: Adding email and removing legacyField produces NewUser
const validMigration = m.pipe($set({ email: "unknown@example.com" }), $unset("legacyField"));

// VALID: Using $addFields then $unset
const validMigration2 = m.pipe(
  $addFields($ => ({ email: $.concat("$name", "@migrated.com") })),
  $unset("legacyField"),
);

// ============================================
// MIGRATION TOOL — invalid: non-update stages
// ============================================

// @ts-expect-error - $group is not allowed in migration pipelines
const invalidGroup = m.pipe($group($ => ({ _id: "$name", count: $.sum(1) })));

// @ts-expect-error - $sort is not allowed in migration pipelines
const invalidSort = m.pipe($sort({ name: 1 }));

// @ts-expect-error - $match is not allowed in migration pipelines
const invalidMatch = m.pipe($match(() => ({ name: "test" })));

// ============================================
// CRUD: FindOneAndOptions shape
// ============================================

// Verify returnDocument option exists with correct type
type _ReturnDoc = FindOneAndOptions<OldUser>["returnDocument"];
expectType<"before" | "after" | undefined>(undefined as unknown as _ReturnDoc);

// Verify all option fields exist in the type
type _AllOpts = Required<FindOneAndOptions<OldUser>>;
expectType<"before" | "after">({} as _AllOpts["returnDocument"]);
expectType<string | Document>({} as _AllOpts["hint"]);
expectType<CollationOptions>({} as _AllOpts["collation"]);
expectType<number>({} as _AllOpts["maxTimeMS"]);
expectType<string>({} as _AllOpts["comment"]);

// ============================================
// CRUD: CountOptions shape
// ============================================

type _CountOpts = Required<CountOptions>;
expectType<number>({} as _CountOpts["limit"]);
expectType<number>({} as _CountOpts["skip"]);
expectType<number>({} as _CountOpts["maxTimeMS"]);

// ============================================
// CRUD: BulkWriteOp structure
// ============================================

// Valid operations
const insertOp: BulkWriteOp<OldUser> = {
  insertOne: { document: { _id: {} as ObjectId, name: "test", age: 25, legacyField: "x" } },
};

const deleteOp: BulkWriteOp<OldUser> = {
  deleteOne: { filter: { name: "test" } },
};

const deleteManyOp: BulkWriteOp<OldUser> = {
  deleteMany: { filter: { age: { $gt: 50 } } },
};

const updateOneOp: BulkWriteOp<OldUser> = {
  updateOne: { filter: { name: "test" }, update: { $set: { age: 30 } } },
};

const updateManyOp: BulkWriteOp<OldUser> = {
  updateMany: { filter: { age: { $lt: 18 } }, update: { $inc: { age: 1 } }, upsert: true },
};

const replaceOneOp: BulkWriteOp<OldUser> = {
  replaceOne: {
    filter: { name: "old" },
    replacement: { _id: {} as ObjectId, name: "new", age: 25, legacyField: "y" },
  },
};

// Invalid: wrong field type
const badInsert: BulkWriteOp<OldUser> = {
  insertOne: {
    // @ts-expect-error - name should be string, not number
    document: { _id: {} as ObjectId, name: 123, age: 25, legacyField: "x" },
  },
};
