import { Schema as S } from "@effect/schema";
import { $addFields, $group, $match, $project, $sort, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

const DocSchema = S.Struct({
  _id: ObjectIdSchema,
  label: S.String,
  value: S.NullOr(S.Number),
  tags: S.Array(S.String),
});

const dbReg = registry("8.0", { docs: DocSchema });

describe("Null/Missing Field Edge Cases", () => {
  let db: Db;

  beforeAll(async () => {
    const ctx = await setup();
    db = ctx.db;

    // Insert via raw driver to include documents with missing fields
    const rawCol = db.collection("docs");
    await rawCol.insertMany([
      { _id: new ObjectId(), label: "has_value", value: 10, tags: ["a", "b"] },
      { _id: new ObjectId(), label: "null_value", value: null, tags: ["c"] },
      { _id: new ObjectId(), label: "zero_value", value: 0, tags: [] },
    ]);
  });

  afterAll(teardown);

  it("$ifNull should provide default for null fields", async () => {
    const coll = dbReg(db).docs;
    const result = await coll
      .aggregate(
        $addFields($ => ({
          safeValue: $.ifNull("$value", -1),
        })),
        $project($ => ({ label: 1, safeValue: 1 })),
        $sort({ label: 1 }),
      )
      .toList();

    expect(result).toHaveLength(3);
    expect(result[0]?.safeValue).toBe(10); // has_value
    expect(result[1]?.safeValue).toBe(-1); // null_value -> default
    expect(result[2]?.safeValue).toBe(0); // zero_value -> 0 (not null)
  });

  it("$cond with null check should branch correctly", async () => {
    const coll = dbReg(db).docs;
    const result = await coll
      .aggregate(
        $addFields($ => ({
          status: $.cond({
            if: $.eq("$value", null),
            then: "missing" as const,
            else: "present" as const,
          }),
        })),
        $project($ => ({ label: 1, status: 1 })),
        $sort({ label: 1 }),
      )
      .toList();

    expect(result).toHaveLength(3);
    expect(result[0]?.status).toBe("present"); // has_value
    expect(result[1]?.status).toBe("missing"); // null_value
    expect(result[2]?.status).toBe("present"); // zero_value
  });

  it("$match on null field should find null documents", async () => {
    const coll = dbReg(db).docs;
    const result = await coll.aggregate($match(() => ({ value: null }))).toList();

    // MongoDB $match { value: null } matches null AND missing
    expect(result).toHaveLength(1);
    expect(result[0]?.label).toBe("null_value");
  });

  it("arithmetic on null should produce null", async () => {
    const coll = dbReg(db).docs;
    const result = await coll
      .aggregate(
        $match(() => ({ label: "null_value" })),
        $addFields($ => ({
          doubled: $.multiply("$value", 2),
        })),
        $project($ => ({ doubled: 1 })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.doubled).toBeNull();
  });

  it("empty collection aggregation should return empty array", async () => {
    const coll = dbReg(db).docs;
    const result = await coll
      .aggregate(
        $match(() => ({ label: "nonexistent_label_xyz" })),
        $project($ => ({ label: 1 })),
      )
      .toList();

    expect(result).toHaveLength(0);
  });

  it("$group on empty result set should return empty", async () => {
    const coll = dbReg(db).docs;
    const result = await coll
      .aggregate(
        $match(() => ({ label: "nonexistent_label_xyz" })),
        $group($ => ({
          _id: null,
          total: $.sum("$value"),
        })),
      )
      .toList();

    expect(result).toHaveLength(0);
  });

  it("empty tags array should have $size 0", async () => {
    const coll = dbReg(db).docs;
    const result = await coll
      .aggregate(
        $match(() => ({ label: "zero_value" })),
        $project($ => ({
          label: 1,
          tagCount: $.size("$tags"),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.tagCount).toBe(0);
  });

  it("$type should identify null values", async () => {
    const coll = dbReg(db).docs;
    const result = await coll
      .aggregate(
        $addFields($ => ({
          valueType: $.type("$value"),
        })),
        $project($ => ({ label: 1, valueType: 1 })),
        $sort({ label: 1 }),
      )
      .toList();

    expect(result).toHaveLength(3);
    expect(result[0]?.valueType).toBe("int"); // has_value (10) — JS number stored as int32 via raw driver
    expect(result[1]?.valueType).toBe("null"); // null_value
    expect(result[2]?.valueType).toBe("int"); // zero_value (0)
  });
});
