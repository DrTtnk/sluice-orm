import { Schema as S } from "@effect/schema";
import { $addFields, $match, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const DocumentSchema = S.Struct({
  _id: ObjectIdSchema,
  flags: S.Number,
  permissions: S.Number,
  mask: S.Number,
  value1: S.Number,
  value2: S.Number,
  bits: S.Array(S.Number),
});

const dbRegistry = registry("8.0", { docs: DocumentSchema });

describe("Bitwise Operators Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .docs.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          flags: 0b1010,
          permissions: 0b0110,
          mask: 0xff,
          value1: 0b1100,
          value2: 0b1001,
          bits: [1, 2, 4, 8],
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  // $bitAnd tests
  it("should AND two fields", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      maskedFlags: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ maskedFlags: $.bitAnd("$flags", "$mask") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should AND field with literal", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      readPermission: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ readPermission: $.bitAnd("$permissions", 0x04) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should AND multiple values", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      commonBits: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({ commonBits: $.bitAnd("$flags", "$permissions", "$mask") })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should AND two literals", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      literal: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ literal: $.bitAnd(0xff, 0x0f) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  // $bitOr tests
  it("should OR two fields", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      combined: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ combined: $.bitOr("$flags", "$permissions") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should OR field with literal", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      withExecute: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ withExecute: $.bitOr("$permissions", 0x01) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should OR multiple values", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      anySet: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ anySet: $.bitOr("$value1", "$value2", "$mask") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  // $bitXor tests
  it("should XOR two fields", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      difference: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ difference: $.bitXor("$flags", "$mask") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should XOR field with literal", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      toggled: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ toggled: $.bitXor("$permissions", 0x02) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should XOR multiple values", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      xored: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ xored: $.bitXor("$value1", "$value2") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  // $bitNot tests
  it("should NOT a field", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      inverted: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ inverted: $.bitNot("$flags") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should NOT a literal", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      notMask: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ notMask: $.bitNot(0xff) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  // Complex permission system tests
  it("should check if user has read permission", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      hasRead: S.Boolean,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ hasRead: $.gt($.bitAnd("$permissions", 0x04), 0) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should check if user has all basic permissions", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      hasAllBasic: S.Boolean,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({ hasAllBasic: $.eq($.bitAnd("$permissions", 0x07), 0x07) })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should grant write permission", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      withWrite: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ withWrite: $.bitOr("$permissions", 0x02) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should revoke admin permission", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      withoutAdmin: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ withoutAdmin: $.bitAnd("$permissions", $.bitNot(0x08)) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should toggle execute permission", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      toggledExec: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ toggledExec: $.bitXor("$permissions", 0x01) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should find permission differences", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      differentBits: S.Number,
      onlyInFlags: S.Number,
      onlyInPerms: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          differentBits: $.bitXor("$flags", "$permissions"),
          onlyInFlags: $.bitAnd("$flags", $.bitNot("$permissions")),
          onlyInPerms: $.bitAnd("$permissions", $.bitNot("$flags")),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should filter documents by permission", async () => {
    const results = await dbRegistry(db)
      .docs.aggregate(
        $match($ => ({
          $expr: $.and(
            $.gt($.bitAnd("$permissions", 0x04), 0),
            $.eq($.bitAnd("$permissions", 0x08), 0),
          ),
        })),
      )
      .toList();

    assertSync(S.Array(DocumentSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should combine flags from multiple sources", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      finalFlags: S.Number,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({ finalFlags: $.bitAnd($.bitOr("$flags", "$permissions"), "$mask") })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should decompose into individual bit flags", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      hasExecute: S.Boolean,
      hasWrite: S.Boolean,
      hasRead: S.Boolean,
      hasAdmin: S.Boolean,
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          hasExecute: $.gt($.bitAnd("$permissions", 0x01), 0),
          hasWrite: $.gt($.bitAnd("$permissions", 0x02), 0),
          hasRead: $.gt($.bitAnd("$permissions", 0x04), 0),
          hasAdmin: $.gt($.bitAnd("$permissions", 0x08), 0),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });
});
