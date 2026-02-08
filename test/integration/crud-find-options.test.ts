/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { Schema as S } from "@effect/schema";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { registry } from "../../src/sluice.js";
import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

const ItemSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  score: S.Number,
  category: S.String,
  active: S.Boolean,
});

const dbReg = registry("8.0", { items: ItemSchema });

describe("CRUD Find Options Runtime Tests", () => {
  let db: Db;
  const ids = Array.from({ length: 5 }, () => new ObjectId());

  beforeAll(async () => {
    const ctx = await setup();
    db = ctx.db;

    await dbReg(db)
      .items.insertMany([
        { _id: ids[0]!, name: "alpha", score: 50, category: "A", active: true },
        { _id: ids[1]!, name: "bravo", score: 30, category: "B", active: false },
        { _id: ids[2]!, name: "charlie", score: 80, category: "A", active: true },
        { _id: ids[3]!, name: "delta", score: 10, category: "B", active: true },
        { _id: ids[4]!, name: "echo", score: 60, category: "A", active: false },
      ])
      .execute();
  });

  afterAll(teardown);

  it("find with sort ascending", async () => {
    const coll = dbReg(db).items;
    const result = await coll.find($ => ({}), { sort: { score: 1 } }).toList();
    expect(result).toHaveLength(5);
    expect(result[0]!.name).toBe("delta"); // score 10
    expect(result[4]!.name).toBe("charlie"); // score 80
  });

  it("find with sort descending", async () => {
    const coll = dbReg(db).items;
    const result = await coll.find($ => ({}), { sort: { score: -1 } }).toList();
    expect(result[0]!.name).toBe("charlie"); // score 80
    expect(result[4]!.name).toBe("delta"); // score 10
  });

  it("find with limit", async () => {
    const coll = dbReg(db).items;
    const result = await coll.find($ => ({}), { sort: { name: 1 }, limit: 3 }).toList();
    expect(result).toHaveLength(3);
    expect(result.map((d: { name: string }) => d.name)).toEqual(["alpha", "bravo", "charlie"]);
  });

  it("find with skip", async () => {
    const coll = dbReg(db).items;
    const result = await coll.find($ => ({}), { sort: { name: 1 }, skip: 3 }).toList();
    expect(result).toHaveLength(2);
    expect(result.map((d: { name: string }) => d.name)).toEqual(["delta", "echo"]);
  });

  it("find with skip + limit (pagination)", async () => {
    const coll = dbReg(db).items;
    const result = await coll.find($ => ({}), { sort: { name: 1 }, skip: 1, limit: 2 }).toList();
    expect(result).toHaveLength(2);
    expect(result.map((d: { name: string }) => d.name)).toEqual(["bravo", "charlie"]);
  });

  it("find with filter and sort", async () => {
    const coll = dbReg(db).items;
    const result = await coll.find($ => ({ active: true }), { sort: { score: -1 } }).toList();
    expect(result).toHaveLength(3);
    expect(result[0]!.name).toBe("charlie"); // score 80
    expect(result[1]!.name).toBe("alpha"); // score 50
    expect(result[2]!.name).toBe("delta"); // score 10
  });

  it("find with filter, sort, and limit", async () => {
    const coll = dbReg(db).items;
    const result = await coll
      .find($ => ({ category: "A" }), { sort: { score: 1 }, limit: 2 })
      .toList();
    expect(result).toHaveLength(2);
    expect(result[0]!.name).toBe("alpha"); // score 50
    expect(result[1]!.name).toBe("echo"); // score 60
  });

  it("findOne should return single doc", async () => {
    const coll = dbReg(db).items;
    const result = await coll.findOne($ => ({ name: "bravo" })).toOne();
    expect(result).not.toBeNull();
    expect(result?.score).toBe(30);
  });

  it("findOne with non-existent filter returns null", async () => {
    const coll = dbReg(db).items;
    const result = await coll.findOne($ => ({ name: "nonexistent" })).toOne();
    expect(result).toBeNull();
  });

  it("find with no filter returns all", async () => {
    const coll = dbReg(db).items;
    const result = await coll.find().toList();
    expect(result).toHaveLength(5);
  });
});
