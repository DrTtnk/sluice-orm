import { Schema as S } from "@effect/schema";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { registry } from "../../src/sluice.js";
import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

const ItemSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  score: S.Number,
  tags: S.Array(S.String),
  active: S.Boolean,
});

type Item = typeof ItemSchema.Type;

const dbRegistry = registry("8.0", { items: ItemSchema });

describe("Extended CRUD Operations", () => {
  let db: Db;
  const id1 = new ObjectId();
  const id2 = new ObjectId();
  const id3 = new ObjectId();

  beforeAll(async () => {
    db = (await setup()).db;
    const coll = dbRegistry(db).items;
    await coll
      .insertMany([
        { _id: id1, name: "alpha", score: 10, tags: ["a", "b"], active: true },
        { _id: id2, name: "beta", score: 20, tags: ["b", "c"], active: false },
        { _id: id3, name: "gamma", score: 30, tags: ["a", "c"], active: true },
      ])
      .execute();
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  describe("countDocuments", () => {
    it("should count all documents", async () => {
      const coll = dbRegistry(db).items;
      const count = await coll.countDocuments().execute();
      expect(count).toBe(3);
      expectType<number>(count);
    });

    it("should count filtered documents", async () => {
      const coll = dbRegistry(db).items;
      const count = await coll.countDocuments($ => ({ active: true })).execute();
      expect(count).toBe(2);
    });
  });

  describe("estimatedDocumentCount", () => {
    it("should return estimated count", async () => {
      const coll = dbRegistry(db).items;
      const count = await coll.estimatedDocumentCount().execute();
      expect(count).toBeGreaterThanOrEqual(3);
      expectType<number>(count);
    });
  });

  describe("distinct", () => {
    it("should return distinct values for a field", async () => {
      const coll = dbRegistry(db).items;
      const names = await coll.distinct("name").execute();
      expect(names.sort()).toEqual(["alpha", "beta", "gamma"]);
      expectType<string[]>(names);
    });

    it("should return distinct values with filter", async () => {
      const coll = dbRegistry(db).items;
      const names = await coll.distinct("name", $ => ({ active: true })).execute();
      expect(names.sort()).toEqual(["alpha", "gamma"]);
    });

    it("should return distinct values for numeric field", async () => {
      const coll = dbRegistry(db).items;
      const scores = await coll.distinct("score").execute();
      expect(scores.sort()).toEqual([10, 20, 30]);
      expectType<number[]>(scores);
    });
  });

  describe("findOneAndDelete", () => {
    it("should find and delete document", async () => {
      const coll = dbRegistry(db).items;
      // Insert a temp doc to delete
      const tempId = new ObjectId();
      await coll
        .insertOne({ _id: tempId, name: "temp", score: 0, tags: [], active: false })
        .execute();

      const deleted = await coll.findOneAndDelete($ => ({ _id: tempId })).execute();
      expect(deleted).not.toBeNull();
      expect(deleted?.name).toBe("temp");
      expectType<Item | null>(deleted);

      // Verify it's gone
      const count = await coll.countDocuments($ => ({ _id: tempId })).execute();
      expect(count).toBe(0);
    });
  });

  describe("findOneAndReplace", () => {
    it("should find and replace document", async () => {
      const coll = dbRegistry(db).items;
      const tempId = new ObjectId();
      await coll
        .insertOne({ _id: tempId, name: "toReplace", score: 5, tags: ["old"], active: true })
        .execute();

      const replaced = await coll
        .findOneAndReplace($ => ({ _id: tempId }), {
          _id: tempId,
          name: "replaced",
          score: 99,
          tags: ["new"],
          active: false,
        })
        .execute();

      expect(replaced).not.toBeNull();
      expect(replaced?.name).toBe("toReplace"); // returns the original
      expectType<Item | null>(replaced);

      // Verify replacement
      const updated = await coll.findOne($ => ({ _id: tempId })).toOne();
      expect(updated?.name).toBe("replaced");
      expect(updated?.score).toBe(99);

      // Cleanup
      await coll.deleteOne($ => ({ _id: tempId })).execute();
    });
  });

  describe("findOneAndUpdate", () => {
    it("should find and update document", async () => {
      const coll = dbRegistry(db).items;
      const tempId = new ObjectId();
      await coll
        .insertOne({ _id: tempId, name: "toUpdate", score: 1, tags: [], active: true })
        .execute();

      const result = await coll
        .findOneAndUpdate($ => ({ _id: tempId }), { $set: { score: 42 } })
        .execute();

      expect(result).not.toBeNull();

      // Verify update
      const doc = await coll.findOne($ => ({ _id: tempId })).toOne();
      expect(doc?.score).toBe(42);

      // Cleanup
      await coll.deleteOne($ => ({ _id: tempId })).execute();
    });
  });
});
