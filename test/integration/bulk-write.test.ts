import { Schema as S } from "@effect/schema";
import { Db, ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { registry } from "../../src/sluice.js";
import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

const WidgetSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  count: S.Number,
  active: S.Boolean,
});

describe("BulkWrite Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
  }, 120000);

  afterAll(teardown);

  it("should execute mixed insert and delete operations", async () => {
    const reg = registry("8.0", { widgets: WidgetSchema });
    const coll = reg(db).widgets;

    const id1 = new ObjectId();
    const id2 = new ObjectId();

    const result = await coll
      .bulkWrite([
        { insertOne: { document: { _id: id1, name: "A", count: 1, active: true } } },
        { insertOne: { document: { _id: id2, name: "B", count: 2, active: false } } },
      ])
      .execute();

    expect(result.insertedCount).toBe(2);

    const count = await coll.countDocuments().execute();
    expect(count).toBe(2);

    // Now delete one
    const deleteResult = await coll.bulkWrite([{ deleteOne: { filter: { _id: id1 } } }]).execute();

    expect(deleteResult.deletedCount).toBe(1);
    const remaining = await coll.countDocuments().execute();
    expect(remaining).toBe(1);
  });

  it("should execute updateOne and updateMany", async () => {
    const reg = registry("8.0", { widgets_upd: WidgetSchema });
    const coll = reg(db).widgets_upd;

    await coll
      .bulkWrite([
        { insertOne: { document: { _id: new ObjectId(), name: "X", count: 10, active: true } } },
        { insertOne: { document: { _id: new ObjectId(), name: "Y", count: 20, active: true } } },
        { insertOne: { document: { _id: new ObjectId(), name: "Z", count: 30, active: false } } },
      ])
      .execute();

    const result = await coll
      .bulkWrite([
        { updateOne: { filter: { name: "X" }, update: { $set: { count: 100 } } } },
        { updateMany: { filter: { active: true }, update: { $inc: { count: 5 } } } },
      ])
      .execute();

    expect(result.modifiedCount).toBeGreaterThanOrEqual(2);

    const x = await coll.findOne($ => ({ name: "X" })).toOne();
    // X was first set to 100, then incremented by 5 (ordered)
    expect(x?.count).toBe(105);

    const y = await coll.findOne($ => ({ name: "Y" })).toOne();
    expect(y?.count).toBe(25); // 20 + 5
  });

  it("should execute replaceOne", async () => {
    const reg = registry("8.0", { widgets_rep: WidgetSchema });
    const coll = reg(db).widgets_rep;

    const id = new ObjectId();
    await coll
      .bulkWrite([{ insertOne: { document: { _id: id, name: "old", count: 0, active: false } } }])
      .execute();

    const result = await coll
      .bulkWrite([
        {
          replaceOne: {
            filter: { _id: id },
            replacement: { _id: id, name: "new", count: 99, active: true },
          },
        },
      ])
      .execute();

    expect(result.modifiedCount).toBe(1);

    const doc = await coll.findOne($ => ({ _id: id })).toOne();
    expect(doc?.name).toBe("new");
    expect(doc?.count).toBe(99);
  });

  it("should execute ordered operations sequentially", async () => {
    const reg = registry("8.0", { widgets_ord: WidgetSchema });
    const coll = reg(db).widgets_ord;

    const id = new ObjectId();

    // Insert then update in same bulk — ordered guarantees insert happens first
    const result = await coll
      .bulkWrite([
        { insertOne: { document: { _id: id, name: "seq", count: 1, active: true } } },
        { updateOne: { filter: { _id: id }, update: { $set: { count: 42 } } } },
      ])
      .execute({ ordered: true });

    expect(result.insertedCount).toBe(1);
    expect(result.modifiedCount).toBe(1);

    const doc = await coll.findOne($ => ({ _id: id })).toOne();
    expect(doc?.count).toBe(42);
  });

  it("should handle upsert in updateOne", async () => {
    const reg = registry("8.0", { widgets_ups: WidgetSchema });
    const coll = reg(db).widgets_ups;

    const result = await coll
      .bulkWrite([
        {
          updateOne: {
            filter: { name: "upserted" },
            update: { $set: { count: 77, active: true } },
            upsert: true,
          },
        },
      ])
      .execute();

    expect(result.upsertedCount).toBe(1);

    const doc = await coll.findOne($ => ({ name: "upserted" })).toOne();
    expect(doc?.count).toBe(77);
  });

  it("should handle deleteMany", async () => {
    const reg = registry("8.0", { widgets_dm: WidgetSchema });
    const coll = reg(db).widgets_dm;

    await coll
      .bulkWrite([
        { insertOne: { document: { _id: new ObjectId(), name: "a", count: 1, active: true } } },
        { insertOne: { document: { _id: new ObjectId(), name: "b", count: 2, active: true } } },
        { insertOne: { document: { _id: new ObjectId(), name: "c", count: 3, active: false } } },
      ])
      .execute();

    const result = await coll.bulkWrite([{ deleteMany: { filter: { active: true } } }]).execute();

    expect(result.deletedCount).toBe(2);
    const remaining = await coll.countDocuments().execute();
    expect(remaining).toBe(1);
  });
});
