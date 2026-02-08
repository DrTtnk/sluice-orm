/**
 * CRUD Update Tests - Traditional Update Syntax Only
 *
 * NOTE: These tests currently use traditional MongoDB update syntax ({ $set: {...} })
 * which supports arrayFilters but NOT aggregation expressions.
 *
 * TODO: Add pipeline update tests later ([{ $set: {...} }]) which support expressions
 * but NOT arrayFilters. The two modes are mutually exclusive in MongoDB.
 */
import { Schema as S } from "@effect/schema";
import { registry } from "@sluice/sluice";
import { type Db } from "mongodb";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

type ComplexDoc = {
  _id: string;
  name: string;
  nested: {
    items: {
      id: string;
      value: number;
      tags: string[];
    }[];
    metadata: {
      created: Date;
      score: number;
    };
  };
  arrayField: {
    subId: string;
    data: number[];
  }[];
  status: "active" | "inactive";
};

const ComplexSchema = S.Struct({
  _id: S.String,
  name: S.String,
  nested: S.Struct({
    items: S.Array(
      S.Struct({
        id: S.String,
        value: S.Number,
        tags: S.Array(S.String),
      }),
    ),
    metadata: S.Struct({
      created: S.Date,
      score: S.Number,
    }),
  }),
  arrayField: S.Array(
    S.Struct({
      subId: S.String,
      data: S.Array(S.Number),
    }),
  ),
  status: S.Literal("active", "inactive"),
});

describe("CRUD Updates (hardcore desiderata)", () => {
  const dbRegistry = registry("8.0", { complex: ComplexSchema });
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
  }, 120000);

  beforeEach(async () => {
    const complexColl = dbRegistry(db).complex;
    await complexColl.deleteMany(() => ({})).execute();

    const seed: ComplexDoc[] = [
      {
        _id: "c1",
        name: "Alpha",
        nested: {
          items: [
            {
              id: "i1",
              value: 4,
              tags: ["base"],
            },
            {
              id: "i2",
              value: 6,
              tags: ["important"],
            },
          ],
          metadata: {
            created: new Date("2024-01-01T00:00:00.000Z"),
            score: 40,
          },
        },
        arrayField: [
          {
            subId: "target",
            data: [1, 2, 3],
          },
          {
            subId: "temp-1",
            data: [50],
          },
        ],
        status: "active",
      },
      {
        _id: "c2",
        name: "Beta",
        nested: {
          items: [
            {
              id: "i3",
              value: 12,
              tags: ["hot"],
            },
            {
              id: "i4",
              value: 2,
              tags: [],
            },
          ],
          metadata: {
            created: new Date("2024-02-01T00:00:00.000Z"),
            score: 60,
          },
        },
        arrayField: [
          {
            subId: "valid-1",
            data: [5, 6],
          },
          {
            subId: "temp-2",
            data: [25],
          },
        ],
        status: "active",
      },
      {
        _id: "c3",
        name: "Gamma",
        nested: {
          items: [
            {
              id: "i5",
              value: 7,
              tags: ["legacy"],
            },
          ],
          metadata: {
            created: new Date("2024-03-01T00:00:00.000Z"),
            score: 45,
          },
        },
        arrayField: [
          {
            subId: "valid-2",
            data: [9],
          },
        ],
        status: "inactive",
      },
    ];

    await complexColl.insertMany(seed).execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("nested set with arrayFilters", async () => {
    // Traditional update with arrayFilters (no expressions)
    await dbRegistry(db)
      .complex.updateOne(
        $ => ({ _id: "c1" }),
        {
          $set: {
            "nested.metadata.score": 50,
            "nested.items.$[item].value": 12,
          },
        },
        { arrayFilters: [{ "item.value": { $gt: 5 } }] },
      )
      .execute();

    const updated = await dbRegistry(db)
      .complex.findOne($ => ({ _id: "c1" }))
      .toOne();
    assertSync(S.Tuple(ComplexSchema), [updated ?? (null as never)]);

    const item = updated?.nested.items.find(entry => entry.id === "i2");
    expect(updated?.nested.metadata.score).toBe(50);
    expect(item?.value).toBe(12);
  });

  it("push with modifiers", async () => {
    const complexColl = dbRegistry(db).complex;

    await complexColl
      .updateMany($ => ({ status: "active" }), {
        $push: {
          "nested.items": {
            $each: [
              {
                id: "newItem",
                value: 9,
                tags: ["auto", "generated"],
              },
            ],
            $slice: -10,
          },
        },
      })
      .execute();

    const results = await complexColl.find($ => ({ status: "active" })).toList();
    assertSync(S.Array(ComplexSchema), results);
    expect(results.every(doc => doc.nested.items.some(item => item.id === "newItem"))).toBe(true);
  });

  it("pull with query conditions", async () => {
    const complexColl = dbRegistry(db).complex;

    await complexColl
      .updateOne($ => ({ _id: "c2" }), { $pull: { arrayField: { subId: { $regex: "^temp" } } } })
      .execute();

    const updated = await complexColl.findOne($ => ({ _id: "c2" })).toOne();
    assertSync(S.Tuple(ComplexSchema), [updated ?? (null as never)]);
    expect(updated?.arrayField.some(entry => entry.subId.startsWith("temp"))).toBe(false);
  });

  it("inc on nested array element", async () => {
    const complexColl = dbRegistry(db).complex;

    await complexColl
      .updateOne($ => ({ _id: "c1" }), { $inc: { "arrayField.0.data.0": 10 } })
      .execute();

    const updated = await complexColl.findOne($ => ({ _id: "c1" })).toOne();
    assertSync(S.Tuple(ComplexSchema), [updated ?? (null as never)]);
    expect(updated?.arrayField[0]?.data[0]).toBe(11);
  });

  it("complex update with multiple operators and arrayFilters", async () => {
    const complexColl = dbRegistry(db).complex;

    await complexColl
      .updateOne(
        $ => ({
          _id: "c2",
          "nested.items": {
            $elemMatch: {
              value: { $gt: 10 },
              tags: { $in: ["hot"] },
            },
          },
          status: "active",
        }),
        {
          $set: {
            "nested.metadata.score": 65,
            "nested.items.$[item].value": 13,
          },
          $inc: { "arrayField.$[arr].data.0": 1 },
          $push: { "nested.items.$[item].tags": "updated" },
        },
        {
          arrayFilters: [{ "item.value": { $lt: 100 } }, { "arr.subId": { $regex: "^valid" } }],
          upsert: false,
        },
      )
      .execute();

    const updated = await complexColl.findOne($ => ({ _id: "c2" })).toOne();
    assertSync(S.Tuple(ComplexSchema), [updated ?? (null as never)]);
    expect(updated?.nested.metadata.score).toBe(65);
    expect(updated?.nested.items.every(item => item.tags.includes("updated"))).toBe(true);
    const valid = updated?.arrayField.find(entry => entry.subId === "valid-1");
    expect(valid?.data[0]).toBe(6);
  });

  it("addToSet with arrayFilters", async () => {
    const complexColl = dbRegistry(db).complex;

    await complexColl
      .updateOne(
        $ => ({ _id: "c1" }),
        { $addToSet: { "nested.items.$[item].tags": { $each: ["newTag", "anotherTag"] } } },
        { arrayFilters: [{ "item.id": { $exists: true } }] },
      )
      .execute();

    const updated = await complexColl.findOne($ => ({ _id: "c1" })).toOne();
    assertSync(S.Tuple(ComplexSchema), [updated ?? (null as never)]);
    const tags = updated?.nested.items.flatMap(item => item.tags) ?? [];
    expect(tags.includes("newTag")).toBe(true);
    expect(tags.includes("anotherTag")).toBe(true);
  });

  describe("Badass Updates (Maximum Capability Testing)", () => {
    it("should handle mixed $set, $inc, and arrayFilters with dot notation", async () => {
      const complexColl = dbRegistry(db).complex;

      // c1 original state:
      // nested.items: [{id:i1, val:4, tags:[base]}, {id:i2, val:6, tags:[important]}]
      // arrayField: [{subId:target, data:[1,2,3]}, {subId:temp-1, data:[50]}]
      // status: active

      const result = await complexColl
        .updateOne(
          $ => ({ _id: "c1" }),
          {
            $set: {
              "nested.items.$[targetItem].tags": ["updated", "forced"],
              status: "inactive",
            },
            $inc: {
              "nested.metadata.score": 5,
              "nested.items.$[targetItem].value": 10,
            },
            // $mul on mixed nested array
            $mul: { "arrayField.$[].data.$[idx]": 2 },
          },
          {
            arrayFilters: [
              { "targetItem.id": "i1" }, // Matches item 0
              { idx: { $gte: 2 } }, // Matches data values >= 2
            ],
          },
        )
        .execute();

      expect(result.matchedCount).toBe(1);
      expect(result.modifiedCount).toBe(1);

      const doc = await complexColl.findOne($ => ({ _id: "c1" })).toOne();
      if (!doc) throw new Error("Document not found");
      assertSync(S.Tuple(ComplexSchema), [doc]);

      // Verify $set status
      expect(doc.status).toBe("inactive");

      // Verify $inc on metadata
      expect(doc.nested.metadata.score).toBe(45); // 40 + 5

      // Verify nested path with arrayFilters operations
      const i1 = doc.nested.items.find(i => i.id === "i1");
      const i2 = doc.nested.items.find(i => i.id === "i2");

      // i1 matched "targetItem.id": "i1"
      expect(i1?.tags).toEqual(["updated", "forced"]); // $set used filter
      expect(i1?.value).toBe(14); // 4 + 10 ($inc used filter)

      // i2 should be untouched by those ops
      expect(i2?.tags).toEqual(["important"]);
      expect(i2?.value).toBe(6);

      // Verify $mul on arrayField (multi-level array $[] + $[idx])
      const arr0 = doc.arrayField.find(a => a.subId === "target");
      const arr1 = doc.arrayField.find(a => a.subId === "temp-1");

      expect(arr0?.data).toEqual([1, 4, 6]);
      expect(arr1?.data).toEqual([100]);
    });

    it("should handle $pull with complex nested conditions", async () => {
      const complexColl = dbRegistry(db).complex;

      // c2 items: [{id:i3, val:12, tags:[hot]}, {id:i4, val:2, tags:[]}]
      // We want to pull items where (value > 10) OR (tags has "hot")

      const result = await complexColl
        .updateOne($ => ({ _id: "c2" }), {
          $pull: { "nested.items": { $or: [{ value: { $gt: 10 } }, { tags: "hot" }] } },
        })
        .execute();

      expect(result.modifiedCount).toBe(1);
      const doc = await complexColl.findOne($ => ({ _id: "c2" })).toOne();
      if (!doc) throw new Error("Missing doc c2");
      assertSync(S.Tuple(ComplexSchema), [doc]);

      // i3 (val 12, tags hot) should be removed
      // i4 (val 2, tags []) should remain
      expect(doc.nested.items).toHaveLength(1);
      expect(doc.nested.items[0]?.id).toBe("i4");
    });

    it("should handle $push with $each, $sort, and $slice", async () => {
      const complexColl = dbRegistry(db).complex;

      const result = await complexColl
        .updateOne(
          $ => ({ _id: "c2" }),
          {
            $push: {
              "arrayField.$[elem].data": {
                $each: [1, 10, 20],
                $sort: -1,
                $slice: 3,
              },
            },
          },
          { arrayFilters: [{ "elem.subId": "valid-1" }] },
        )
        .execute();

      expect(result.modifiedCount).toBe(1);
      const doc = await complexColl.findOne($ => ({ _id: "c2" })).toOne();
      if (!doc) throw new Error("Missing doc c2");
      assertSync(S.Tuple(ComplexSchema), [doc]);

      const target = doc.arrayField.find(a => a.subId === "valid-1");
      expect(target?.data).toEqual([20, 10, 6]);

      const other = doc.arrayField.find(a => a.subId === "temp-2");
      expect(other?.data).toEqual([25]);
    });

    it("should handle $set with empty array (regression test)", async () => {
      const complexColl = dbRegistry(db).complex;
      await complexColl
        .updateOne($ => ({ _id: "c1" }), { $set: { "nested.items.0.tags": [] } })
        .execute();

      const doc = await complexColl.findOne($ => ({ _id: "c1" })).toOne();
      if (!doc) throw new Error("Missing doc c1");
      assertSync(S.Tuple(ComplexSchema), [doc]);
      expect(doc.nested.items[0]?.tags).toEqual([]);
    });

    it("should validate $addToSet behavior", async () => {
      const complexColl = dbRegistry(db).complex;

      await complexColl
        .updateOne(
          $ => ({
            _id: "c1",
            "arrayField.subId": "target",
          }),
          { $addToSet: { "arrayField.$.data": { $each: [2, 4] } } },
        )
        .execute();

      const doc = await complexColl.findOne($ => ({ _id: "c1" })).toOne();
      if (!doc) throw new Error("Missing doc c1");
      assertSync(S.Tuple(ComplexSchema), [doc]);
      const target = doc.arrayField.find(a => a.subId === "target");
      expect(target?.data).toEqual(expect.arrayContaining([1, 2, 3, 4]));
      expect(target?.data).toHaveLength(4);
    });

    it("should handle misc numeric operators ($mul, $min, $max)", async () => {
      const complexColl = dbRegistry(db).complex;

      // Seed
      await complexColl
        .insertOne({
          _id: "num1",
          name: "Numeric Test",
          nested: {
            items: [],
            metadata: {
              created: new Date(),
              score: 10,
            },
          },
          arrayField: [],
          status: "active",
        })
        .execute();

      // $mul
      await complexColl
        .updateOne($ => ({ _id: "num1" }), { $mul: { "nested.metadata.score": 2 } })
        .execute();

      let doc = await complexColl.findOne($ => ({ _id: "num1" })).toOne();
      expect(doc?.nested.metadata.score).toBe(20);

      // $min
      await complexColl
        .updateOne($ => ({ _id: "num1" }), { $min: { "nested.metadata.score": 5 } })
        .execute();

      doc = await complexColl.findOne($ => ({ _id: "num1" })).toOne();
      expect(doc?.nested.metadata.score).toBe(5);

      // $max
      await complexColl
        .updateOne($ => ({ _id: "num1" }), { $max: { "nested.metadata.score": 50 } })
        .execute();

      doc = await complexColl.findOne($ => ({ _id: "num1" })).toOne();
      expect(doc?.nested.metadata.score).toBe(50);
    });
  });

  describe("Valid MongoDB queries that sluice previously failed to compile", () => {
    it("$pull with complex nested conditions", async () => {
      const complexColl = dbRegistry(db).complex;
      await complexColl
        .updateOne($ => ({ _id: "c1" }), {
          $pull: {
            "nested.items": {
              $and: [{ value: { $gt: 5 } }, { tags: { $in: ["important"] } }],
            },
          },
        })
        .execute();
    });
  });

  describe("Invalid updates caught by type system", () => {
    it("should reject wrong value types at compile time", () => {
      const complexColl = dbRegistry(db).complex;
      // The type system catches this: score is number, not string
      complexColl.updateOne($ => ({ _id: "c1" }), {
        $set: {
          // @ts-expect-error - score is number, "not a number" is string
          "nested.metadata.score": "not a number",
        },
      });
    });

    it("should reject invalid paths at compile time", () => {
      const complexColl = dbRegistry(db).complex;
      // The type system catches this: nonexistent.field is not a valid path
      complexColl.updateOne($ => ({ _id: "c1" }), {
        $set: {
          // @ts-expect-error - nonexistent path
          "nonexistent.field": "value",
        },
      });
    });

    it("should require array filters for nested array updates", async () => {
      const complexColl = dbRegistry(db).complex;
      // This should fail to compile without arrayFilters but might not be fully enforced
      await expect(async () => {
        await complexColl
          .updateOne($ => ({ _id: "c1" }), {
            $set: {
              "arrayField.$.data.0": 999, // Missing arrayFilters - may not be caught
            },
          })
          .execute();
      }).rejects.toThrow();
    });

    it("should catch $pull with invalid conditions", async () => {
      const complexColl = dbRegistry(db).complex;
      // Complex $pull conditions might not be fully validated
      await complexColl
        .updateOne($ => ({ _id: "c1" }), {
          $pull: {
            "nested.items": {
              // @ts-expect-error Testing invalid operator in $pull
              invalidOperator: { $gt: 5 },
            },
          },
        })
        .execute();
    });

    it("should reject multiple operators on same field (MongoDB limitation)", async () => {
      const complexColl = dbRegistry(db).complex;
      await expect(async () => {
        await complexColl
          // @ts-expect-error - Conflicting path updates
          .updateOne($ => ({ _id: "c1" }), {
            $set: { "nested.metadata.score": 100 },
            $inc: { "nested.metadata.score": 10 }, // Same field as $set above
          })
          .execute();
      }).rejects.toThrow();
    });

    it("should reject updating field and its subfield (MongoDB limitation)", async () => {
      const complexColl = dbRegistry(db).complex;
      // MongoDB limitation: Cannot update a field and then a subfield of that field
      // This should fail at runtime with conflict error
      await expect(async () => {
        await complexColl
          // @ts-expect-error - Conflicting path updates
          .updateOne($ => ({ _id: "c1" }), {
            $set: {
              "nested.metadata": { created: new Date(), score: 200 }, // Update parent
              "nested.metadata.score": 300, // Update child - conflict!
            },
          })
          .execute();
      }).rejects.toThrow();
    });

    it("should reject paths with double $ (.$. pattern)", async () => {
      const complexColl = dbRegistry(db).complex;
      await expect(async () => {
        await complexColl
          .updateOne($ => ({ _id: "c1" }), {
            $set: {
              // @ts-expect-error - This should fail to compile due to double $ / invalid path
              "arrayField.$.data.$.value": "invalid", // Contains .$.
            },
          })
          .execute();
      }).rejects.toThrow();
    });

    it("should reject array filter paths with double $", async () => {
      const complexColl = dbRegistry(db).complex;
      // sluice should reject paths containing .$. in array filter context
      await expect(async () => {
        await complexColl
          .updateOne(
            $ => ({ _id: "c1" }),
            {
              $set: {
                // @ts-expect-error - This should fail to compile due to double $
                "arrayField.$[item].data.$.value": "invalid", // Contains .$.
              },
            },
            { arrayFilters: [{ "item.subId": "target" }] },
          )
          .execute();
      }).rejects.toThrow();
    });
  });
});
