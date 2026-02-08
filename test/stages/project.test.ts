import { Schema as S } from "@effect/schema";
import { $addFields, $match, $project, $sort, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

const ProductSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  price: S.Number,
  category: S.String,
  stock: S.Number,
  tags: S.Array(S.String),
  metadata: S.Struct({
    weight: S.Number,
    color: S.String,
  }),
});

const dbReg = registry("8.0", { products: ProductSchema });

describe("Project Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const ctx = await setup();
    db = ctx.db;

    const coll = dbReg(db).products;
    await coll
      .insertMany([
        {
          _id: new ObjectId(),
          name: "Widget",
          price: 10,
          category: "tools",
          stock: 50,
          tags: ["sale"],
          metadata: { weight: 0.5, color: "red" },
        },
        {
          _id: new ObjectId(),
          name: "Gadget",
          price: 25,
          category: "electronics",
          stock: 10,
          tags: ["new"],
          metadata: { weight: 1.2, color: "blue" },
        },
      ])
      .execute();
  });

  afterAll(teardown);

  it("should include specific fields", async () => {
    const coll = dbReg(db).products;

    const result = await coll.aggregate($project($ => ({ name: 1, price: 1 }))).toList();

    expect(result.length).toBe(2);
    expectType<{ name: string; price: number }[]>(result);
    expect(result[0]).toHaveProperty("name");
    expect(result[0]).toHaveProperty("price");
  });

  it("should exclude specific fields", async () => {
    const coll = dbReg(db).products;

    const result = await coll.aggregate($project($ => ({ tags: 0, metadata: 0 }))).toList();

    expect(result.length).toBe(2);
    expectType<{ _id: ObjectId }[]>(result);
    expect(result[0]).not.toHaveProperty("tags");
    expect(result[0]).not.toHaveProperty("metadata");
  });

  it("should compute new fields with expressions", async () => {
    const coll = dbReg(db).products;

    const result = await coll
      .aggregate(
        $match(() => ({ name: "Widget" })),
        $project($ => ({
          name: 1,
          totalValue: $.multiply("$price", "$stock"),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expectType<{ name: string; totalValue: number }[]>(result);
    expect(result[0]?.totalValue).toBe(500);
  });

  it("should project nested field reference", async () => {
    const coll = dbReg(db).products;

    const result = await coll
      .aggregate(
        $match(() => ({ name: "Widget" })),
        $project($ => ({
          name: 1,
          color: "$metadata.color",
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.color).toBe("red");
  });

  it("should chain project with addFields and match", async () => {
    const coll = dbReg(db).products;

    const result = await coll
      .aggregate(
        $addFields($ => ({ value: $.multiply("$price", "$stock") })),
        $match(() => ({ value: { $gte: 250 } })),
        $project($ => ({ name: 1, value: 1 })),
      )
      .toList();

    expect(result.length).toBe(2);
    // Widget: 10*50=500, Gadget: 25*10=250
    for (const doc of result) {
      expect(doc.value).toBeGreaterThanOrEqual(250);
    }
  });

  it("should project with $cond expression", async () => {
    const coll = dbReg(db).products;

    const result = await coll
      .aggregate(
        $addFields($ => ({
          inStock: $.cond({ if: $.gt("$stock", 0), then: true as const, else: false as const }),
        })),
        $project($ => ({
          name: 1,
          inStock: 1,
        })),
        $sort({ name: 1 }),
      )
      .toList();

    expect(result.length).toBe(2);
    for (const doc of result) {
      expect(typeof doc.inStock).toBe("boolean");
    }
  });

  it("should project array size", async () => {
    const coll = dbReg(db).products;

    const result = await coll
      .aggregate(
        $match(() => ({ name: "Widget" })),
        $project($ => ({
          name: 1,
          tagCount: $.size("$tags"),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.tagCount).toBe(1); // ["sale"]
  });
});
