// ==========================================
// Accumulator Tests
// ==========================================

import { Schema as S } from "@effect/schema";
import {
  $group,
  $project,
  $setWindowFields,
  $sort,
  registry,
} from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

// Define schemas using @effect/schema
const SalesCollectionSchema = S.Struct({
  _id: S.instanceOf(ObjectId),
  productId: S.String,
  customerId: S.String,
  quantity: S.Number,
  price: S.Number,
  category: S.String,
  date: S.Date,
  ratings: S.Array(S.Number),
  tags: S.Array(S.String),
  metadata: S.Struct({
    source: S.String,
    version: S.Number,
  }),
});
type SalesCollection = typeof SalesCollectionSchema.Type;

const ProductCollectionSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  category: S.String,
  price: S.Number,
  stock: S.Number,
  ratings: S.Array(S.Number),
  tags: S.Array(S.String),
});
type ProductCollection = typeof ProductCollectionSchema.Type;

const productSeed = [
  {
    _id: new ObjectId("000000000000000000000001"),
    name: "Widget",
    category: "electronics",
    price: 100,
    stock: 10,
    ratings: [4, 5],
    tags: ["new", "featured"],
  },
  {
    _id: new ObjectId("000000000000000000000002"),
    name: "Gadget",
    category: "electronics",
    price: 200,
    stock: 5,
    ratings: [3, 4],
    tags: ["sale", "featured"],
  },
  {
    _id: new ObjectId("000000000000000000000003"),
    name: "Novel",
    category: "books",
    price: 20,
    stock: 30,
    ratings: [5, 4, 5],
    tags: ["paperback"],
  },
  {
    _id: new ObjectId("000000000000000000000004"),
    name: "Guide",
    category: "books",
    price: 30,
    stock: 15,
    ratings: [4, 4],
    tags: ["paperback", "bestseller"],
  },
];

const salesIds = {
  s1: new ObjectId("64b000000000000000000001"),
  s2: new ObjectId("64b000000000000000000002"),
  s3: new ObjectId("64b000000000000000000003"),
  s4: new ObjectId("64b000000000000000000004"),
  s5: new ObjectId("64b000000000000000000005"),
  s6: new ObjectId("64b000000000000000000006"),
};

const salesSeed = [
  {
    _id: salesIds.s1,
    productId: "p1",
    customerId: "c1",
    quantity: 2,
    price: 100,
    category: "electronics",
    date: new Date("2024-01-01T00:00:00.000Z"),
    ratings: [5, 4, 4, 5],
    tags: ["new", "featured"],
    metadata: {
      source: "web",
      version: 1,
    },
  },
  {
    _id: salesIds.s2,
    productId: "p2",
    customerId: "c2",
    quantity: 1,
    price: 200,
    category: "electronics",
    date: new Date("2024-01-02T00:00:00.000Z"),
    ratings: [3, 4, 5],
    tags: ["sale"],
    metadata: {
      source: "app",
      version: 1,
    },
  },
  {
    _id: salesIds.s3,
    productId: "p1",
    customerId: "c3",
    quantity: 3,
    price: 150,
    category: "electronics",
    date: new Date("2024-01-03T00:00:00.000Z"),
    ratings: [4, 4, 4],
    tags: ["featured", "bundle"],
    metadata: {
      source: "web",
      version: 2,
    },
  },
  {
    _id: salesIds.s4,
    productId: "p3",
    customerId: "c1",
    quantity: 5,
    price: 20,
    category: "books",
    date: new Date("2024-01-01T00:00:00.000Z"),
    ratings: [5, 5, 4],
    tags: ["paperback"],
    metadata: {
      source: "web",
      version: 1,
    },
  },
  {
    _id: salesIds.s5,
    productId: "p4",
    customerId: "c2",
    quantity: 2,
    price: 30,
    category: "books",
    date: new Date("2024-01-04T00:00:00.000Z"),
    ratings: [4, 3],
    tags: ["bestseller", "paperback"],
    metadata: {
      source: "app",
      version: 1,
    },
  },
  {
    _id: salesIds.s6,
    productId: "p3",
    customerId: "c3",
    quantity: 1,
    price: 25,
    category: "books",
    date: new Date("2024-01-05T00:00:00.000Z"),
    ratings: [5, 4, 4, 5],
    tags: ["paperback", "gift"],
    metadata: {
      source: "web",
      version: 2,
    },
  },
];

describe("Accumulators", () => {
  const dbRegistry = registry("8.0", {
    sales: SalesCollectionSchema,
    products: ProductCollectionSchema,
  });
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    const boundRegistry = dbRegistry(db);

    await boundRegistry.sales.deleteMany(() => ({})).execute();
    await boundRegistry.products.deleteMany(() => ({})).execute();

    await boundRegistry.products.insertMany(productSeed).execute();
    await boundRegistry.sales.insertMany(salesSeed).execute();
  });

  afterAll(async () => {
    await teardown();
  });

  // ToDo: insert data and make actual runtime tests, use S.Schema and validate to validate results, like in $addToSet tests below
  describe("$addToSet", () => {
    it("collects unique values from field", async () => {
      const res = await dbRegistry(db)
        .products.aggregate(
          $group($ => ({
            _id: "$category",
            uniqueTags: $.addToSet("$tags"),
          })),
          $project($ => ({
            _id: 1,
            uniqueTags: $.sortArray({
              input: "$uniqueTags",
              sortBy: 1,
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.String, uniqueTags: S.Array(S.Array(S.String)) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", uniqueTags: [["paperback"], ["paperback", "bestseller"]] },
        {
          _id: "electronics",
          uniqueTags: [
            ["new", "featured"],
            ["sale", "featured"],
          ],
        },
      ]);
    });

    it("collects unique values from expression", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            uniqueSources: $.addToSet("$metadata.source"),
          })),
          $project($ => ({
            _id: 1,
            uniqueSources: $.sortArray({
              input: "$uniqueSources",
              sortBy: 1,
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, uniqueSources: S.Array(S.String) }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", uniqueSources: ["app", "web"] },
        { _id: "electronics", uniqueSources: ["app", "web"] },
      ]);
    });
  });

  describe("$avg", () => {
    it("calculates average of numeric field", async () => {
      const res = await dbRegistry(db)
        .products.aggregate(
          $group($ => ({
            _id: "$category",
            avgPrice: $.avg("$price"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.String, avgPrice: S.Union(S.Number, S.Null) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", avgPrice: 25 },
        { _id: "electronics", avgPrice: 150 },
      ]);
    });

    it("calculates average of numeric array", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$_id",
            avgRating: $.avg("$ratings"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), avgRating: S.Union(S.Number, S.Null) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s1, avgRating: null },
        { _id: salesIds.s2, avgRating: null },
        { _id: salesIds.s3, avgRating: null },
        { _id: salesIds.s4, avgRating: null },
        { _id: salesIds.s5, avgRating: null },
        { _id: salesIds.s6, avgRating: null },
      ]);
    });

    it("calculates average of expression", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            avgTotal: $.avg($.multiply("$quantity", "$price")),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.String, avgTotal: S.Union(S.Number, S.Null) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", avgTotal: 185 / 3 },
        { _id: "electronics", avgTotal: 850 / 3 },
      ]);
    });
  });

  describe("$sum", () => {
    it("sums numeric field", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            totalQuantity: $.sum("$quantity"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, totalQuantity: S.Number }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", totalQuantity: 8 },
        { _id: "electronics", totalQuantity: 6 },
      ]);
    });

    it("sums numeric array", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$_id",
            totalRatings: $.sum("$ratings"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), totalRatings: S.Number }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s1, totalRatings: 0 },
        { _id: salesIds.s2, totalRatings: 0 },
        { _id: salesIds.s3, totalRatings: 0 },
        { _id: salesIds.s4, totalRatings: 0 },
        { _id: salesIds.s5, totalRatings: 0 },
        { _id: salesIds.s6, totalRatings: 0 },
      ]);
    });

    it("counts documents with sum(1)", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            count: $.sum(1),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, count: S.Number }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", count: 3 },
        { _id: "electronics", count: 3 },
      ]);
    });

    it("sums expression", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            totalValue: $.sum($.multiply("$quantity", "$price")),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, totalValue: S.Number }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", totalValue: 185 },
        { _id: "electronics", totalValue: 850 },
      ]);
    });
  });

  describe("$push", () => {
    it("pushes field values into array", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $sort({ date: 1 }),
          $group($ => ({
            _id: "$category",
            allPrices: $.push("$price"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, allPrices: S.Array(S.Number) }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", allPrices: [20, 30, 25] },
        { _id: "electronics", allPrices: [100, 200, 150] },
      ]);
    });

    it("pushes object literals into array", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $sort({ date: 1 }),
          $group($ => ({
            _id: "$category",
            items: $.push({
              productId: "$productId",
              quantity: "$quantity",
              price: "$price",
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({
          _id: S.String,
          items: S.Array(
            S.Struct({
              productId: S.String,
              quantity: S.Number,
              price: S.Number,
            }),
          ),
        }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        {
          _id: "books",
          items: [
            {
              productId: "p3",
              quantity: 5,
              price: 20,
            },
            {
              productId: "p4",
              quantity: 2,
              price: 30,
            },
            {
              productId: "p3",
              quantity: 1,
              price: 25,
            },
          ],
        },
        {
          _id: "electronics",
          items: [
            {
              productId: "p1",
              quantity: 2,
              price: 100,
            },
            {
              productId: "p2",
              quantity: 1,
              price: 200,
            },
            {
              productId: "p1",
              quantity: 3,
              price: 150,
            },
          ],
        },
      ]);
    });

    it("pushes expression results into array", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $sort({ date: 1 }),
          $group($ => ({
            _id: "$category",
            totals: $.push($.multiply("$quantity", "$price")),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, totals: S.Array(S.Number) }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", totals: [100, 60, 25] },
        { _id: "electronics", totals: [200, 200, 450] },
      ]);
    });
  });

  describe("$first", () => {
    it("gets first value of field in group", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $sort({ date: 1 }),
          $group($ => ({
            _id: "$category",
            firstProduct: $.first("$productId"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, firstProduct: S.String }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", firstProduct: "p3" },
        { _id: "electronics", firstProduct: "p1" },
      ]);
    });

    it("gets first element of array expression", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$_id",
            firstTag: $.first("$tags"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), firstTag: S.Array(S.String) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s1, firstTag: ["new", "featured"] },
        { _id: salesIds.s2, firstTag: ["sale"] },
        { _id: salesIds.s3, firstTag: ["featured", "bundle"] },
        { _id: salesIds.s4, firstTag: ["paperback"] },
        { _id: salesIds.s5, firstTag: ["bestseller", "paperback"] },
        { _id: salesIds.s6, firstTag: ["paperback", "gift"] },
      ]);
    });
  });

  describe("$last", () => {
    it("gets last value of field in group", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $sort({ date: 1 }),
          $group($ => ({
            _id: "$category",
            lastProduct: $.last("$productId"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, lastProduct: S.String }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", lastProduct: "p3" },
        { _id: "electronics", lastProduct: "p1" },
      ]);
    });

    it("gets last element of array expression", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$_id",
            lastTag: $.last("$tags"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), lastTag: S.Array(S.String) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s1, lastTag: ["new", "featured"] },
        { _id: salesIds.s2, lastTag: ["sale"] },
        { _id: salesIds.s3, lastTag: ["featured", "bundle"] },
        { _id: salesIds.s4, lastTag: ["paperback"] },
        { _id: salesIds.s5, lastTag: ["bestseller", "paperback"] },
        { _id: salesIds.s6, lastTag: ["paperback", "gift"] },
      ]);
    });
  });

  describe("$max", () => {
    it("gets maximum value in group", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            maxPrice: $.max("$price"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, maxPrice: S.Number }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", maxPrice: 30 },
        { _id: "electronics", maxPrice: 200 },
      ]);
    });

    it("gets maximum of multiple arguments", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            maxValue: $.max($.max("$price", $.multiply("$quantity", 2))),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, maxValue: S.Number }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", maxValue: 30 },
        { _id: "electronics", maxValue: 200 },
      ]);
    });
  });

  describe("$min", () => {
    it("gets minimum value in group", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            minPrice: $.min("$price"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, minPrice: S.Number }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", minPrice: 20 },
        { _id: "electronics", minPrice: 100 },
      ]);
    });

    it("gets minimum of multiple arguments", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            minValue: $.min($.min("$price", $.multiply("$quantity", 0.5))),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, minValue: S.Number }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", minValue: 0.5 },
        { _id: "electronics", minValue: 0.5 },
      ]);
    });
  });

  describe("$stdDevPop", () => {
    it("calculates population standard deviation", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            stdDev: $.stdDevPop("$price"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, stdDev: S.NullOr(S.Number) }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", stdDev: Math.sqrt(50 / 3) },
        { _id: "electronics", stdDev: Math.sqrt(5000 / 3) },
      ]);
    });
  });

  describe("$stdDevSamp", () => {
    it("calculates sample standard deviation", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            stdDev: $.stdDevSamp("$price"),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, stdDev: S.NullOr(S.Number) }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", stdDev: 5 },
        { _id: "electronics", stdDev: 50 },
      ]);
    });
  });

  describe("$firstN", () => {
    it("gets first N elements from array", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$_id",
            firstThreeTags: $.firstN({
              input: "$tags",
              n: 3,
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), firstThreeTags: S.Array(S.Array(S.String)) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s1, firstThreeTags: [["new", "featured"]] },
        { _id: salesIds.s2, firstThreeTags: [["sale"]] },
        { _id: salesIds.s3, firstThreeTags: [["featured", "bundle"]] },
        { _id: salesIds.s4, firstThreeTags: [["paperback"]] },
        { _id: salesIds.s5, firstThreeTags: [["bestseller", "paperback"]] },
        { _id: salesIds.s6, firstThreeTags: [["paperback", "gift"]] },
      ]);
    });
  });

  describe("$lastN", () => {
    it("gets last N elements from array", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$_id",
            lastThreeTags: $.lastN({
              input: "$tags",
              n: 3,
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), lastThreeTags: S.Array(S.Array(S.String)) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s1, lastThreeTags: [["new", "featured"]] },
        { _id: salesIds.s2, lastThreeTags: [["sale"]] },
        { _id: salesIds.s3, lastThreeTags: [["featured", "bundle"]] },
        { _id: salesIds.s4, lastThreeTags: [["paperback"]] },
        { _id: salesIds.s5, lastThreeTags: [["bestseller", "paperback"]] },
        { _id: salesIds.s6, lastThreeTags: [["paperback", "gift"]] },
      ]);
    });
  });

  describe("$maxN", () => {
    it("gets top N maximum values from array", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$_id",
            topThreeRatings: $.maxN({
              input: "$ratings",
              n: 3,
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), topThreeRatings: S.Array(S.Array(S.Number)) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s1, topThreeRatings: [[5, 4, 4, 5]] },
        { _id: salesIds.s2, topThreeRatings: [[3, 4, 5]] },
        { _id: salesIds.s3, topThreeRatings: [[4, 4, 4]] },
        { _id: salesIds.s4, topThreeRatings: [[5, 5, 4]] },
        { _id: salesIds.s5, topThreeRatings: [[4, 3]] },
        { _id: salesIds.s6, topThreeRatings: [[5, 4, 4, 5]] },
      ]);
    });
  });

  describe("$minN", () => {
    it("gets top N minimum values from array", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$_id",
            bottomThreeRatings: $.minN({
              input: "$ratings",
              n: 3,
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), bottomThreeRatings: S.Array(S.Array(S.Number)) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s1, bottomThreeRatings: [[5, 4, 4, 5]] },
        { _id: salesIds.s2, bottomThreeRatings: [[3, 4, 5]] },
        { _id: salesIds.s3, bottomThreeRatings: [[4, 4, 4]] },
        { _id: salesIds.s4, bottomThreeRatings: [[5, 5, 4]] },
        { _id: salesIds.s5, bottomThreeRatings: [[4, 3]] },
        { _id: salesIds.s6, bottomThreeRatings: [[5, 4, 4, 5]] },
      ]);
    });
  });

  it("calculates percentiles", async () => {
    const res = await dbRegistry(db)
      .sales.aggregate(
        $group($ => ({
          _id: "$category",
          percentiles: $.percentile({
            input: "$price",
            p: [0.5, 0.9, 0.95],
            method: "approximate",
          }),
        })),
        $sort({ _id: 1 }),
      )
      .toList();

    const expectedSchema = S.Array(
      S.Struct({
        _id: S.String,
        percentiles: S.Array(S.Number),
      }),
    );
    assertSync(expectedSchema, res);

    expect(res).toEqual([
      {
        _id: "books",
        percentiles: [25, 30, 30],
      },
      {
        _id: "electronics",
        percentiles: [150, 200, 200],
      },
    ]);
  });

  it("calculates median", async () => {
    const res = await dbRegistry(db)
      .sales.aggregate(
        $group($ => ({
          _id: "$category",
          medianPrice: $.median({ input: "$price", method: "approximate" }),
        })),
        $sort({ _id: 1 }),
      )
      .toList();

    const expectedSchema = S.Array(
      S.Struct({
        _id: S.String,
        medianPrice: S.Number,
      }),
    );
    assertSync(expectedSchema, res);

    expect(res).toEqual([
      {
        _id: "books",
        medianPrice: 25,
      },
      {
        _id: "electronics",
        medianPrice: 150,
      },
    ]);
  });

  describe("$top", () => {
    it("gets top value by sort criteria", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            topProduct: $.top({
              output: "$productId",
              sortBy: { price: -1 },
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, topProduct: S.String }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", topProduct: "p4" },
        { _id: "electronics", topProduct: "p2" },
      ]);
    });
  });

  describe("$topN", () => {
    it("gets top N values by sort criteria", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            topProducts: $.topN({
              output: "$productId",
              sortBy: { price: -1 },
              n: 5,
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, topProducts: S.Array(S.String) }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", topProducts: ["p4", "p3", "p3"] },
        { _id: "electronics", topProducts: ["p2", "p1", "p1"] },
      ]);
    });
  });

  describe("$bottom", () => {
    it("gets bottom value by sort criteria", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            bottomProduct: $.bottom({
              output: "$productId",
              sortBy: { price: 1 },
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, bottomProduct: S.String }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", bottomProduct: "p4" },
        { _id: "electronics", bottomProduct: "p2" },
      ]);
    });
  });

  describe("$bottomN", () => {
    it("gets bottom N values by sort criteria", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            bottomProducts: $.bottomN({
              output: "$productId",
              sortBy: { price: 1 },
              n: 5,
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.String, bottomProducts: S.Array(S.String) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", bottomProducts: ["p3", "p3", "p4"] },
        { _id: "electronics", bottomProducts: ["p1", "p1", "p2"] },
      ]);
    });
  });

  describe("$rank", () => {
    it("calculates rank in window", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { price: -1 },
            output: { rank: $.rank() },
          })),
          $sort({
            category: 1,
            price: -1,
          }),
          $project($ => ({
            _id: 1,
            category: 1,
            price: 1,
            rank: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({
          _id: S.instanceOf(ObjectId),
          category: S.String,
          price: S.Number,
          rank: S.Number,
        }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s5, category: "books", price: 30, rank: 1 },
        { _id: salesIds.s6, category: "books", price: 25, rank: 2 },
        { _id: salesIds.s4, category: "books", price: 20, rank: 3 },
        { _id: salesIds.s2, category: "electronics", price: 200, rank: 1 },
        { _id: salesIds.s3, category: "electronics", price: 150, rank: 2 },
        { _id: salesIds.s1, category: "electronics", price: 100, rank: 3 },
      ]);
    });
  });

  describe("$denseRank", () => {
    it("calculates dense rank in window", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { price: -1 },
            output: { denseRank: $.denseRank() },
          })),
          $sort({
            category: 1,
            price: -1,
          }),
          $project($ => ({
            _id: 1,
            category: 1,
            price: 1,
            denseRank: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({
          _id: S.instanceOf(ObjectId),
          category: S.String,
          price: S.Number,
          denseRank: S.Number,
        }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s5, category: "books", price: 30, denseRank: 1 },
        { _id: salesIds.s6, category: "books", price: 25, denseRank: 2 },
        { _id: salesIds.s4, category: "books", price: 20, denseRank: 3 },
        { _id: salesIds.s2, category: "electronics", price: 200, denseRank: 1 },
        { _id: salesIds.s3, category: "electronics", price: 150, denseRank: 2 },
        { _id: salesIds.s1, category: "electronics", price: 100, denseRank: 3 },
      ]);
    });
  });

  describe("$documentNumber", () => {
    it("calculates document number in window", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { date: 1 },
            output: { docNum: $.documentNumber() },
          })),
          $sort({
            category: 1,
            date: 1,
          }),
          $project($ => ({
            _id: 1,
            category: 1,
            date: 1,
            docNum: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({
          _id: S.instanceOf(ObjectId),
          category: S.String,
          date: S.Date,
          docNum: S.Number,
        }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        {
          _id: salesIds.s4,
          category: "books",
          date: new Date("2024-01-01T00:00:00.000Z"),
          docNum: 1,
        },
        {
          _id: salesIds.s5,
          category: "books",
          date: new Date("2024-01-04T00:00:00.000Z"),
          docNum: 2,
        },
        {
          _id: salesIds.s6,
          category: "books",
          date: new Date("2024-01-05T00:00:00.000Z"),
          docNum: 3,
        },
        {
          _id: salesIds.s1,
          category: "electronics",
          date: new Date("2024-01-01T00:00:00.000Z"),
          docNum: 1,
        },
        {
          _id: salesIds.s2,
          category: "electronics",
          date: new Date("2024-01-02T00:00:00.000Z"),
          docNum: 2,
        },
        {
          _id: salesIds.s3,
          category: "electronics",
          date: new Date("2024-01-03T00:00:00.000Z"),
          docNum: 3,
        },
      ]);
    });
  });

  describe("$shift", () => {
    it("shifts values in window", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { date: 1 },
            output: {
              prevPrice: {
                $shift: {
                  output: "$price",
                  by: -1,
                },
              },
            },
          })),
          $sort({
            category: 1,
            date: 1,
          }),
          $project($ => ({
            _id: 1,
            category: 1,
            date: 1,
            prevPrice: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({
          _id: S.instanceOf(ObjectId),
          category: S.String,
          date: S.Date,
          prevPrice: S.Union(S.Number, S.Null),
        }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        {
          _id: salesIds.s4,
          category: "books",
          date: new Date("2024-01-01T00:00:00.000Z"),
          prevPrice: null,
        },
        {
          _id: salesIds.s5,
          category: "books",
          date: new Date("2024-01-04T00:00:00.000Z"),
          prevPrice: 20,
        },
        {
          _id: salesIds.s6,
          category: "books",
          date: new Date("2024-01-05T00:00:00.000Z"),
          prevPrice: 30,
        },
        {
          _id: salesIds.s1,
          category: "electronics",
          date: new Date("2024-01-01T00:00:00.000Z"),
          prevPrice: null,
        },
        {
          _id: salesIds.s2,
          category: "electronics",
          date: new Date("2024-01-02T00:00:00.000Z"),
          prevPrice: 100,
        },
        {
          _id: salesIds.s3,
          category: "electronics",
          date: new Date("2024-01-03T00:00:00.000Z"),
          prevPrice: 200,
        },
      ]);
    });

    it("shifts values with default", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { date: 1 },
            output: {
              prevPrice: {
                $shift: {
                  output: "$price",
                  by: -1,
                  default: 0,
                },
              },
            },
          })),
          $sort({
            category: 1,
            date: 1,
          }),
          $project($ => ({
            _id: 1,
            category: 1,
            date: 1,
            prevPrice: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({
          _id: S.instanceOf(ObjectId),
          category: S.String,
          date: S.Date,
          prevPrice: S.Number,
        }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        {
          _id: salesIds.s4,
          category: "books",
          date: new Date("2024-01-01T00:00:00.000Z"),
          prevPrice: 0,
        },
        {
          _id: salesIds.s5,
          category: "books",
          date: new Date("2024-01-04T00:00:00.000Z"),
          prevPrice: 20,
        },
        {
          _id: salesIds.s6,
          category: "books",
          date: new Date("2024-01-05T00:00:00.000Z"),
          prevPrice: 30,
        },
        {
          _id: salesIds.s1,
          category: "electronics",
          date: new Date("2024-01-01T00:00:00.000Z"),
          prevPrice: 0,
        },
        {
          _id: salesIds.s2,
          category: "electronics",
          date: new Date("2024-01-02T00:00:00.000Z"),
          prevPrice: 100,
        },
        {
          _id: salesIds.s3,
          category: "electronics",
          date: new Date("2024-01-03T00:00:00.000Z"),
          prevPrice: 200,
        },
      ]);
    });
  });

  describe("$expMovingAvg", () => {
    it("calculates exponential moving average", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { date: 1 },
            output: {
              ema: {
                $expMovingAvg: {
                  input: "$price",
                  N: 5,
                },
              },
            },
          })),
          $sort({
            category: 1,
            date: 1,
          }),
          $project($ => ({
            _id: 1,
            ema: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), ema: S.NullOr(S.Number) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s4, ema: 20 },
        { _id: salesIds.s5, ema: 23.333333333333332 },
        { _id: salesIds.s6, ema: 23.88888888888889 },
        { _id: salesIds.s1, ema: 100 },
        { _id: salesIds.s2, ema: 133.33333333333334 },
        { _id: salesIds.s3, ema: 138.88888888888889 },
      ]);
    });

    it("calculates exponential moving average with alpha", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { date: 1 },
            output: {
              ema: {
                $expMovingAvg: {
                  // ToDo: IN what cases $expMovingAvg can be null?
                  input: "$price",
                  alpha: 0.1,
                },
              },
            },
          })),
          $sort({
            category: 1,
            date: 1,
          }),
          $project($ => ({
            _id: 1,
            ema: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), ema: S.NullOr(S.Number) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s4, ema: 20 },
        { _id: salesIds.s5, ema: 21 },
        { _id: salesIds.s6, ema: 21.4 },
        { _id: salesIds.s1, ema: 100 },
        { _id: salesIds.s2, ema: 110 },
        { _id: salesIds.s3, ema: 114 },
      ]);
    });
  });

  describe("$derivative", () => {
    it("calculates derivative", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { date: 1 },
            output: {
              derivative: {
                $derivative: {
                  input: "$price",
                  unit: "day",
                },
                window: { documents: [-1, 0] },
              },
            },
          })),
          $sort({
            category: 1,
            date: 1,
          }),
          $project($ => ({
            _id: 1,
            derivative: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), derivative: S.Union(S.Number, S.Null) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s4, derivative: null },
        { _id: salesIds.s5, derivative: 10 / 3 },
        { _id: salesIds.s6, derivative: -5 },
        { _id: salesIds.s1, derivative: null },
        { _id: salesIds.s2, derivative: 100 },
        { _id: salesIds.s3, derivative: -50 },
      ]);
    });
  });

  describe("$integral", () => {
    it("calculates integral", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { date: 1 },
            output: {
              integral: {
                $integral: {
                  input: "$price",
                  unit: "day",
                },
              },
            },
          })),
          $sort({
            category: 1,
            date: 1,
          }),
          $project($ => ({
            _id: 1,
            integral: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), integral: S.NullOr(S.Number) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s4, integral: 102.5 },
        { _id: salesIds.s5, integral: 102.5 },
        { _id: salesIds.s6, integral: 102.5 },
        { _id: salesIds.s1, integral: 325 },
        { _id: salesIds.s2, integral: 325 },
        { _id: salesIds.s3, integral: 325 },
      ]);
    });
  });

  describe("$linearFill", () => {
    it("fills missing values with linear interpolation", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { date: 1 },
            output: { filledPrice: $.linearFill("$price") },
          })),
          $sort({
            category: 1,
            date: 1,
          }),
          $project($ => ({
            _id: 1,
            filledPrice: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), filledPrice: S.Number }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s4, filledPrice: 20 },
        { _id: salesIds.s5, filledPrice: 30 },
        { _id: salesIds.s6, filledPrice: 25 },
        { _id: salesIds.s1, filledPrice: 100 },
        { _id: salesIds.s2, filledPrice: 200 },
        { _id: salesIds.s3, filledPrice: 150 },
      ]);
    });
  });

  describe("$count", () => {
    it("counts documents in group", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $group($ => ({
            _id: "$category",
            docCount: $.count({}),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, docCount: S.Number }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", docCount: 3 },
        { _id: "electronics", docCount: 3 },
      ]);
    });
  });

  describe("$accumulator", () => {
    it("defines custom accumulator", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $sort({
            category: 1,
            date: 1,
          }),
          $group($ => ({
            _id: "$category",
            customSum: $.accumulator({
              init: () => 0,
              initArgs: [],
              accumulate: (state: number, value: number) => state + value,
              accumulateArgs: ["$price"],
              merge: (state1, state2) => state1 + state2,
              lang: "js",
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, customSum: S.Number }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", customSum: 75 },
        { _id: "electronics", customSum: 450 },
      ]);
    });

    it("defines custom accumulator with initArgs", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $sort({
            category: 1,
            date: 1,
          }),
          $group($ => ({
            _id: "$category",
            customCalc: $.accumulator({
              init: (state: number, value: number) => state,
              initArgs: [100, 0],
              accumulate: (state: number, value: number) => state * value,
              accumulateArgs: ["$quantity"],
              merge: (state1, state2) => state1 + state2,
              finalize: state => state / 2,
              lang: "js",
            }),
          })),
          $sort({ _id: 1 }),
        )
        .toList();

      const expectedSchema = S.Array(S.Struct({ _id: S.String, customCalc: S.Number }));
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: "books", customCalc: 500 },
        { _id: "electronics", customCalc: 300 },
      ]);
    });
  });
});

// ==========================================
// Window Operator Tests
// ==========================================

describe("Window Operators", () => {
  const dbRegistry = registry("8.0", {
    sales: SalesCollectionSchema,
    products: ProductCollectionSchema,
  });
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    const boundRegistry = dbRegistry(db);

    await boundRegistry.sales.deleteMany(() => ({})).execute();
    await boundRegistry.products.deleteMany(() => ({})).execute();

    await boundRegistry.products.insertMany(productSeed).execute();
    await boundRegistry.sales.insertMany(salesSeed).execute();
  });

  afterAll(async () => {
    await teardown();
  });

  describe("$setWindowFields with window options", () => {
    it("allows window options for sum", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { date: 1 },
            output: {
              runningTotal: {
                $sum: "$quantity",
                window: { documents: ["unbounded", "current"] },
              },
            },
          })),
          $sort({
            category: 1,
            date: 1,
          }),
          $project($ => ({
            _id: 1,
            runningTotal: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), runningTotal: S.Number }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s4, runningTotal: 5 },
        { _id: salesIds.s5, runningTotal: 7 },
        { _id: salesIds.s6, runningTotal: 8 },
        { _id: salesIds.s1, runningTotal: 2 },
        { _id: salesIds.s2, runningTotal: 3 },
        { _id: salesIds.s3, runningTotal: 6 },
      ]);
    });

    it("allows window options for avg", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { date: 1 },
            output: {
              movingAvg: {
                $avg: "$price",
                window: { documents: [-2, 2] },
              },
            },
          })),
          $sort({
            category: 1,
            date: 1,
          }),
          $project($ => ({
            _id: 1,
            movingAvg: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({ _id: S.instanceOf(ObjectId), movingAvg: S.Union(S.Number, S.Null) }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s4, movingAvg: 25 },
        { _id: salesIds.s5, movingAvg: 25 },
        { _id: salesIds.s6, movingAvg: 25 },
        { _id: salesIds.s1, movingAvg: 150 },
        { _id: salesIds.s2, movingAvg: 150 },
        { _id: salesIds.s3, movingAvg: 150 },
      ]);
    });

    it("allows window options for min/max", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { date: 1 },
            output: {
              rollingMin: {
                $min: "$price",
                window: {
                  range: ["unbounded", 0],
                  unit: "day",
                },
              },
              rollingMax: {
                $max: "$price",
                window: {
                  range: [0, "unbounded"],
                  unit: "day",
                },
              },
            },
          })),
          $sort({
            category: 1,
            date: 1,
          }),
          $project($ => ({
            _id: 1,
            rollingMin: 1,
            rollingMax: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({
          _id: S.instanceOf(ObjectId),
          rollingMin: S.NullOr(S.Number),
          rollingMax: S.NullOr(S.Number),
        }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s4, rollingMin: 20, rollingMax: 30 },
        { _id: salesIds.s5, rollingMin: 20, rollingMax: 30 },
        { _id: salesIds.s6, rollingMin: 20, rollingMax: 25 },
        { _id: salesIds.s1, rollingMin: 100, rollingMax: 200 },
        { _id: salesIds.s2, rollingMin: 100, rollingMax: 200 },
        { _id: salesIds.s3, rollingMin: 100, rollingMax: 150 },
      ]);
    });

    it("enforces sortBy for rank-based operators", async () => {
      const res = await dbRegistry(db)
        .sales.aggregate(
          $setWindowFields($ => ({
            partitionBy: "$category",
            sortBy: { price: -1 },
            output: {
              rank: $.rank(),
              denseRank: $.denseRank(),
              docNum: $.documentNumber(),
            },
          })),
          $sort({
            category: 1,
            price: -1,
          }),
          $project($ => ({
            _id: 1,
            category: 1,
            price: 1,
            rank: 1,
            denseRank: 1,
            docNum: 1,
          })),
        )
        .toList();

      const expectedSchema = S.Array(
        S.Struct({
          _id: S.instanceOf(ObjectId),
          category: S.String,
          price: S.Number,
          rank: S.Number,
          denseRank: S.Number,
          docNum: S.Number,
        }),
      );
      assertSync(expectedSchema, res);

      expect(res).toEqual([
        { _id: salesIds.s5, category: "books", price: 30, rank: 1, denseRank: 1, docNum: 1 },
        { _id: salesIds.s6, category: "books", price: 25, rank: 2, denseRank: 2, docNum: 2 },
        { _id: salesIds.s4, category: "books", price: 20, rank: 3, denseRank: 3, docNum: 3 },
        { _id: salesIds.s2, category: "electronics", price: 200, rank: 1, denseRank: 1, docNum: 1 },
        { _id: salesIds.s3, category: "electronics", price: 150, rank: 2, denseRank: 2, docNum: 2 },
        { _id: salesIds.s1, category: "electronics", price: 100, rank: 3, denseRank: 3, docNum: 3 },
      ]);
    });
  });
});
