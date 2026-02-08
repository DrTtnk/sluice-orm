// Runtime equivalent of complexIntegration.test-d.ts
import { Schema as S } from "@effect/schema";
import {
  $addFields,
  $count,
  $facet,
  $graphLookup,
  $group,
  $lookup,
  $project,
  $unwind,
  registry,
} from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

// ============================================
// COMPLEX SCHEMA: E-commerce Domain
// ============================================

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  email: S.String,
  name: S.String,
  tier: S.Literal("free", "premium", "enterprise"),
  signupDate: S.Date,
  referredBy: S.optional(S.String),
  preferences: S.Struct({
    notifications: S.Boolean,
    language: S.String,
    currency: S.Literal("USD", "EUR", "GBP"),
  }),
});
type User = typeof UserSchema.Type;

const ProductSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  sku: S.String,
  categoryId: ObjectIdSchema,
  price: S.Number,
  cost: S.Number,
  stock: S.Number,
  tags: S.Array(S.String),
  attributes: S.Array(
    S.Struct({
      key: S.String,
      value: S.String,
    }),
  ),
  reviews: S.Array(
    S.Struct({
      userId: S.String,
      rating: S.Number,
      comment: S.String,
      date: S.Date,
    }),
  ),
});
type Product = typeof ProductSchema.Type;

const CategorySchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  parentId: S.NullOr(ObjectIdSchema),
  level: S.Number,
  path: S.Array(S.String),
});
type Category = typeof CategorySchema.Type;

const OrderSchema = S.Struct({
  _id: ObjectIdSchema,
  userId: ObjectIdSchema,
  status: S.Literal("pending", "processing", "shipped", "delivered", "cancelled"),
  items: S.Array(
    S.Struct({
      productId: ObjectIdSchema,
      quantity: S.Number,
      unitPrice: S.Number,
      discount: S.Number,
    }),
  ),
  shippingAddress: S.Struct({
    street: S.String,
    city: S.String,
    country: S.String,
    postalCode: S.String,
  }),
  createdAt: S.Date,
  updatedAt: S.Date,
});
type Order = typeof OrderSchema.Type;

const InventorySchema = S.Struct({
  _id: ObjectIdSchema,
  productId: ObjectIdSchema,
  warehouseId: S.String,
  quantity: S.Number,
  lastRestocked: S.Date,
});
type Inventory = typeof InventorySchema.Type;

const WarehouseSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  location: S.Struct({
    type: S.Literal("Point"),
    coordinates: S.Tuple(S.Number, S.Number),
  }),
  capacity: S.Number,
});
type Warehouse = typeof WarehouseSchema.Type;

// Collection refs using schemas
const myRegistry = registry("8.0", {
  users: UserSchema,
  products: ProductSchema,
  categories: CategorySchema,
  orders: OrderSchema,
  inventory: InventorySchema,
  warehouses: WarehouseSchema,
});

describe("Complex Integration Runtime", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;
    const boundRegistry = myRegistry(db);
    // Seed Users
    await boundRegistry.users
      .insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          email: "alice@example.com",
          name: "Alice",
          tier: "premium",
          signupDate: new Date("2023-01-01"),
          preferences: {
            notifications: true,
            language: "en",
            currency: "USD",
          },
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          email: "bob@example.com",
          name: "Bob",
          tier: "free",
          signupDate: new Date("2023-02-01"),
          preferences: {
            notifications: false,
            language: "en",
            currency: "GBP",
          },
        },
      ])
      .execute();

    // Seed Categories
    await boundRegistry.categories
      .insertMany([
        {
          _id: new ObjectId("000000000000000000000003"),
          name: "Electronics",
          parentId: null,
          level: 0,
          path: ["Electronics"],
        },
        {
          _id: new ObjectId("000000000000000000000004"),
          name: "Computers",
          parentId: new ObjectId("000000000000000000000003"),
          level: 1,
          path: ["Electronics", "Computers"],
        },
        {
          _id: new ObjectId("000000000000000000000005"),
          name: "Laptops",
          parentId: new ObjectId("000000000000000000000004"),
          level: 2,
          path: ["Electronics", "Computers", "Laptops"],
        },
      ])
      .execute();

    // Seed Products
    await boundRegistry.products
      .insertMany([
        {
          _id: new ObjectId("000000000000000000000006"),
          name: "MacBook Pro",
          sku: "MBP1",
          categoryId: new ObjectId("000000000000000000000005"),
          price: 2000,
          cost: 1500,
          stock: 10,
          tags: ["apple", "laptop"],
          attributes: [
            {
              key: "color",
              value: "silver",
            },
          ],
          reviews: [
            {
              userId: new ObjectId("000000000000000000000001").toString(),
              rating: 5,
              comment: "Great",
              date: new Date(),
            },
            {
              userId: new ObjectId("000000000000000000000002").toString(),
              rating: 4,
              comment: "Solid",
              date: new Date(),
            },
          ],
        },
        {
          _id: new ObjectId("000000000000000000000007"),
          name: "Dell XPS",
          sku: "DXPS",
          categoryId: new ObjectId("000000000000000000000005"),
          price: 1500,
          cost: 1200,
          stock: 20,
          tags: ["dell", "laptop"],
          attributes: [
            {
              key: "color",
              value: "black",
            },
          ],
          reviews: [],
        },
      ])
      .execute();

    await boundRegistry.orders
      .insertMany([
        {
          _id: new ObjectId("000000000000000000000008"),
          userId: new ObjectId("000000000000000000000001"),
          status: "delivered",
          items: [
            {
              productId: new ObjectId("000000000000000000000006"),
              quantity: 1,
              unitPrice: 2000,
              discount: 0,
            },
          ],
          shippingAddress: {
            street: "123 Main",
            city: "NYC",
            country: "USA",
            postalCode: "10001",
          },
          createdAt: new Date("2023-03-01"),
          updatedAt: new Date("2023-03-02"),
        },
        {
          _id: new ObjectId("000000000000000000000009"),
          userId: new ObjectId("000000000000000000000002"),
          status: "pending",
          items: [
            {
              productId: new ObjectId("000000000000000000000007"),
              quantity: 2,
              unitPrice: 1500,
              discount: 100,
            },
          ],
          shippingAddress: {
            street: "456 Elm",
            city: "London",
            country: "UK",
            postalCode: "SW1",
          },
          createdAt: new Date("2023-03-05"),
          updatedAt: new Date("2023-03-05"),
        },
      ])
      .execute();
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("ordersWithHierarchy: Deep nested lookup and graphLookup", async () => {
    const results = await myRegistry(db)
      .orders.aggregate(
        $unwind("$items"),
        $lookup({
          from: myRegistry(db).products,
          localField: "items.productId",
          foreignField: "_id",
          as: "product",
          pipeline: $ =>
            $.pipe(
              $lookup({
                from: myRegistry(db).categories,
                localField: "categoryId",
                foreignField: "_id",
                as: "category",
                pipeline: $ =>
                  $.pipe(
                    $graphLookup({
                      from: myRegistry(db).categories,
                      startWith: "$parentId",
                      connectFromField: "parentId",
                      connectToField: "_id",
                      as: "ancestors",
                      maxDepth: 10,
                      depthField: "depth",
                    }),
                  ),
              }),
              $unwind({
                path: "$category",
                preserveNullAndEmptyArrays: true,
              }),
              $project($ => ({
                name: 1,
                price: 1,
                categoryName: "$category.name",
                categoryPath: "$category.ancestors.name",
              })),
            ),
        }),
        $unwind("$product"),
      )
      .toList();

    // Assertions
    expect(results).toHaveLength(2);

    const p1 = results.find(
      r => r._id.toString() === new ObjectId("000000000000000000000008").toString(),
    );
    expect(p1?.product.name).toBe("MacBook Pro");
    expect(p1?.product.categoryName).toBe("Laptops");
    // Ancestors of Laptops(c3) are Computers(c2) and Electronics(c1).
    // graphLookup returns unordered usually?
    // ancestors.name will be ["Computers", "Electronics"] (order not guaranteed by graphLookup without sort)

    // Type Validation
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      userId: ObjectIdSchema,
      status: S.Literal("pending", "processing", "shipped", "delivered", "cancelled"),
      items: S.Struct({
        productId: ObjectIdSchema,
        quantity: S.Number,
        unitPrice: S.Number,
        discount: S.Number,
      }),
      shippingAddress: S.Struct({
        street: S.String,
        city: S.String,
        country: S.String,
        postalCode: S.String,
      }),
      createdAt: S.Date,
      updatedAt: S.Date,
      product: S.Struct({
        _id: ObjectIdSchema,
        name: S.String,
        price: S.Number,
        categoryName: S.String,
        categoryPath: S.Array(S.String),
      }),
    });

    assertSync(S.Array(ResultSchema), results);
  });

  it("orderAnalytics: Facet with multiple paths", async () => {
    const results = await myRegistry(db)
      .orders.aggregate(
        $facet($ => ({
          // Path 1: Revenue by status
          revenueByStatus: $.pipe(
            $unwind("$items"),
            $group($ => ({
              _id: "$status",
              totalRevenue: $.sum($.multiply("$items.quantity", "$items.unitPrice")),
              totalDiscount: $.sum($.multiply("$items.quantity", "$items.discount")),
              orderCount: $.sum(1),
            })),
            $addFields($ => ({
              netRevenue: $.subtract("$totalRevenue", "$totalDiscount"),
              avgOrderValue: $.divide("$totalRevenue", "$orderCount"),
            })),
          ),
          // Path 2: Just count
          count: $.pipe($count("total")),
        })),
      )
      .toList();

    expect(results).toHaveLength(1);
    const facet = results[0];
    if (!facet) {
      throw new Error("Expected facet result");
    }

    // o1: delivered, 1 * 2000 = 2000 rev, 0 disc.
    // o2: pending, 2 * 1500 = 3000 rev, 2 * 100 = 200 disc? items price is unitPrice.
    // items: [{ ... quantity: 2, unitPrice: 1500, discount: 100 }]
    // 2*1500 = 3000. 2*100 = 200. Net 2800.

    const delivered = facet.revenueByStatus.find(r => r._id === "delivered");
    const pending = facet.revenueByStatus.find(r => r._id === "pending");

    expect(delivered).toEqual({
      _id: "delivered",
      totalRevenue: 2000,
      totalDiscount: 0,
      orderCount: 1,
      netRevenue: 2000,
      avgOrderValue: 2000,
    });

    expect(pending).toEqual({
      _id: "pending",
      totalRevenue: 3000,
      totalDiscount: 200,
      orderCount: 1,
      netRevenue: 2800,
      avgOrderValue: 3000,
    });

    expect(facet.count).toEqual([{ total: 2 }]);

    // Type Validation
    const ResultSchema = S.Struct({
      revenueByStatus: S.Array(
        S.Struct({
          _id: S.String,
          totalRevenue: S.Number,
          totalDiscount: S.Number,
          orderCount: S.Number,
          netRevenue: S.Number,
          avgOrderValue: S.NullOr(S.Number),
        }),
      ),
      count: S.Array(S.Struct({ total: S.Number })),
    });

    assertSync(S.Array(ResultSchema), results);
  });
});
