// ==========================================
// Examples
// ==========================================

import { Schema as S } from "@effect/schema";
import {
  $facet,
  $graphLookup,
  $group,
  $limit,
  $lookup,
  $match,
  $project,
  $sort,
  $unwind,
  registry,
} from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import {
  CategorySchema as CategoryCollectionSchema,
  OrderSchema as OrderCollectionSchema,
  ProductSchema as ProductCollectionSchema,
  UserSchema as UserCollectionSchema,
} from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

type UserCollection = typeof UserCollectionSchema.Type;
type OrderCollection = typeof OrderCollectionSchema.Type;
type ProductCollection = typeof ProductCollectionSchema.Type;
type CategoryCollection = typeof CategoryCollectionSchema.Type;

const dbRegistry = registry("8.0", {
  users: UserCollectionSchema,
  orders: OrderCollectionSchema,
  products: ProductCollectionSchema,
  categories: CategoryCollectionSchema,
});

// ==========================================
// COMPLEX AGGREGATION EXAMPLE
// ==========================================

// ==========================================
// RUNTIME TESTS
// ==========================================

const AggResultSchema = S.Struct({
  _id: S.Boolean,
  count: S.Number,
  avgAge: S.NullOr(S.Number),
});

describe("Sluice Runtime Integration", () => {
  let db: Db;

  const dbRegistry = registry("8.0", {
    users: UserCollectionSchema,
    orders: OrderCollectionSchema,
    products: ProductCollectionSchema,
    categories: CategoryCollectionSchema,
  });

  beforeAll(async () => {
    // Setup container
    const res = await setup();
    db = res.db;

    const boundRegistry = dbRegistry(db);

    // Insert mock data matching UserCollection structure but simpler for this test
    // clear first just in case
    await boundRegistry.users.deleteMany(() => ({})).execute();

    await boundRegistry.users
      .insertMany([
        {
          _id: new ObjectId("507f1f77bcf86cd799439011"),
          name: "Alice",
          age: 30,
          scores: [85, 90],
          tags: ["premium", "active"],
          active: true,
          addresses: [
            {
              city: "New York",
              zip: 10001,
            },
            {
              city: "Los Angeles",
              zip: 90001,
            },
          ],
        },
        {
          _id: new ObjectId("507f1f77bcf86cd799439012"),
          name: "Bob",
          age: 25,
          scores: [75, 80],
          tags: ["trial"],
          active: true,
          addresses: [
            {
              city: "Chicago",
              zip: 60601,
            },
          ],
        },
        {
          _id: new ObjectId("507f1f77bcf86cd799439013"),
          name: "Charlie",
          age: 35,
          scores: [95],
          tags: ["inactive"],
          active: false,
          addresses: [],
        },
        {
          _id: new ObjectId("507f1f77bcf86cd799439014"),
          name: "David",
          age: 40,
          scores: [88, 92, 85],
          tags: ["vip", "active"],
          active: true,
          addresses: [
            {
              city: "New York",
              zip: 10002,
            },
          ],
        },
      ])
      .execute();

    // Insert categories
    await boundRegistry.categories
      .insertMany([
        {
          _id: new ObjectId("507f1f77bcf86cd799439021"),
          name: "Electronics",
          level: 0,
          path: ["Electronics"],
        },
        {
          _id: new ObjectId("507f1f77bcf86cd799439022"),
          name: "Computers",
          parentId: new ObjectId("507f1f77bcf86cd799439021"),
          level: 1,
          path: ["Electronics", "Computers"],
        },
        {
          _id: new ObjectId("507f1f77bcf86cd799439023"),
          name: "Laptops",
          parentId: new ObjectId("507f1f77bcf86cd799439022"),
          level: 2,
          path: ["Electronics", "Computers", "Laptops"],
        },
      ])
      .execute();

    // Insert products
    await boundRegistry.products
      .insertMany([
        {
          _id: new ObjectId("507f1f77bcf86cd799439031"),
          name: "MacBook Pro",
          sku: "MBP",
          categoryId: new ObjectId("507f1f77bcf86cd799439023"),
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
              userId: new ObjectId("507f1f77bcf86cd799439011"),
              rating: 5,
              comment: "Great!",
              date: new Date(),
            },
          ],
        },
        {
          _id: new ObjectId("507f1f77bcf86cd799439032"),
          name: "Dell XPS",
          sku: "DXPS",
          categoryId: new ObjectId("507f1f77bcf86cd799439023"),
          price: 1500,
          cost: 1200,
          stock: 5,
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

    // Insert orders
    await boundRegistry.orders
      .insertMany([
        {
          _id: new ObjectId("507f1f77bcf86cd799439041"),
          userId: new ObjectId("507f1f77bcf86cd799439011"),
          items: [
            {
              productId: new ObjectId("507f1f77bcf86cd799439031"),
              quantity: 1,
              price: 2000,
            },
          ],
          orderDate: new Date("2023-01-01"),
          status: "completed",
        },
        {
          _id: new ObjectId("507f1f77bcf86cd799439042"),
          userId: new ObjectId("507f1f77bcf86cd799439012"),
          items: [
            {
              productId: new ObjectId("507f1f77bcf86cd799439032"),
              quantity: 2,
              price: 1500,
            },
          ],
          orderDate: new Date("2023-02-01"),
          status: "pending",
        },
      ])
      .execute();
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should execute a pipeline and validate results", async () => {
    // We define a transient schema for this test
    const TestUserSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      age: S.Number,
      role: S.Literal("admin", "user"),
      active: S.Boolean,
    });

    // Create pipeline
    const results = await dbRegistry(db)
      .users.aggregate(
        $match($ => ({ active: true })),
        $group($ => ({
          _id: "$active",
          count: $.sum(1),
          avgAge: $.avg("$age"),
        })),
        $sort({ count: -1 }),
      )
      .toList();

    // Validate runtime values

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(AggResultSchema), results);
    expectType<typeof AggResultSchema.Type>({} as (typeof results)[number]);
    expect(results).toEqual([
      {
        _id: true,
        count: 3,
        avgAge: (30 + 25 + 40) / 3,
      }, // Alice, Bob, David are active
    ]);
  });

  it("should execute complex aggregation pipeline", async () => {
    const boundRegistry = dbRegistry(db);
    const results = await boundRegistry.users
      .aggregate(
        // Initial match to filter users - builder function provides $
        $match($ => ({
          // Using builder methods for expressions
          $expr: $.and(
            $.gt(
              "$age",
              $.subtract(
                21,
                $.mod(
                  $.size("$tags"),
                  $.add(
                    3,
                    $.cond({
                      if: $.gte($.avg("$scores"), 50),
                      then: 1,
                      else: $.multiply(
                        $.size(
                          $.filter({
                            input: "$addresses",
                            cond: $ => $.eq("$$this.city", "New York"),
                          }),
                        ),
                        2,
                      ),
                    }),
                  ),
                ),
              ),
            ),
            $.lt(
              "$age",
              $.add(
                40,
                $.multiply(
                  $.avg("$scores"),
                  $.divide(
                    $.size("$addresses"),
                    $.max(
                      1,
                      $.reduce({
                        input: "$tags",
                        initialValue: 0,
                        in: $ =>
                          $.add(
                            "$$value",
                            $.cond({
                              if: $.regexMatch({
                                input: "$$this",
                                regex: "vip",
                              }),
                              then: 5,
                              else: 1,
                            }),
                          ),
                      }),
                    ),
                  ),
                ),
              ),
            ),
            $.cond({
              if: $.gte(
                $.size("$addresses"),
                $.switch({
                  branches: [
                    {
                      case: $.eq($.arrayElemAt("$scores", -1), 100),
                      then: 3,
                    },
                    {
                      case: $.gt($.sum("$scores"), 1000),
                      then: 2,
                    },
                  ],
                  default: 1,
                }),
              ),
              then: $.switch({
                branches: [
                  {
                    case: $.and($.eq($.arrayElemAt("$scores", 0), 100), $.in("gold", "$tags")),
                    then: $.gte("$age", 25),
                  },
                  {
                    case: $.gt($.sum("$scores"), 500),
                    then: $.cond({
                      if: $.gte($.size("$tags"), 5),
                      then: $.lt("$age", 35),
                      else: $.gte("$age", 30),
                    }),
                  },
                ],
                default: $.or(
                  $.eq("$age", 30),
                  $.and($.gte("$age", 18), $.lt("$age", $.add(20, $.size("$addresses")))),
                ),
              }),
              else: $.cond({
                if: $.or(
                  $.in("premium", "$tags"),
                  $.regexMatch({
                    input: "$name",
                    regex: "^A",
                  }),
                ),
                then: $.gte(
                  "$age",
                  $.add(
                    22,
                    $.multiply(
                      $.size(
                        $.filter({
                          input: "$scores",
                          cond: $ => $.gte("$$this", 80),
                        }),
                      ),
                      0.5,
                    ),
                  ),
                ),
                else: $.lt(
                  "$age",
                  $.subtract(
                    28,
                    $.divide(
                      $.sum(
                        $.map({
                          input: "$addresses",
                          in: "$$this.zip",
                        }),
                      ),
                      $.max($.size("$addresses"), 1),
                    ),
                  ),
                ),
              }),
            }),
          ),
          "addresses.city": { $in: ["New York", "Los Angeles", "Chicago"] },
          name: {
            $regex: "^[A-Z][a-z]+$",
            $options: "i",
          },
          "scores.0": {
            $gte: 50,
            $lte: 100,
          },
          tags: {
            $all: ["active"],
            $nin: ["banned", "suspended"],
          },
          "addresses.zip": {
            $gte: 10000,
            $lt: 99999,
          },
        })),
        // Lookup orders for each user
        $lookup({
          from: boundRegistry.orders,
          localField: "_id",
          foreignField: "userId",
          as: "userOrders",
          pipeline: $ =>
            $.pipe(
              // Sub-pipeline for orders
              $match($ => ({
                status: { $in: ["completed", "shipped"] },
                orderDate: { $gte: new Date("2020-01-01") },
                "items.quantity": { $gte: 1 },
                "items.price": { $gt: 0 },
                $expr: $.and(
                  $.gte($.sum("$items.quantity"), 1),
                  $.lte($.size("$items"), 10),
                  $.gt($.multiply($.sum("$items.quantity"), $.avg("$items.price")), 50),
                ),
              })),
              // Lookup products for each order item
              $lookup({
                from: boundRegistry.products,
                localField: "items.productId",
                foreignField: "_id",
                as: "productDetails",
                pipeline: $ =>
                  $.pipe(
                    $lookup({
                      from: boundRegistry.categories,
                      localField: "categoryId",
                      foreignField: "_id",
                      as: "category",
                      pipeline: $ =>
                        $.pipe(
                          // Graph lookup for hierarchical categories
                          $graphLookup({
                            from: boundRegistry.categories,
                            startWith: "$parentId",
                            connectFromField: "parentId",
                            connectToField: "_id",
                            as: "ancestors",
                            maxDepth: 5,
                            depthField: "depth",
                          }),
                        ),
                    }),
                    // Unwind category
                    $unwind({
                      path: "$category",
                      preserveNullAndEmptyArrays: true,
                    }),
                    // Project product with category hierarchy
                    $project($ => ({
                      name: 1,
                      price: 1,
                      stock: 1,
                      categoryName: "$category.name",
                      categoryAncestors: "$category.ancestors.name",
                    })),
                  ),
              }),
              // Unwind items to process each item
              $unwind("$items"),
              // Lookup product details for the item
              $lookup({
                from: boundRegistry.products,
                localField: "items.productId",
                foreignField: "_id",
                as: "itemProduct",
                pipeline: $ =>
                  $.pipe(
                    $lookup({
                      from: boundRegistry.categories,
                      localField: "categoryId",
                      foreignField: "_id",
                      as: "category",
                    }),
                    $unwind({
                      path: "$category",
                      preserveNullAndEmptyArrays: true,
                    }),
                    $project($ => ({
                      name: 1,
                      categoryName: "$category.name",
                    })),
                  ),
              }),
              $unwind("$itemProduct"),
              // Group back to order level with aggregated item data
              $group($ => ({
                _id: "$_id",
                userId: $.first("$userId"),
                orderDate: $.first("$orderDate"),
                status: $.first("$status"),
                totalValue: $.sum($.multiply("$items.quantity", "$items.price")),
                itemCount: $.sum("$items.quantity"),
                products: $.push("$itemProduct.name"),
                categories: $.addToSet("$itemProduct.categoryName"),
              })),
            ),
        }),
        // Unwind userOrders to process each order
        $unwind({
          path: "$userOrders",
          preserveNullAndEmptyArrays: true,
        }),
        // Note: After $group, userOrders has complex nested structure
        // localField typing has limitation with deeply nested paths after pipeline transformations
        $lookup({
          from: boundRegistry.products,
          localField: "userOrders",
          foreignField: "name",
          as: "productReviews",
          pipeline: $ =>
            $.pipe(
              $unwind("$reviews"),
              $group($ => ({
                _id: "$_id",
                name: $.first("$name"),
                avgRating: $.avg("$reviews.rating"),
                reviewCount: $.sum(1),
                topComments: $.push("$reviews.comment"),
              })),
              $sort({ avgRating: -1 }),
              $limit(10),
            ),
        }),
        // Facet for multiple analyses
        $facet($ => ({
          userStats: $.pipe(
            $group($ => ({
              _id: "$_id",
              name: $.first("$name"),
              age: $.first("$age"),
              totalOrders: $.sum(1),
              totalSpent: $.sum("$userOrders.totalValue"),
              avgOrderValue: $.avg("$userOrders.totalValue"),
              favoriteCategories: $.push("$userOrders.categories"),
            })),
            $project($ => ({
              name: 1,
              age: 1,
              totalOrders: 1,
              totalSpent: 1,
              avgOrderValue: 1,
              favoriteCategories: $.reduce({
                input: "$favoriteCategories",
                initialValue: [],
                in: $ => $.setUnion("$$value", "$$this"),
              }),
            })),
          ),
          orderAnalysis: $.pipe(
            $group($ => ({
              _id: "$userOrders._id",
              userName: $.first("$name"),
              orderDate: $.first("$userOrders.orderDate"),
              totalValue: $.first("$userOrders.totalValue"),
              itemCount: $.first("$userOrders.itemCount"),
              products: $.first("$userOrders.products"),
            })),
            $sort({ totalValue: -1 }),
            $limit(100),
          ),
          productInsights: $.pipe(
            $unwind("$productReviews"),
            $group($ => ({
              _id: "$productReviews._id",
              name: $.first("$productReviews.name"),
              avgRating: $.first("$productReviews.avgRating"),
              reviewCount: $.first("$productReviews.reviewCount"),
              associatedUsers: $.addToSet("$name"),
            })),
            $match($ => ({
              $expr: $.and(
                $.gte($.ifNull("$reviewCount", 0), 5),
                $.gte($.ifNull("$avgRating", 0), 4.0),
                $.lte($.ifNull("$reviewCount", 0), 100),
                $.gt($.ifNull("$avgRating", 0), 3.5),
                $.gte($.size("$associatedUsers"), 1),
              ),
            })),
          ),
        })),
        // Final project to structure the output
        $project($ => ({
          userStatistics: "$userStats",
          topOrders: "$orderAnalysis",
          recommendedProducts: "$productInsights",
        })),
      )
      .toList();

    // Check structure
    const ResultSchema = S.Struct({
      userStatistics: S.Array(
        S.Struct({
          _id: S.instanceOf(ObjectId),
          name: S.NullOr(S.String),
          age: S.NullOr(S.Number),
          totalOrders: S.Number,
          totalSpent: S.Number,
          avgOrderValue: S.NullOr(S.Number),
          favoriteCategories: S.Array(S.String),
        }),
      ),
      topOrders: S.Array(
        S.Struct({
          _id: S.instanceOf(ObjectId),
          userName: S.NullOr(S.String),
          orderDate: S.optional(S.Date),
          totalValue: S.NullOr(S.Number),
          itemCount: S.NullOr(S.Number),
          products: S.NullOr(S.Array(S.String)),
        }),
      ),
      recommendedProducts: S.Array(
        S.Struct({
          _id: S.instanceOf(ObjectId),
          name: S.NullOr(S.String),
          avgRating: S.NullOr(S.Number),
          reviewCount: S.NullOr(S.Number),
          associatedUsers: S.Array(S.String),
        }),
      ),
    });

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
    // For more specific checks, we can add based on the data
    // For example, check if Alice has stats
    const firstResult = results[0];

    if (!firstResult) {
      throw new Error("Expected at least one result");
    }

    const aliceStats = firstResult.userStatistics.find(u => u.name === "Alice");

    if (!aliceStats) {
      throw new Error("Alice stats not found");
    }

    expect(aliceStats.totalOrders).toBe(1);
    expect(aliceStats.totalSpent).toBe(2000);
  });
});
