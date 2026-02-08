/**
 * Complex Integration Tests - Stress testing the type system
 *
 * Tests multi-stage pipelines, nested lookups with graphLookup,
 * complex facets, and window functions with extreme type transformations
 */

import { Schema as S } from "@effect/schema";
import type { Db } from "mongodb";
import { expectType } from "tsd";

import {
  $addFields,
  $bucket,
  $facet,
  $graphLookup,
  $group,
  $limit,
  $lookup,
  $match,
  $project,
  $redact,
  $setWindowFields,
  $sort,
  $unwind,
  collection,
} from "../../../src/sluice.js";

// ============================================
// COMPLEX SCHEMA: E-commerce Domain
// ============================================

type User = {
  _id: string;
  email: string;
  name: string;
  tier: "free" | "premium" | "enterprise";
  signupDate: Date;
  referredBy?: string | undefined;
  preferences: {
    notifications: boolean;
    language: string;
    currency: "USD" | "EUR" | "GBP";
  };
};

type Product = {
  _id: string;
  name: string;
  sku: string;
  categoryId: string;
  price: number;
  cost: number;
  stock: number;
  tags: readonly string[];
  attributes: readonly { key: string; value: string }[];
  reviews: readonly {
    userId: string;
    rating: number;
    comment: string;
    date: Date;
  }[];
};

type Category = {
  _id: string;
  name: string;
  parentId: string | null;
  level: number;
  path: readonly string[];
};

type Order = {
  _id: string;
  userId: string;
  status: "pending" | "processing" | "shipped" | "delivered" | "cancelled";
  items: readonly {
    productId: string;
    quantity: number;
    unitPrice: number;
    discount: number;
  }[];
  shippingAddress: {
    street: string;
    city: string;
    country: string;
    postalCode: string;
  };
  createdAt: Date;
  updatedAt: Date;
};

type Inventory = {
  _id: string;
  productId: string;
  warehouseId: string;
  quantity: number;
  lastRestocked: Date;
};

type Warehouse = {
  _id: string;
  name: string;
  location: { type: "Point"; coordinates: readonly [number, number] };
  capacity: number;
};

const UserSchema = S.Struct({
  _id: S.String,
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

const ProductSchema = S.Struct({
  _id: S.String,
  name: S.String,
  sku: S.String,
  categoryId: S.String,
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

const CategorySchema = S.Struct({
  _id: S.String,
  name: S.String,
  parentId: S.NullOr(S.String),
  level: S.Number,
  path: S.Array(S.String),
});

const OrderSchema = S.Struct({
  _id: S.String,
  userId: S.String,
  status: S.Literal("pending", "processing", "shipped", "delivered", "cancelled"),
  items: S.Array(
    S.Struct({
      productId: S.String,
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

const InventorySchema = S.Struct({
  _id: S.String,
  productId: S.String,
  warehouseId: S.String,
  quantity: S.Number,
  lastRestocked: S.Date,
});

const WarehouseSchema = S.Struct({
  _id: S.String,
  name: S.String,
  location: S.Struct({
    type: S.Literal("Point"),
    coordinates: S.Tuple(S.Number, S.Number),
  }),
  capacity: S.Number,
});

// Collection refs
const mockDb = {} as Db;

const users = collection("users", UserSchema, mockDb.collection("users"));
const products = collection("products", ProductSchema, mockDb.collection("products"));
const categories = collection("categories", CategorySchema, mockDb.collection("categories"));
const orders = collection("orders", OrderSchema, mockDb.collection("orders"));
const inventory = collection("inventory", InventorySchema, mockDb.collection("inventory"));
const warehouses = collection("warehouses", WarehouseSchema, mockDb.collection("warehouses"));

// ============================================
// TEST 1: Deeply nested $lookup with $graphLookup
// ============================================

// Get orders with full product info including category hierarchy
const ordersWithHierarchy = orders
  .aggregate(
    $unwind("$items"),
    $lookup({
      from: products,
      localField: "items.productId",
      foreignField: "_id",
      as: "product",
      pipeline: $ =>
        $.pipe(
          $lookup({
            from: categories,
            localField: "categoryId",
            foreignField: "_id",
            as: "category",
            pipeline: $ =>
              $.pipe(
                $graphLookup({
                  from: categories,
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
type OrdersWithHierarchy = Awaited<typeof ordersWithHierarchy>;
// Verify the nested pipeline types propagate

// ============================================
// TEST 2: Complex $facet with multiple aggregation paths
// ============================================

const orderAnalytics = orders
  .aggregate(
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
        $sort({ netRevenue: -1 }),
      ),

      // Path 2: Top products by quantity
      topProducts: $.pipe(
        $unwind("$items"),
        $group($ => ({
          _id: "$items.productId",
          totalQuantity: $.sum("$items.quantity"),
          totalRevenue: $.sum($.multiply("$items.quantity", "$items.unitPrice")),
        })),
        $sort({ totalQuantity: -1 }),
        $limit(10),
        $lookup({
          from: products,
          localField: "_id",
          foreignField: "_id",
          as: "productInfo",
        }),
        $unwind("$productInfo"),
        $project($ => ({
          productName: "$productInfo.name",
          productSku: "$productInfo.sku",
          totalQuantity: 1,
          totalRevenue: 1,
        })),
      ),

      // Path 3: Geographic distribution
      geoDistribution: $.pipe(
        $group($ => ({
          _id: {
            country: "$shippingAddress.country",
            city: "$shippingAddress.city",
          },
          orderCount: $.sum(1),
          avgOrderValue: $.avg(
            $.reduce({
              input: "$items",
              initialValue: 0,
              in: $ => $.add("$$value", $.multiply("$$this.quantity", "$$this.unitPrice")),
            }),
          ),
        })),
        $sort({ orderCount: -1 }),
        $limit(20),
      ),

      // Path 4: Time series analysis
      timeSeries: $.pipe(
        $addFields($ => ({
          orderMonth: $.dateTrunc({
            date: "$createdAt",
            unit: "month",
          }),
        })),
        $group($ => ({
          _id: "$orderMonth",
          orderCount: $.sum(1),
          uniqueCustomers: $.addToSet("$userId"),
        })),
        $addFields($ => ({ customerCount: $.size("$uniqueCustomers") })),
        $project($ => ({
          _id: 1,
          orderCount: 1,
          customerCount: 1,
        })),
        $sort({ _id: 1 }),
      ),
    })),
  )
  .toList();
type OrderAnalytics = Awaited<typeof orderAnalytics>;
// Verify facet shape

// ============================================
// TEST 3: $setWindowFields with complex partitions
// ============================================

const productRankings = products
  .aggregate(
    $setWindowFields($ => ({
      partitionBy: "$categoryId",
      sortBy: { price: -1 },
      output: {
        priceRank: { $rank: {} },
        pricePercentile: { $percentRank: {} },
        categoryAvgPrice: {
          $avg: "$price",
          window: { documents: ["unbounded", "unbounded"] },
        },
      },
    })),
    $addFields($ => ({
      priceVsAvg: $.subtract("$price", "$categoryAvgPrice"),
      isAboveAverage: $.gt($.subtract("$price", "$categoryAvgPrice"), 0),
      priceTier: $.switch({
        branches: [
          {
            case: $.lte("$pricePercentile", 0.25),
            then: "budget",
          },
          {
            case: $.lte("$pricePercentile", 0.75),
            then: "mid-range",
          },
        ],
        default: "premium",
      }),
    })),
  )
  .toList();
type ProductRankings = Awaited<typeof productRankings>;
expectType<number>({} as ProductRankings[number]["priceRank"]);
expectType<number>({} as ProductRankings[number]["pricePercentile"]);
expectType<number>({} as ProductRankings[number]["categoryAvgPrice"]);
expectType<boolean>({} as ProductRankings[number]["isAboveAverage"]);
expectType<"budget" | "mid-range" | "premium">({} as ProductRankings[number]["priceTier"]);

// ============================================
// TEST 4: Multi-stage transformation with type evolution
// ============================================

const userEngagementReport = users
  .aggregate(
    // Stage 1: Join with orders
    $lookup({
      from: orders,
      localField: "_id",
      foreignField: "userId",
      as: "orders",
    }),

    // Stage 2: Calculate order statistics
    $addFields($ => ({
      orderCount: $.size("$orders"),
      totalSpent: $.sum(
        $.map({
          input: "$orders",
          in: $ =>
            $.sum(
              $.map({
                input: "$$this.items",
                in: $ => $.multiply("$$this.quantity", "$$this.unitPrice"),
              }),
            ),
        }),
      ),
      lastOrderDate: $.first(
        $.map({
          input: "$orders",
          in: "$$this.createdAt",
        }),
      ),
    })),

    // Stage 3: Add engagement metrics
    $addFields($ => ({
      avgOrderValue: $.cond({
        if: $.gt("$orderCount", 0),
        then: $.divide("$totalSpent", "$orderCount"),
        else: 0,
      }),
      daysSinceLastOrder: $.cond({
        if: $.ne("$lastOrderDate", null),
        then: $.dateDiff({
          startDate: "$lastOrderDate",
          endDate: "$$NOW",
          unit: "day",
        }),
        else: null,
      }),
    })),

    // Stage 4: Classify users
    $addFields($ => ({
      engagementLevel: $.switch({
        branches: [
          {
            case: $.and($.gte("$orderCount", 10), $.gte("$totalSpent", 1000)),
            then: "champion",
          },
          {
            case: $.and($.gte("$orderCount", 5), $.gte("$totalSpent", 500)),
            then: "loyal",
          },
          {
            case: $.gte("$orderCount", 1),
            then: "active",
          },
        ],
        default: "inactive",
      }),
      churnRisk: $.cond({
        if: $.or($.eq("$orderCount", 0), $.gt("$daysSinceLastOrder", 90)),
        then: "high",
        else: $.cond({
          if: $.gt("$daysSinceLastOrder", 30),
          then: "medium",
          else: "low",
        }),
      }),
    })),

    // Stage 5: Project final shape
    $project($ => ({
      _id: 1,
      email: 1,
      name: 1,
      tier: 1,
      orderCount: 1,
      totalSpent: 1,
      avgOrderValue: 1,
      daysSinceLastOrder: 1,
      engagementLevel: 1,
      churnRisk: 1,
    })),
  )
  .toList();
type UserEngagement = Awaited<typeof userEngagementReport>;
expectType<string>({} as UserEngagement[number]["_id"]);
expectType<string>({} as UserEngagement[number]["email"]);
expectType<string>({} as UserEngagement[number]["name"]);
expectType<"free" | "premium" | "enterprise">({} as UserEngagement[number]["tier"]);
expectType<number>({} as UserEngagement[number]["orderCount"]);
expectType<number>({} as UserEngagement[number]["totalSpent"]);
expectType<number>({} as UserEngagement[number]["avgOrderValue"]);
expectType<"champion" | "loyal" | "active" | "inactive">(
  {} as UserEngagement[number]["engagementLevel"],
);
expectType<"high" | "medium" | "low">({} as UserEngagement[number]["churnRisk"]);

// ============================================
// TEST 5: Recursive data with $graphLookup
// ============================================

const categoryTree = categories
  .aggregate(
    $graphLookup({
      from: categories,
      startWith: "$_id",
      connectFromField: "_id",
      connectToField: "parentId",
      as: "descendants",
      maxDepth: 5,
      depthField: "depth",
    }),
    $addFields($ => ({
      descendantCount: $.size("$descendants"),
      maxDepth: $.max(
        $.map({
          input: "$descendants",
          in: "$$this.depth",
        }),
      ),
      descendantNames: $.map({
        input: "$descendants",
        in: "$$this.name",
      }),
    })),
    $match($ => ({
      parentId: null, // Root categories only
    })),
  )
  .toList();
type CategoryTree = Awaited<typeof categoryTree>;
// With $graphLookup + depthField, descendants now correctly have the full Category type + depth field
type DescendantType = CategoryTree[number]["descendants"][number];
expectType<string>({} as DescendantType["_id"]);
expectType<string>({} as DescendantType["name"]);
expectType<string | null>({} as DescendantType["parentId"]);
expectType<number>({} as DescendantType["level"]);
expectType<string[]>({} as DescendantType["path"]);
expectType<number>({} as DescendantType["depth"]);
expectType<number>({} as CategoryTree[number]["descendantCount"]);
// maxDepth result from $.max over array is number | null (empty array case)
expectType<number[]>({} as CategoryTree[number]["maxDepth"]);

// ============================================
// TEST 6: Complex $bucket with accumulator expressions (NOW WITH CALLBACK SUPPORT!)
// ============================================

const priceBuckets = products
  .aggregate(
    $bucket({
      groupBy: "$price",
      boundaries: [0, 25, 50, 100, 250, 500, 1000],
      default: "luxury",
      // Full accumulator power via callback!
      output: $ => ({
        count: $.sum(1),
        products: $.push("$name"),
        avgCost: $.avg("$cost"),
        totalStock: $.sum("$stock"),
        profitMargin: $.avg($.divide($.subtract("$price", "$cost"), "$price")),
        minPrice: $.min("$price"),
        maxPrice: $.max("$price"),
        allTags: $.push("$tags"),
      }),
    }),
  )
  .toList();
type PriceBuckets = Awaited<typeof priceBuckets>;
// Full type inference with callback support!
expectType<0 | 25 | 50 | 100 | 250 | 500 | 1000 | "luxury">({} as PriceBuckets[number]["_id"]);
expectType<number>({} as PriceBuckets[number]["count"]);
expectType<string[]>({} as PriceBuckets[number]["products"]);
expectType<number>({} as PriceBuckets[number]["avgCost"]);
expectType<number>({} as PriceBuckets[number]["totalStock"]);
expectType<number>({} as PriceBuckets[number]["profitMargin"]);
expectType<number>({} as PriceBuckets[number]["minPrice"]);
expectType<number>({} as PriceBuckets[number]["maxPrice"]);
expectType<string[][]>({} as PriceBuckets[number]["allTags"]);

// ============================================
// TEST 7: $redact with complex conditions
// ============================================

type SensitiveDoc = {
  _id: string;
  public: boolean;
  level: number;
  data: {
    public: boolean;
    info: string;
    nested: {
      public: boolean;
      secret: string;
    };
  };
  tags: readonly string[];
};

const SensitiveDocSchema = S.Struct({
  _id: S.String,
  public: S.Boolean,
  level: S.Number,
  data: S.Struct({
    public: S.Boolean,
    info: S.String,
    nested: S.Struct({
      public: S.Boolean,
      secret: S.String,
    }),
  }),
  tags: S.Array(S.String),
});

const sensitive = collection("sensitive", SensitiveDocSchema, mockDb.collection("sensitive"));

// Complex conditional redaction with callback support!
const redactedDocs = sensitive
  .aggregate(
    $redact($ =>
      $.cond({
        if: $.or($.eq("$public", true), $.lt("$level", 3)),
        then: "$$DESCEND",
        else: "$$PRUNE",
      }),
    ),
  )
  .toList();
type RedactedDocs = Awaited<typeof redactedDocs>;
// $redact preserves document type (fields may be pruned at runtime)

// ============================================
// TEST 8: Inventory check with multi-collection joins
// ============================================

const inventoryReport = products
  .aggregate(
    $lookup({
      from: inventory,
      localField: "_id",
      foreignField: "productId",
      as: "inventory",
      pipeline: $ =>
        $.pipe(
          $lookup({
            from: warehouses,
            localField: "warehouseId",
            foreignField: "_id",
            as: "warehouse",
          }),
          $unwind("$warehouse"),
          $project($ => ({
            quantity: 1,
            warehouseName: "$warehouse.name",
            warehouseCapacity: "$warehouse.capacity",
            lastRestocked: 1,
          })),
        ),
    }),
    $addFields($ => ({
      totalInventory: $.sum(
        $.map({
          input: "$inventory",
          in: "$$this.quantity",
        }),
      ),
      warehouseCount: $.size("$inventory"),
      needsRestock: $.lt(
        $.sum(
          $.map({
            input: "$inventory",
            in: "$$this.quantity",
          }),
        ),
        "$stock", // Compare to min stock level
      ),
    })),
    $match($ => ({ needsRestock: true })),
    $project($ => ({
      _id: 1,
      name: 1,
      sku: 1,
      totalInventory: 1,
      warehouseCount: 1,
      inventory: 1,
    })),
  )
  .toList();
type InventoryReport = Awaited<typeof inventoryReport>;
expectType<string>({} as InventoryReport[number]["name"]);
expectType<string>({} as InventoryReport[number]["sku"]);
expectType<number>({} as InventoryReport[number]["totalInventory"]);
expectType<number>({} as InventoryReport[number]["warehouseCount"]);

// ============================================
// TEST 9: User referral chain with $graphLookup
// ============================================

const referralChain = users
  .aggregate(
    $match($ => ({ referredBy: { $exists: true } })),
    $graphLookup({
      from: users,
      startWith: "$referredBy",
      connectFromField: "referredBy",
      connectToField: "_id",
      as: "referralChain",
      maxDepth: 10,
      depthField: "level",
    }),
    $addFields($ => ({
      chainLength: $.size("$referralChain"),
      referrerNames: $.map({
        input: "$referralChain",
        in: "$$this.name", // Now properly infers string type from $$this path!
      }),
      originalReferrer: $.arrayElemAt(
        $.filter({
          input: "$referralChain",
          cond: $ => $.not($.ifNull("$$this.referredBy", false)),
        }),
        0,
      ),
    })),
  )
  .toList();
type ReferralChain = Awaited<typeof referralChain>;
// With $graphLookup + depthField, referralChain has the full User type + level field
type ChainMember = ReferralChain[number]["referralChain"][number];
expectType<string>({} as ChainMember["_id"]);
expectType<string>({} as ChainMember["email"]);
expectType<string>({} as ChainMember["name"]);
expectType<"free" | "premium" | "enterprise">({} as ChainMember["tier"]);
expectType<Date>({} as ChainMember["signupDate"]);
expectType<number>({} as ChainMember["level"]);
expectType<number>({} as ReferralChain[number]["chainLength"]);
expectType<string[]>({} as ReferralChain[number]["referrerNames"]);
// originalReferrer is also a User & { level: number }
type OriginalReferrer = ReferralChain[number]["originalReferrer"];
expectType<string>({} as OriginalReferrer["_id"]);
expectType<string>({} as OriginalReferrer["name"]);
expectType<number>({} as OriginalReferrer["level"]);

// ============================================
// TEST 10: End-to-end order fulfillment pipeline
// ============================================

const fulfillmentPipeline = orders
  .aggregate(
    // Filter pending orders
    $match($ => ({ status: "pending" })),

    // Explode items
    $unwind("$items"),

    // Join product details
    $lookup({
      from: products,
      localField: "items.productId",
      foreignField: "_id",
      as: "product",
    }),
    $unwind("$product"),

    // Join inventory
    $lookup({
      from: inventory,
      localField: "items.productId",
      foreignField: "productId",
      as: "inventoryRecords",
    }),

    // Calculate fulfillment status
    $addFields($ => ({
      availableStock: $.sum(
        $.map({
          input: "$inventoryRecords",
          in: "$$this.quantity",
        }),
      ),
      canFulfill: $.gte(
        $.sum(
          $.map({
            input: "$inventoryRecords",
            in: "$$this.quantity",
          }),
        ),
        "$items.quantity",
      ),
      lineTotal: $.multiply("$items.quantity", "$items.unitPrice"),
    })),

    // Group back to order level
    $group($ => ({
      _id: "$_id",
      userId: $.first("$userId"),
      shippingAddress: $.first("$shippingAddress"),
      createdAt: $.first("$createdAt"),
      // $.push now supports object literals with full type inference!
      items: $.push({
        productId: "$items.productId",
        productName: "$product.name",
        quantity: "$items.quantity",
        unitPrice: "$items.unitPrice",
        lineTotal: "$lineTotal",
        canFulfill: "$canFulfill",
        availableStock: "$availableStock",
      }),
      totalValue: $.sum("$lineTotal"),
      allFulfillable: $.min(
        $.cond({
          if: "$canFulfill",
          then: 1,
          else: 0,
        }),
      ),
    })),

    // Add fulfillment summary
    $addFields($ => ({
      canShip: $.eq("$allFulfillable", 1),
      itemCount: $.size("$items"),
    })),

    // Sort by creation date
    $sort({ createdAt: 1 }),

    // Limit for processing batch
    $limit(100),
  )
  .toList();
type Fulfillment = Awaited<typeof fulfillmentPipeline>;
expectType<string>({} as Fulfillment[number]["_id"]);
expectType<string>({} as Fulfillment[number]["userId"]);
expectType<Order["shippingAddress"]>({} as Fulfillment[number]["shippingAddress"]);
expectType<number>({} as Fulfillment[number]["totalValue"]);
expectType<boolean>({} as Fulfillment[number]["canShip"]);
expectType<number>({} as Fulfillment[number]["itemCount"]);
// Full type inference for pushed objects!
type FulfillmentItem = Fulfillment[number]["items"][number];
expectType<string>({} as FulfillmentItem["productId"]);
expectType<string>({} as FulfillmentItem["productName"]);
expectType<number>({} as FulfillmentItem["quantity"]);
expectType<number>({} as FulfillmentItem["unitPrice"]);
expectType<number>({} as FulfillmentItem["lineTotal"]);
expectType<boolean>({} as FulfillmentItem["canFulfill"]);
expectType<number>({} as FulfillmentItem["availableStock"]);
