---
sidebar_position: 12
---

# Examples

Real-world examples demonstrating Sluice's capabilities with full type inference.

## E-commerce Analytics

```typescript
import { Schema as S } from "@effect/schema";
import { registry, $match, $group, $sort, $project, $unwind, $lookup,
         $addFields, $facet, $count, $limit, $sortByCount } from "sluice-orm";

const UserSchema = S.Struct({
  _id: S.String,
  name: S.String,
  email: S.String,
  registrationDate: S.Date,
  country: S.String,
});

const OrderSchema = S.Struct({
  _id: S.String,
  userId: S.String,
  amount: S.Number,
  currency: S.String,
  items: S.Array(S.Struct({
    productId: S.String,
    name: S.String,
    price: S.Number,
    quantity: S.Number,
    category: S.String,
  })),
  status: S.Literal("pending", "paid", "shipped", "delivered"),
  createdAt: S.Date,
});

const ProductSchema = S.Struct({
  _id: S.String,
  name: S.String,
  category: S.String,
  price: S.Number,
  stock: S.Number,
  tags: S.Array(S.String),
});

const db = registry("8.0", {
  users: UserSchema,
  orders: OrderSchema,
  products: ProductSchema,
});

const boundDb = db(client.db("ecommerce"));
const { users, orders, products } = boundDb;
```

### Top-Selling Products by Revenue

```typescript
const topProducts = await orders
  .aggregate(
    $match($ => ({ status: "paid" })),
    $unwind("$items"),
    $group($ => ({
      _id: "$items.productId",
      productName: $.first("$items.name"),           // Type: string
      totalRevenue: $.sum($.multiply("$items.price", "$items.quantity")),
                                                     // Type: number
      totalSold: $.sum("$items.quantity"),            // Type: number
      orderCount: $.sum(1),                          // Type: number
    })),
    $sort({ totalRevenue: -1 }),
    $limit(10),
  )
  .toList();

// topProducts: {
//   _id: string;
//   productName: string;
//   totalRevenue: number;
//   totalSold: number;
//   orderCount: number;
// }[]
```

### Customer Lifetime Value

```typescript
const customerValue = await orders
  .aggregate(
    $match($ => ({ status: "paid" })),
    $group($ => ({
      _id: "$userId",
      totalSpent: $.sum("$amount"),                  // Type: number
      orderCount: $.sum(1),                          // Type: number
      avgOrderValue: $.avg("$amount"),               // Type: number | null
      firstOrder: $.min("$createdAt"),               // Type: Date
      lastOrder: $.max("$createdAt"),                // Type: Date
    })),
    // Type: { _id: string; totalSpent: number; orderCount: number;
    //   avgOrderValue: number | null; firstOrder: Date; lastOrder: Date }

    $lookup({
      from: boundDb.users,    // ← typed collection reference
      localField: "_id",
      foreignField: "_id",
      as: "userInfo",
    }),
    // Type: { ... & { userInfo: User[] } }

    $unwind("$userInfo"),
    // Type: { ... & { userInfo: User } }

    $project($ => ({
      customerId: "$_id",
      customerName: "$userInfo.name",
      totalSpent: $.include,
      orderCount: $.include,
      avgOrderValue: $.include,
      customerSince: "$firstOrder",
      lastOrder: $.include,
      _id: $.exclude,
    })),
    $sort({ totalSpent: -1 }),
  )
  .toList();

// customerValue: {
//   customerId: string;
//   customerName: string;
//   totalSpent: number;
//   orderCount: number;
//   avgOrderValue: number | null;
//   customerSince: Date;
//   lastOrder: Date;
// }[]
```

### Product Category Performance

```typescript
const categoryPerformance = await orders
  .aggregate(
    $match($ => ({ status: "paid" })),
    $unwind("$items"),
    $group($ => ({
      _id: "$items.category",
      revenue: $.sum($.multiply("$items.price", "$items.quantity")),
      unitsSold: $.sum("$items.quantity"),
      uniqueProducts: $.addToSet("$items.productId"),
      orderCount: $.sum(1),
    })),
    // Type: { _id: string; revenue: number; unitsSold: number;
    //   uniqueProducts: string[]; orderCount: number }

    $project($ => ({
      category: "$_id",
      revenue: $.include,
      unitsSold: $.include,
      productCount: $.size("$uniqueProducts"),       // Type: number
      orderCount: $.include,
      avgOrderValue: $.divide("$revenue", "$orderCount"),
      _id: $.exclude,
    })),
    $sort({ revenue: -1 }),
  )
  .toList();

// categoryPerformance: {
//   category: string;
//   revenue: number;
//   unitsSold: number;
//   productCount: number;
//   orderCount: number;
//   avgOrderValue: number;
// }[]
```

## Content Management System

```typescript
const ArticleSchema = S.Struct({
  _id: S.String,
  title: S.String,
  content: S.String,
  authorId: S.String,
  tags: S.Array(S.String),
  published: S.Boolean,
  publishedAt: S.optional(S.Date),
  views: S.Number,
  likes: S.Number,
});

const CommentSchema = S.Struct({
  _id: S.String,
  articleId: S.String,
  authorId: S.String,
  content: S.String,
  createdAt: S.Date,
  likes: S.Number,
});

const cmsDb = registry("8.0", {
  articles: ArticleSchema,
  comments: CommentSchema,
});

const boundCms = cmsDb(client.db("cms"));
const { articles, comments } = boundCms;
```

### Popular Articles with Comment Counts

```typescript
const popularArticles = await articles
  .aggregate(
    $match($ => ({ published: true })),
    $lookup({
      from: boundCms.comments,     // ← typed reference
      localField: "_id",
      foreignField: "articleId",
      as: "comments",
    }),
    $project($ => ({
      title: $.include,
      authorId: $.include,
      tags: $.include,
      publishedAt: $.include,
      views: $.include,
      likes: $.include,
      commentCount: $.size("$comments"),             // Type: number
      engagementScore: $.add(
        $.multiply("$views", 0.1),
        $.multiply("$likes", 2),
        $.multiply($.size("$comments"), 3),
      ),                                             // Type: number
    })),
    $sort({ engagementScore: -1 }),
    $limit(20),
  )
  .toList();
```

### Tag Cloud with Usage Counts

```typescript
const tagCloud = await articles
  .aggregate(
    $match($ => ({ published: true })),
    $unwind("$tags"),
    $group($ => ({
      _id: "$tags",
      articleCount: $.sum(1),
      totalViews: $.sum("$views"),
      totalLikes: $.sum("$likes"),
    })),
    $project($ => ({
      tag: "$_id",
      articleCount: $.include,
      totalViews: $.include,
      totalLikes: $.include,
      popularity: $.add(
        $.multiply("$articleCount", 10),
        $.multiply("$totalViews", 0.01),
        $.multiply("$totalLikes", 0.1),
      ),
      _id: $.exclude,
    })),
    $sort({ popularity: -1 }),
    $limit(50),
  )
  .toList();

// tagCloud: {
//   tag: string;
//   articleCount: number;
//   totalViews: number;
//   totalLikes: number;
//   popularity: number;
// }[]
```

## Social Media Analytics

```typescript
const PostSchema = S.Struct({
  _id: S.String,
  authorId: S.String,
  content: S.String,
  type: S.Literal("text", "image", "video"),
  likes: S.Number,
  shares: S.Number,
  comments: S.Number,
  createdAt: S.Date,
  tags: S.Array(S.String),
});

const socialDb = registry("8.0", { posts: PostSchema });
const { posts } = socialDb(client.db("social"));
```

### Viral Content Analysis

```typescript
const viralPosts = await posts
  .aggregate(
    $addFields($ => ({
      engagement: $.add("$likes", $.multiply("$shares", 2), $.multiply("$comments", 3)),
      // Type: number
      hoursOld: $.divide(
        $.subtract(new Date(), "$createdAt"),
        1000 * 60 * 60,
      ),
      // Type: number
    })),
    $addFields($ => ({
      engagementRate: $.divide("$engagement", $.add(1, "$hoursOld")),
      // Type: number
    })),
    $match($ => ({ engagement: { $gt: 100 } })),
    $sort({ engagementRate: -1 }),
    $limit(10),
  )
  .toList();
```

### Hashtag Performance

```typescript
const hashtagPerformance = await posts
  .aggregate(
    $unwind("$tags"),
    $group($ => ({
      _id: "$tags",
      postCount: $.sum(1),
      totalLikes: $.sum("$likes"),
      totalShares: $.sum("$shares"),
      totalComments: $.sum("$comments"),
      avgEngagement: $.avg(
        $.add("$likes", $.multiply("$shares", 2), $.multiply("$comments", 3)),
      ),
    })),
    // Type: { _id: string; postCount: number; totalLikes: number; totalShares: number;
    //   totalComments: number; avgEngagement: number | null }

    $match($ => ({ postCount: { $gte: 5 } })),
    $project($ => ({
      hashtag: "$_id",
      postCount: $.include,
      totalEngagement: $.add("$totalLikes", $.multiply("$totalShares", 2), $.multiply("$totalComments", 3)),
      avgEngagement: $.include,
      _id: $.exclude,
    })),
    $sort({ totalEngagement: -1 }),
    $limit(20),
  )
  .toList();

// hashtagPerformance: {
//   hashtag: string;
//   postCount: number;
//   totalEngagement: number;
//   avgEngagement: number | null;
//   _id is excluded
// }[]
```

### Multi-Metric Dashboard with $facet

```typescript
const dashboard = await posts
  .aggregate(
    $facet($ => ({
      byType: $.pipe(
        $group($ => ({
          _id: "$type",
          count: $.sum(1),
          avgLikes: $.avg("$likes"),
        })),
        $sort({ count: -1 }),
      ),
      // Type: { _id: "text" | "image" | "video"; count: number; avgLikes: number | null }[]

      topTags: $.pipe(
        $unwind("$tags"),
        $sortByCount("$tags"),
        $limit(10),
      ),
      // Type: { _id: string; count: number }[]

      stats: $.pipe(
        $group($ => ({
          _id: null,
          totalPosts: $.sum(1),
          avgLikes: $.avg("$likes"),
          maxShares: $.max("$shares"),
        })),
      ),
      // Type: { _id: null; totalPosts: number; avgLikes: number | null; maxShares: number }[]
    })),
  )
  .toOne();
```

These examples demonstrate Sluice's capabilities for complex, real-world pipelines with full type safety. Every intermediate and final type is inferred automatically — no manual generics needed.
