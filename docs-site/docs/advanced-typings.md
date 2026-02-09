---
sidebar_position: 10
---

# Advanced Type Inference

Sluice's killer feature is **end-to-end type inference through aggregation pipelines**. Every stage computes a precise output type that becomes the next stage's input — no manual generics, no casts, no `as unknown as MyType`.

This page showcases the type system in action with real-world patterns.

## Pipeline Type Flow

The core idea: each stage transforms the document shape, and Sluice infers every intermediate type automatically.

```typescript
import { Schema as S } from "@effect/schema";
import { registry, $match, $group, $project, $sort, $addFields, $unwind } from "sluice-orm";

const OrderSchema = S.Struct({
  _id: S.String,
  userId: S.String,
  amount: S.Number,
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

const db = registry("8.0", { orders: OrderSchema });
const { orders } = db(client.db("shop"));

const categoryReport = await orders
  .aggregate(
    // Stage 1: Filter — output type stays the same
    $match($ => ({ status: "paid" })),
    // Type: { _id: string; userId: string; amount: number;
    //   items: { productId: string; name: string; price: number; quantity: number; category: string }[];
    //   status: "pending" | "paid" | "shipped" | "delivered"; createdAt: Date }

    // Stage 2: Unwind items array — each doc now has a single item
    $unwind("$items"),
    // Type: { _id: string; userId: string; amount: number;
    //   items: { productId: string; name: string; price: number; quantity: number; category: string };
    //   status: "pending" | "paid" | "shipped" | "delivered"; createdAt: Date }
    //   ↑ items is now a single object, not an array!

    // Stage 3: Group by category — completely new shape
    $group($ => ({
      _id: "$items.category",
      revenue: $.sum($.multiply("$items.price", "$items.quantity")),
      unitsSold: $.sum("$items.quantity"),
      orderCount: $.sum(1),
    })),
    // Type: { _id: string; revenue: number; unitsSold: number; orderCount: number }

    // Stage 4: Add computed fields
    $addFields($ => ({
      avgRevenuePerOrder: $.divide("$revenue", "$orderCount"),
    })),
    // Type: { _id: string; revenue: number; unitsSold: number; orderCount: number;
    //   avgRevenuePerOrder: number }

    // Stage 5: Sort — type unchanged
    $sort({ revenue: -1 }),
    // Type: { _id: string; revenue: number; unitsSold: number; orderCount: number;
    //   avgRevenuePerOrder: number }
  )
  .toList();

// categoryReport: {
//   _id: string;
//   revenue: number;
//   unitsSold: number;
//   orderCount: number;
//   avgRevenuePerOrder: number;
// }[]
```

**What just happened:** We went from `OrderSchema` to a completely different shape through 5 stages, and TypeScript knows the exact type at every step. No generics were harmed.

## Type-Aware Autocomplete

When writing expressions, the `$` builder constrains field references by type. This means your editor only shows **fields of the correct type** in autocomplete:

- `$.multiply("$` → only shows numeric fields (`$amount`, `$items.price`, `$items.quantity`, ...)
- `$.concat("$` → only shows string fields (`$userId`, `$items.name`, `$items.category`, ...)
- `$.gte("$` → only shows numeric/date fields
- `$.filter({ input: "$` → only shows array fields (`$items`, ...)

This is powered by filtered path types — `NumericFieldRef<T>`, `StringFieldRef<T>`, `ArrayFieldRef<T>`, etc. — that walk your schema's dot-paths and keep only those whose resolved type matches the operator's constraint.

```typescript
$project($ => ({
  // ✅ TypeScript allows — "$amount" resolves to number
  doubled: $.multiply("$amount", 2),

  // ❌ TypeScript error — "$userId" resolves to string, not number
  bad: $.multiply("$userId", 2),

  // ✅ TypeScript allows — "$userId" resolves to string
  greeting: $.concat("Hello, ", "$userId"),

  // ❌ TypeScript error — "$amount" resolves to number, not string
  bad2: $.concat("$amount", " dollars"),
}))
```

## Type Narrowing Tricks

Sluice's type system goes beyond basic inference — it performs **type narrowing** that removes nullability and produces precise literal types.

### `$.ifNull` — Removing Nullability

When a field is `T | null`, `$.ifNull` narrows the result to just `T` by providing a fallback:

```typescript
const MonsterSchema = S.Struct({
  _id: S.String,
  name: S.String,
  legacyScore: S.NullOr(S.Number), // number | null
  deletedAt: S.NullOr(S.Date),     // Date | null
});

$addFields($ => ({
  // legacyScore is number | null → after ifNull, it's number
  safeScore: $.ifNull("$legacyScore", 0),
  // Type: number

  // Can chain multiple fallbacks — returns the first non-null
  safeName: $.ifNull("$nickname", "$name", "Anonymous"),
  // Type: string
}))
```

Under the hood, `$.ifNull` uses `FirstNotNil<T>` — a recursive type that walks the argument tuple and returns the first type that isn't `null | undefined`.

### `$.switch` — Union of Literal Types

`$.switch` infers a precise **union of literal types** from all `then` values and the `default`:

```typescript
$project($ => ({
  ageBand: $.switch({
    branches: [
      { case: $.lt("$age", 18), then: "minor" },
      { case: $.lt("$age", 65), then: "adult" },
    ],
    default: "senior",
  }),
  // Type: "minor" | "adult" | "senior"
  //   ↑ Not string — a precise union of the exact literals you wrote!

  priceRange: $.switch({
    branches: [
      { case: $.lte("$price", 10), then: 1 },
      { case: $.lte("$price", 50), then: 2 },
      { case: $.lte("$price", 100), then: 3 },
    ],
    default: 4,
  }),
  // Type: 1 | 2 | 3 | 4
}))
```

This works because branches are captured with `const Br`, preserving literal types in `then` values. The result type is `Br[number]["then"] | D` — a distributed union over all branches.

### `$.cond` — Narrowed Branch Types

`$.cond` similarly narrows to the union of `then` and `else`:

```typescript
$addFields($ => ({
  tier: $.cond({
    if: $.gte("$totalSpent", 1000),
    then: "premium",
    else: "standard",
  }),
  // Type: "premium" | "standard"

  // Nested cond chains keep precise types
  bracket: $.cond({
    if: $.lt("$score", 30),
    then: "low",
    else: $.cond({
      if: $.lt("$score", 70),
      then: "mid",
      else: "high",
    }),
  }),
  // Type: "low" | "mid" | "high"
}))
```

### `$.concat` — Template Literal Types

`$.concat` produces **template literal types** when mixing literals with field references:

```typescript
$project($ => ({
  // All literals → exact template literal
  label: $.concat("Hello, ", "World"),
  // Type: "Hello, World"

  // Field ref (string) + literal → template literal with string
  greeting: $.concat("Hello, ", "$name"),
  // Type: `Hello, ${string}`

  // Multiple segments
  tag: $.concat("user_", "$userId", "_v2"),
  // Type: `user_${string}_v2`
}))
```

Under the hood, `ConcatStrings<T>` recursively builds `` `${First}${ConcatStrings<Rest>}` ``, so any literal segment stays literal and any `string` distributes via TypeScript's template literal mechanics.

### `$.mergeObjects` — Null-Free Accumulation

`$.mergeObjects` uses `MergeTwo<A, B>` which **skips null and undefined entirely**:

```typescript
// MergeTwo<A, B>:
//   A extends null | undefined → B       (null is skipped)
//   B extends null | undefined → A       (null is skipped)
//   otherwise → MergeWithIndexOverride<A, B>  (last-wins merge)

$addFields($ => ({
  merged: $.mergeObjects(
    "$metadata",              // { version: string; counts: { views: number } }
    { extra: "value" as const },
  ),
  // Type: { version: string; counts: { views: number }; extra: "value" }
  //   ↑ No null contamination from the accumulator chain

  // Overlapping keys: last wins
  overridden: $.mergeObjects(
    { a: 1, b: "old" },
    { b: "new", c: true },
  ),
  // Type: { a: number; b: string; c: boolean }
  //   ↑ b is string (from last object), not number | string
}))
```

## Array Transformations

Sluice types `$map`, `$filter`, and `$reduce` with full inference of the iteration variables (`$$this`, `$$value`, custom `as` names).

### $map — Transform Array Elements

```typescript
$project($ => ({
  // Map items to just their names
  itemNames: $.map({
    input: "$items",
    as: "item",
    in: "$$item.name",
  }),
  // Type: string[]

  // Map with expression
  itemTotals: $.map({
    input: "$items",
    as: "item",
    in: $ => $.multiply("$$item.price", "$$item.quantity"),
  }),
  // Type: number[]
}))
```

### $filter — Type-Safe Array Filtering

```typescript
$project($ => ({
  // Filter keeps the element type — only the cond callback is required
  expensiveItems: $.filter({
    input: "$items",
    as: "item",
    cond: $ => $.gte("$$item.price", 100),
  }),
  // Type: { productId: string; name: string; price: number; quantity: number; category: string }[]

  highScores: $.filter({
    input: "$scores",
    cond: $ => $.gte("$$this", 80),
  }),
  // Type: number[]
}))
```

:::info
`$filter` and `$reduce` require **callback syntax** for their `cond`/`in` arguments: `$ => expr`. Using a bare expression is a compile error (`CallbackOnlyError`).
:::

### $reduce — Fold Arrays with Type Tracking

```typescript
$project($ => ({
  // Sum all quantities — $$value is number, $$this is the element type
  totalQuantity: $.reduce({
    input: "$items",
    initialValue: 0,
    in: $ => $.add("$$value", "$$this.quantity"),
  }),
  // Type: number

  // Collect rarities into an array
  allRarities: $.reduce({
    input: $.map({ input: "$items", as: "item", in: "$$item.rarity" }),
    initialValue: [] as string[],
    in: $ => $.concatArrays("$$value", ["$$this"]),
  }),
  // Type: string[]
}))
```

### $sortArray — Sort with Type Preservation

```typescript
$project($ => ({
  // Sort primitives ascending/descending
  sortedScores: $.sortArray({ input: "$scores", sortBy: 1 }),
  // Type: number[]

  sortedDesc: $.sortArray({ input: "$scores", sortBy: -1 }),
  // Type: number[]

  // Sort objects by field
  sortedGrades: $.sortArray({ input: "$grades", sortBy: { score: -1 } }),
  // Type: { subject: string; score: number; date: Date }[]

  // Multi-field sort
  sortedItems: $.sortArray({ input: "$items", sortBy: { price: -1, quantity: 1 } }),
  // Type: { name: string; price: number; quantity: number }[]

  // Composition: sort after selecting top N
  top3Sorted: $.sortArray({
    input: $.maxN({ input: "$scores", n: 3 }),
    sortBy: -1,
  }),
  // Type: number[]
}))
```

## Multi-Branch Analysis with $facet

`$facet` runs multiple sub-pipelines on the same input. Each branch is built with `$.pipe(...)` and independently typed.

```typescript
const analysis = await monsters
  .aggregate(
    $facet($ => ({
      // Branch 1: Group by level bracket
      byLevel: $.pipe(
        $addFields($ => ({
          bracket: $.cond({
            if: $.lt("$level", 10),
            then: "low",
            else: $.cond({
              if: $.lt("$level", 30),
              then: "mid",
              else: "high",
            }),
          }),
        })),
        // Intermediate: { ...; bracket: "low" | "mid" | "high" }
        $group($ => ({
          _id: "$bracket",
          count: $.sum(1),
          avgAttack: $.avg("$attack"),
        })),
      ),
      // Type: { _id: "low" | "mid" | "high"; count: number; avgAttack: number | null }[]

      // Branch 2: Tag frequency
      byTags: $.pipe(
        $unwind("$tags"),
        $sortByCount("$tags"),
      ),
      // Type: { _id: string; count: number }[]

      // Branch 3: Global stats
      stats: $.pipe(
        $group($ => ({
          _id: null,
          totalMonsters: $.sum(1),
          avgLevel: $.avg("$level"),
          maxHp: $.max("$hp"),
        })),
      ),
      // Type: { _id: null; totalMonsters: number; avgLevel: number | null; maxHp: number }[]
    })),
  )
  .toList();

// analysis[0].byLevel  → { _id: "low" | "mid" | "high"; count: number; avgAttack: number | null }[]
// analysis[0].byTags   → { _id: string; count: number }[]
// analysis[0].stats    → { _id: null; totalMonsters: number; avgLevel: number | null; maxHp: number }[]
```

## Accumulator Expressions in $group

The `$` builder in `$group` unlocks accumulator operators alongside all expression operators.

```typescript
$group($ => ({
  _id: "$department",

  // Basic accumulators
  headcount: $.sum(1),                           // Type: number
  avgSalary: $.avg("$salary"),                   // Type: number | null
  maxSalary: $.max("$salary"),                   // Type: number
  minSalary: $.min("$salary"),                   // Type: number

  // First/last (order-dependent)
  newestHire: $.last("$name"),                   // Type: string
  oldestHire: $.first("$name"),                  // Type: string

  // Collect values
  allNames: $.push("$name"),                     // Type: string[]
  uniqueRoles: $.addToSet("$role"),              // Type: string[]

  // Push complex objects — result type is inferred from the object literal
  employees: $.push({
    name: "$name",
    salary: "$salary",
    level: "$level",
  }),
  // Type: { name: string; salary: number; level: number }[]

  // Computed accumulators
  totalCompensation: $.sum($.add("$salary", $.ifNull("$bonus", 0))),
  // Type: number
}))

// Full group result type:
// {
//   _id: string;
//   headcount: number;
//   avgSalary: number | null;
//   maxSalary: number;
//   minSalary: number;
//   newestHire: string;
//   oldestHire: string;
//   allNames: string[];
//   uniqueRoles: string[];
//   employees: { name: string; salary: number; level: number }[];
//   totalCompensation: number;
// }
```

:::note
`$.avg()` returns `number | null` because an empty group has no average. `$.sum()` returns `number` because an empty group sums to `0`.
:::

## Type-Safe $lookup with Sub-Pipelines

`$lookup` with a pipeline sub-query carries full type safety. The `from` field takes a **typed collection reference** from your bound registry — not a raw string.

```typescript
const dbRegistry = registry("8.0", {
  users: UserSchema,
  orders: OrderSchema,
});
const boundRegistry = dbRegistry(client.db("myapp"));

const usersWithOrders = await boundRegistry.users
  .aggregate(
    $lookup({
      from: boundRegistry.orders,   // ← typed collection reference, not "orders"
      localField: "_id",
      foreignField: "userId",
      as: "recentOrders",
      pipeline: $ => $.pipe(
        $match($ => ({
          status: { $in: ["paid", "shipped"] },
        })),
        $sort({ createdAt: -1 }),
        $limit(5),
      ),
    }),
    // Type: User & { recentOrders: Order[] }
  )
  .toList();

// usersWithOrders: (User & { recentOrders: Order[] })[]
```

The sub-pipeline inside `$lookup` is independently typed against the `from` collection's schema. The result adds the `as` field with the correct element type.

## Deeply Nested Schema Inference

Sluice handles arbitrarily deep schemas with nullables, optionals, arrays of structs, and literal types — all resolved through dot-path access.

```typescript
const MonsterSchema = S.Struct({
  _id: S.instanceOf(ObjectId),
  name: S.String,
  score: S.Number,
  active: S.Boolean,
  deletedAt: S.NullOr(S.Date),
  legacyScore: S.NullOr(S.Number),
  status: S.Literal("draft", "published", "archived"),
  metadata: S.Struct({
    version: S.String,
    counts: S.Struct({
      views: S.Number,
      likes: S.Number,
    }),
  }),
  tags: S.Array(S.String),
  items: S.Array(S.Struct({
    name: S.String,
    price: S.Number,
    discounts: S.Array(S.Struct({
      code: S.String,
      percent: S.Number,
    })),
  })),
});

$project($ => ({
  version: "$metadata.version",              // Type: string
  viewCount: "$metadata.counts.views",       // Type: number
  firstTag: $.arrayElemAt("$tags", 0),       // Type: string
  safeScore: $.ifNull("$legacyScore", 0),    // Type: number (narrowed from number | null)
  itemNames: $.map({
    input: "$items",
    as: "item",
    in: "$$item.name",                       // Type: string — type-safe even inside map
  }),
  // Type: string[]
}))
```

## $project: Inclusion, Exclusion, and Reshaping

`$project` supports inclusion mode (`1` / `$.include`), exclusion mode (`0` / `$.exclude`), and expression-based field definitions — with the output type precisely reflecting the projection spec.

```typescript
$project($ => ({
  name: 1,            // or $.include   → keeps string
  age: 1,             // or $.include   → keeps number

  _id: 0,             // or $.exclude   → removed from output

  userName: "$name",                         // Type: string (renamed)
  ageInMonths: $.multiply("$age", 12),       // Type: number
  isAdult: $.gte("$age", 18),               // Type: boolean
  greeting: $.concat("Hello, ", "$name"),    // Type: `Hello, ${string}`

  tagCount: $.size("$tags"),                 // Type: number
  topScore: $.max("$scores"),               // Type: number
  filteredScores: $.filter({
    input: "$scores",
    cond: $ => $.gte("$$this", 80),
  }),                                        // Type: number[]
}))
// Result type: {
//   name: string;
//   age: number;
//   userName: string;
//   ageInMonths: number;
//   isAdult: boolean;
//   greeting: `Hello, ${string}`;
//   tagCount: number;
//   topScore: number;
//   filteredScores: number[];
// }
```

## Union Types in Schemas

Sluice supports discriminated unions in schemas. After `$match` on the discriminant field, the full union type remains — use conditional expressions for safe variant access.

```typescript
const EventSchema = S.Struct({
  _id: S.String,
  timestamp: S.Date,
  payload: S.Union(
    S.Struct({ type: S.Literal("click"), elementId: S.String }),
    S.Struct({ type: S.Literal("purchase"), amount: S.Number }),
    S.Struct({ type: S.Literal("pageview"), url: S.String }),
  ),
});

const db = registry("8.0", { events: EventSchema });
const { events } = db(client.db("analytics"));

const summary = await events
  .aggregate(
    $group($ => ({
      _id: "$payload.type",
      // Type: "click" | "purchase" | "pageview"
      count: $.sum(1),
      // Type: number
      totalRevenue: $.sum(
        $.cond({
          if: $.eq("$payload.type", "purchase"),
          then: "$payload.amount",
          else: 0,
        }),
      ),
      // Type: number
    })),
    $sort({ count: -1 }),
  )
  .toList();

// summary: {
//   _id: "click" | "purchase" | "pageview";
//   count: number;
//   totalRevenue: number;
// }[]
```

## Real-World: E-Commerce Analytics Pipeline

Putting it all together — a complex 7-stage pipeline with full type safety:

```typescript
const topSpenderReport = await orders
  .aggregate(
    $match($ => ({
      status: "paid",
      createdAt: { $gte: new Date("2025-01-01") },
    })),
    // Type: Order (filtered)

    $unwind("$items"),
    // Type: Order with items: single Item

    $group($ => ({
      _id: { userId: "$userId", category: "$items.category" },
      spent: $.sum($.multiply("$items.price", "$items.quantity")),
      itemCount: $.sum("$items.quantity"),
    })),
    // Type: { _id: { userId: string; category: string }; spent: number; itemCount: number }

    $group($ => ({
      _id: "$_id.userId",
      categories: $.push({
        category: "$_id.category",
        spent: "$spent",
        itemCount: "$itemCount",
      }),
      totalSpent: $.sum("$spent"),
    })),
    // Type: { _id: string; categories: { category: string; spent: number; itemCount: number }[];
    //   totalSpent: number }

    $addFields($ => ({
      topCategory: $.arrayElemAt(
        $.sortArray({ input: "$categories", sortBy: { spent: -1 } }),
        0,
      ),
    })),
    // Type: { ...; topCategory: { category: string; spent: number; itemCount: number } }

    $project($ => ({
      userId: "$_id",
      totalSpent: 1,
      topCategory: "$topCategory.category",
      topCategorySpent: "$topCategory.spent",
      categoryCount: $.size("$categories"),
      _id: 0,
    })),
    // Type: { userId: string; totalSpent: number; topCategory: string;
    //   topCategorySpent: number; categoryCount: number }

    $sort({ totalSpent: -1 }),
  )
  .toList();

// topSpenderReport: {
//   userId: string;
//   totalSpent: number;
//   topCategory: string;
//   topCategorySpent: number;
//   categoryCount: number;
// }[]
```

## Type Safety Guarantees

| What | How |
|---|---|
| **Field references** | `"$fieldName"` is validated against the current document shape |
| **Dot-path access** | `"$nested.deep.field"` resolves to the correct type |
| **Operator arguments** | `$.multiply` only accepts numeric fields, `$.concat` only strings |
| **Autocomplete** | Each operator narrows `$field` suggestions to the correct type |
| **Stage output** | Each stage's output type is computed and fed to the next |
| **Accumulator context** | `$.push` and `$.addToSet` are only available inside `$group` |
| **Nullable narrowing** | `$.ifNull` resolves `T \| null` to `T` with a fallback |
| **Literal preservation** | `$.switch` and `$.cond` preserve literal types in branches |
| **Template literals** | `$.concat` produces precise template literal types |
| **Merge null removal** | `$.mergeObjects` skips null/undefined in the merge chain |
| **Array iteration** | `$$this`, `$$value`, and `$$<as>` variables are typed inside `$map`/`$filter`/`$reduce` |
