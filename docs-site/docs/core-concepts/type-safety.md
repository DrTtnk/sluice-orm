---
sidebar_position: 2
---

# Type Safety & Inference

Sluice's type system catches errors **at compile time** that would otherwise surface as silent wrong results or runtime crashes in production. Every field reference, operator argument, and pipeline output is statically validated against your schema.

## How Types Flow Through Pipelines

Every pipeline stage is a function `Agg<TIn, A> → Agg<TIn, B>` — it takes a document shape and produces a new one. Sluice chains up to 15 stages, threading the output of each as the input to the next:

```typescript
import { Schema as S } from "@effect/schema";
import { registry, $match, $group, $addFields, $unwind, $sort } from "sluice-orm";

const OrderSchema = S.Struct({
  _id: S.String,
  userId: S.String,
  amount: S.Number,
  items: S.Array(S.Struct({
    name: S.String,
    price: S.Number,
    quantity: S.Number,
  })),
  status: S.Literal("pending", "paid", "shipped"),
});

const db = registry("8.0", { orders: OrderSchema });
const { orders } = db(client.db("shop"));

const result = await orders
  .aggregate(
    $match($ => ({ status: "paid" })),
    // Type: still the original Order shape

    $unwind("$items"),
    // Type: { ... items: { name: string; price: number; quantity: number } }
    //   ↑ items is now a single object, not an array

    $group($ => ({
      _id: "$items.name",
      revenue: $.sum($.multiply("$items.price", "$items.quantity")),
      count: $.sum(1),
    })),
    // Type: { _id: string; revenue: number; count: number }
    //   ↑ completely new shape — previous fields are gone

    $sort({ revenue: -1 }),
    // Type: unchanged — $sort doesn't change the document shape
  )
  .toList();

// result: { _id: string; revenue: number; count: number }[]
//   ↑ Fully inferred from the pipeline. No manual type annotation needed.
```

No generics were annotated. No `as` casts. The return type is **computed** from the pipeline definition.

## Dot-Notation Path Inference

Sluice infers all valid dot-notation paths from your schema, including nested fields and array traversal:

```typescript
const UserSchema = S.Struct({
  _id: S.String,
  name: S.String,
  profile: S.Struct({
    avatar: S.String,
    settings: S.Struct({
      theme: S.Literal("light", "dark"),
      notifications: S.Boolean,
    }),
  }),
  scores: S.Array(S.Number),
});

// Valid paths (autocomplete shows all of these):
// "name"                         → string
// "profile.avatar"               → string
// "profile.settings.theme"       → "light" | "dark"
// "profile.settings.notifications" → boolean
// "scores"                       → number[]
// "scores.0"                     → number
```

### Type-Filtered Paths

Operators only accept paths that resolve to the correct type. `$.avg` only accepts numeric paths:

```typescript
$group($ => ({
  _id: "$department",
  avgAge: $.avg("$age"),         // ✅ age is number
  // avgAge: $.avg("$name"),     // ❌ Compile error: name is string
})),
```

The same applies to string operators (`$.concat`, `$.toUpper`), date operators (`$.year`, `$.month`), and array operators (`$.size`, `$.arrayElemAt`).

### Positional Operators in Updates

Update operations support MongoDB's positional operators with full type safety:

```typescript
await orders.updateOne(
  () => ({ "items.name": "Widget" }),
  { $set: { "items.$.price": 29.99 } },
  // ✅ items.$.price resolves to number — 29.99 is valid
);
```

The paths `items.$`, `items.$[]`, and `items.$[elem]` all resolve through array elements correctly.

## Compile-Time Error Messages

When something is wrong, Sluice produces structured error types that appear directly in your IDE:

### Type Mismatch

```typescript
$addFields($ => ({
  doubled: $.multiply("$name", 2),
  //                  ~~~~~~
  // TypeError: $numeric expects number | numeric field ref, got string
}));
```

### Invalid Field Reference

```typescript
$match($ => ({ $nonExistent: 1 }));
//             ~~~~~~~~~~~~~
// Error: "$nonExistent" is not a valid path in the schema
```

### Bare Strings in Accumulators

A common MongoDB mistake — forgetting the `$` prefix:

```typescript
$group($ => ({
  _id: "$department",
  total: $.sum("score"),
  //          ~~~~~~~
  // OpaqueError: Bare string "score" is not valid in accumulator position
  //              — did you mean "$score"?
}));
```

### `any` Prevention

If a stage would produce `any` (e.g., from a loosely typed operation), Sluice catches it instead of letting it silently propagate:

```typescript
// If a stage produces `any`, you get:
// OpaqueError: "Unexpected 'any' type detected — check your stage inputs"
// instead of the pipeline silently becoming untyped from that point on
```

## Null Safety

Operators that can receive nullable inputs propagate nullability through the output type:

```typescript
const UserSchema = S.Struct({
  _id: S.String,
  name: S.String,
  score: S.optional(S.Number),   // number | undefined
});

$addFields($ => ({
  // $.ifNull narrows the type — removes null/undefined
  safeScore: $.ifNull("$score", 0),
  // Type: number  (not number | undefined)

  // $.cond preserves both branches
  label: $.cond($.gt("$score", 50), "high", "low"),
  // Type: "high" | "low"

  // $.concat produces template literal types
  greeting: $.concat("Hello, ", "$name"),
  // Type: `Hello, ${string}`
}));
```

## Context-Aware Variables

Array operators like `$map`, `$filter`, and `$reduce` introduce scoped variables with proper typing:

```typescript
$addFields($ => ({
  discountedPrices: $.map("$items", item =>
    $.multiply(item("$price"), 0.9)
    // ↑ item() resolves paths relative to the array element
    // item("$price") → number
    // item("$name") → string
  ),
  // Type: number[]

  expensiveItems: $.filter("$items", item =>
    $.gt(item("$price"), 100)
  ),
  // Type: { name: string; price: number; quantity: number }[]
}));
```

`$reduce` adds a `$$value` variable for the accumulator:

```typescript
$addFields($ => ({
  totalQuantity: $.reduce("$items", 0, ($value, $this) =>
    $.add($value, $this("$quantity"))
  ),
  // Type: number
}));
```

## What This Means in Practice

The type system catches the most common MongoDB aggregation mistakes:

| Common Bug | How Sluice Catches It |
|---|---|
| Typo in field name | `PathType<T>` — only valid paths autocomplete |
| Wrong operator argument type | `NumericFieldRef<T>` filters paths by resolved type |
| Forgot `$` prefix on field ref | `AccumulatorArgInput` rejects bare strings |
| Used wrong accumulator | `AccumulatorBuilder` vs `ExprBuilder` context |
| Pipeline produces wrong shape | Output type is computed, not annotated |
| `any` slips through | `CheckedResult<T>` catches and flags it |
| Null not handled | `NullishResult` propagates nullability honestly |

See the [Advanced Typings](/docs/advanced-typings) page for complex real-world patterns including `$facet`, `$lookup`, union types, and discriminated unions.
