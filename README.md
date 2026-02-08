# Sluice ORM

**Type-safe MongoDB aggregation pipeline builder** where every stage's output type becomes the next stage's input — fully inferred, zero runtime overhead.

```typescript
const result = await users
  .aggregate(
    $match(() => ({ age: { $gte: 18 } })),
    $group($ => ({
      _id: "$department",
      avgAge: $.avg("$age"),
      headcount: $.sum(1),
    })),
    $sort({ headcount: -1 }),
  )
  .toList();
// result: { _id: string; avgAge: number; headcount: number }[]
```

**What makes it different:** every `$field` reference, every expression operator, every accumulator is validated against your document schema at compile time. `$.multiply("$name", 2)` won't compile — it knows `name` is a `string`.

---

## Getting Started

```bash
npm install @sluice/sluice mongodb
```

### Schema-agnostic — bring your own validation

Sluice works with **Effect Schema**, **Zod 4**, or plain type markers:

```typescript
// Effect Schema
import { Schema as S } from "effect/Schema";
const UserSchema = S.Struct({ _id: S.String, name: S.String, age: S.Number });

// Zod 4
import { z } from "zod";
const UserSchema = z.object({ _id: z.string(), name: z.string(), age: z.number() });

// Plain type marker (no runtime validation)
const UserSchema = { Type: null! as { _id: string; name: string; age: number } };
```

### Create a registry

```typescript
import { registry, $match, $group, $sort, $project } from "@sluice/sluice";

const db = registry("8.0", {
  users: UserSchema,
  orders: OrderSchema,
});

// Bind to a MongoDB connection
const { users, orders } = db(client.db("myapp"));
```

### Aggregate with full type inference

```typescript
// Each stage's output type flows into the next
const topSpenders = await orders
  .aggregate(
    $group($ => ({
      _id: "$customerId",
      totalSpent: $.sum("$amount"),
      orderCount: $.sum(1),
    })),
    $match(() => ({ totalSpent: { $gt: 1000 } })),
    $sort({ totalSpent: -1 }),
    $project($ => ({
      customerId: "$_id",
      totalSpent: $.include,
      orderCount: $.include,
      _id: $.exclude,
    })),
  )
  .toList();
// topSpenders: { customerId: string; totalSpent: number; orderCount: number }[]
```

### CRUD — type-safe find, update, insert, delete

```typescript
// Type-safe filter and projection
const adults = await users
  .find(() => ({ age: { $gte: 18 } }), { sort: { name: 1 }, limit: 10 })
  .toList();

// Type-safe updates with arrayFilters
await users.updateOne(
  () => ({ _id: "user-1" }),
  { $set: { name: "Alice" }, $inc: { age: 1 } },
);

// Type-safe bulk operations
await users.bulkWrite([
  { insertOne: { document: { _id: "u1", name: "Bob", age: 30 } } },
  { updateOne: { filter: { _id: "u2" }, update: { $inc: { age: 1 } } } },
]);
```

### Update pipelines

```typescript
// The $ callback provides typed update operators: $.set, $.unset, $.addFields, $.replaceRoot, $.replaceWith
await users.updateMany(
  () => ({}),
  $ => $.pipe(
    $.set($ => ({ fullName: $.concat("$firstName", " ", "$lastName") })),
    $.unset("firstName", "lastName"),
  ),
);
```

### Debug with `.toMQL()`

```typescript
const pipeline = users.aggregate(
  $match(() => ({ status: "active" })),
  $group($ => ({ _id: "$role", count: $.sum(1) })),
);

console.log(pipeline.toMQL());
// [
//   { "$match": { "status": "active" } },
//   { "$group": { "_id": "$role", "count": { "$sum": 1 } } }
// ]
```

### Expression operators

The `$` callback parameter exposes 100+ typed MongoDB expression operators:

```typescript
$project($ => ({
  // Arithmetic
  total: $.multiply("$price", "$quantity"),
  rounded: $.round($.divide("$total", 100), 2),

  // String
  fullName: $.concat("$first", " ", "$last"),
  slug: $.toLower("$title"),

  // Conditional
  tier: $.cond({ if: $.gte("$score", 90), then: "gold", else: "silver" }),
  label: $.switch({
    branches: [
      { case: $.eq("$status", "active"), then: "Active" },
      { case: $.eq("$status", "paused"), then: "On Hold" },
    ],
    default: "Unknown",
  }),

  // Arrays
  tagCount: $.size("$tags"),
  prices: $.map({
    input: "$items", as: "item",
    in: $ => $.multiply("$$item.price", "$$item.qty"),
  }),

  // Null handling
  displayName: $.ifNull("$nickname", "$name", "Anonymous"),
}))
```

---

## Running Tests

```bash
# Full test suite (type checks + runtime tests)
npm test

# Runtime tests only
npm run test:runtime

# Type checks only
npm run test:types
```

## Build

```bash
npm run build:ci
```
