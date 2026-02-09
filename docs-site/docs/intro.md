---
sidebar_position: 1
---

# Welcome to Sluice

**Type-safe MongoDB aggregation pipeline builder** where every stage's output type becomes the next stage's input — fully inferred, zero runtime overhead.

## What Makes Sluice Different?

Sluice provides **compile-time type safety** for MongoDB operations. Every `$field` reference, every expression operator, and every accumulator is validated against your document schema at compile time — and the output type of each stage is **inferred automatically**.

```typescript
// The return type is inferred from the pipeline — not from a generic annotation
const result = await users
  .aggregate(
    $match($ => ({ age: { $gte: 18 } })),
    $group($ => ({
      _id: "$department",
      avgAge: $.avg("$age"),      // ✅ "age" is numeric — $.avg accepts it
      headcount: $.sum(1),
    })),
    $sort({ headcount: -1 }),
  )
  .toList();

// result: { _id: string; avgAge: number | null; headcount: number }[]
// ↑ This type was inferred, not annotated
```

## Key Features

- **Full type inference** — Every stage computes its output type. Chain 10 stages and the final type is exact.
- **Schema agnostic** — Works with Effect Schema, Zod, or plain TypeScript types.
- **Zero runtime overhead** — All type checking happens at compile time.
- **Full MongoDB 8.0+ support** — All aggregation operators, CRUD operations, and advanced features.
- **Effect integration** — Optional functional programming with Effect.ts.

## Quick Example

```typescript
import { registry, $match, $group, $sort } from "sluice-orm";
import { Schema as S } from "@effect/schema";

const UserSchema = S.Struct({
  _id: S.String,
  name: S.String,
  age: S.Number,
  department: S.String,
});

const db = registry("8.0", { users: UserSchema });

const result = await db(client.db("myapp"))
  .users.aggregate(
    $match($ => ({ age: { $gte: 18 } })),
    $group($ => ({
      _id: "$department",
      avgAge: $.avg("$age"),
      headcount: $.sum(1),
    })),
    $sort({ headcount: -1 }),
  )
  .toList();

// result: { _id: string; avgAge: number | null; headcount: number }[]
```

## Why Sluice?

Traditional MongoDB drivers provide no type safety:

```typescript
// No type safety — runtime errors waiting to happen
collection.aggregate([
  { $match: { age: { $gte: 18 } } },
  { $group: { _id: "$departement", avgAge: { $avg: "$agee" } } },
  //                 ^ typo: "departement"         ^ typo: "agee"
  // These won't fail until production 💥
]);
```

Sluice catches these at compile time:

```typescript
$group($ => ({
  _id: "$departement",     // ❌ TS Error: field "departement" does not exist
  avgAge: $.avg("$agee"),  // ❌ TS Error: field "agee" does not exist
})),
```

## Next Steps

- **[Installation](./installation.md)** — Get started with Sluice
- **[Quick Start](./quick-start.md)** — Your first type-safe pipeline
- **[Advanced Type Inference](./advanced-typings.md)** — See the type system in action
