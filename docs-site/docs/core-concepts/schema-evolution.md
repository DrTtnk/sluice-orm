---
sidebar_position: 3
---

# Schema Evolution

Production schemas change. Fields get added, renamed, reshaped, removed. Sluice's `migrate()` function turns schema migrations into **type-checked pipelines** — the compiler proves your transformation is correct before it touches a single document.

## The Problem

Schema migrations in MongoDB are risky:
- Forget to add a new required field → runtime `null` errors
- Typo in a field name → data written to the wrong key
- Remove a field that other code still reads → silent breakage
- Transformation produces the wrong shape → corrupted data

All of these are **silent**. MongoDB won't complain. Your app will — at 3 AM.

## The Solution

`migrate<OldSchema, NewSchema>()` gives you a pipeline builder that:

1. **Only accepts update-allowed stages** — no `$group`, `$sort`, `$match`
2. **Validates the transformation** — the output must match `NewSchema` exactly
3. **Produces a MongoDB update pipeline** — runs natively via `updateMany`

```typescript
import { migrate, $set, $unset, $addFields } from "sluice-orm";

// Version 1 of our schema
type UserV1 = {
  _id: string;
  name: string;
  age: number;
  legacyField: string;
};

// Version 2 — added email, removed legacyField
type UserV2 = {
  _id: string;
  name: string;
  age: number;
  email: string;
};

const m = migrate<UserV1, UserV2>();

const migration = m.pipe(
  $set({ email: "unknown@example.com" }),
  $unset("legacyField"),
);
// ✅ Compiles — output matches UserV2
```

## Computed Fields

Use expression operators to derive new field values from existing data:

```typescript
type UserV2 = {
  _id: string;
  fullName: string;        // computed from first + last
  firstName: string;
  lastName: string;
  createdAt: Date;
};

type UserV1 = {
  _id: string;
  firstName: string;
  lastName: string;
  createdAt: Date;
};

const m = migrate<UserV1, UserV2>();

const migration = m.pipe(
  $addFields($ => ({
    fullName: $.concat("$firstName", " ", "$lastName"),
    // Type: `${string} ${string}`  — assignable to string ✅
  })),
);
```

## What the Compiler Catches

### Missing fields

```typescript
// ❌ Forgot to add email — output doesn't match UserV2
const bad = m.pipe(
  $unset("legacyField"),
);
// OpaqueError: "Migration output does not match target schema"
```

### Wrong field types

```typescript
// ❌ email should be string, not number
const bad = m.pipe(
  $set({ email: 42 }),
  $unset("legacyField"),
);
// Type error: number is not assignable to string
```

### Non-update stages

MongoDB update pipelines only support specific stages. Sluice enforces this:

```typescript
// ❌ $group can't appear in an update pipeline
const bad = m.pipe($group($ => ({ _id: "$name", count: $.sum(1) })));

// ❌ $sort can't appear in an update pipeline
const bad = m.pipe($sort({ name: 1 }));

// ❌ $match can't appear in an update pipeline
const bad = m.pipe($match(() => ({ name: "test" })));
```

The only allowed stages are: `$set`, `$unset`, `$addFields`, `$project`, `$replaceRoot`, `$replaceWith`.

## Multi-Step Migrations

Complex schema changes may need multiple stages:

```typescript
type ProductV1 = {
  _id: string;
  name: string;
  price: number;
  currency: string;
};

type ProductV2 = {
  _id: string;
  name: string;
  pricing: {
    amount: number;
    currency: string;
  };
};

const m = migrate<ProductV1, ProductV2>();

const migration = m.pipe(
  // Step 1: Create nested structure from flat fields
  $addFields($ => ({
    pricing: {
      amount: "$price",
      currency: "$currency",
    },
  })),
  // Step 2: Remove old flat fields
  $unset("price", "currency"),
);
// ✅ Output matches ProductV2
```

Each stage's output type feeds into the next. The compiler tracks the evolving shape through every step.

## Running Migrations

Migrations produce a standard MongoDB update pipeline:

```typescript
// Apply to all documents
await collection.updateMany(
  () => ({}),
  $ => migration,
).execute();

// Apply selectively
await collection.updateMany(
  () => ({ legacyField: { $exists: true } }),
  $ => migration,
).execute();
```

## Allowed Stages

| Stage | Purpose |
|---|---|
| `$set` / `$addFields` | Add or overwrite fields |
| `$unset` | Remove fields |
| `$project` | Reshape documents (include/exclude/compute) |
| `$replaceRoot` / `$replaceWith` | Replace the entire document |

These are the only stages MongoDB supports in update pipelines, and the only stages `migrate()` accepts.

## Why Not Just Write Raw Updates?

Raw `{ $set: { ... }, $unset: { ... } }` updates work, but they have no connection between the old and new schema. You can:
- Set fields that don't exist in the new schema
- Miss required fields
- Use wrong types

`migrate()` makes the connection explicit: **`OldSchema` goes in, `NewSchema` comes out, and the compiler proves it**.
