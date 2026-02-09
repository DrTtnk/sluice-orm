---
sidebar_position: 8
---

# Migrations

Sluice provides a `migrate()` function for **type-safe collection schema migrations** via MongoDB update pipelines. The migration pipeline validates at compile time that the final output matches your target schema.

## How It Works

`migrate<OldSchema, NewSchema>()` returns a builder with a `.pipe()` method that only accepts **update-allowed stages**: `$set`, `$unset`, `$addFields`, `$project`, `$replaceRoot`, `$replaceWith`.

The pipeline validates that applying all stages to `OldSchema` produces a type assignable to `NewSchema`. If it doesn't — compile error.

## Basic Migration

```typescript
import { migrate, $set, $unset, $addFields } from "sluice-orm";

type OldUser = {
  _id: string;
  name: string;
  age: number;
  legacyField: string;    // ← field to remove
};

type NewUser = {
  _id: string;
  name: string;
  age: number;
  email: string;           // ← new field
};

const m = migrate<OldUser, NewUser>();

// ✅ Valid: add email, remove legacyField → output matches NewUser
const migration = m.pipe(
  $set({ email: "unknown@example.com" }),
  $unset("legacyField"),
);
```

## Using Expressions

You can use expression operators inside `$addFields` / `$set` to compute migration values from existing fields:

```typescript
const migration = m.pipe(
  $addFields($ => ({
    email: $.concat("$name", "@migrated.com"),
    // Type: `${string}@migrated.com`
  })),
  $unset("legacyField"),
);
```

## Type Safety

### Wrong output shape → compile error

```typescript
// ❌ Missing email — output doesn't match NewUser
const bad = m.pipe(
  $unset("legacyField"),
);
// TypeScript error: output { _id, name, age } is not assignable to NewUser
```

### Non-update stages → compile error

Only stages that can appear in a MongoDB update pipeline are allowed:

```typescript
// ❌ $group is not allowed in migration pipelines
const invalid1 = m.pipe($group($ => ({ _id: "$name", count: $.sum(1) })));

// ❌ $sort is not allowed
const invalid2 = m.pipe($sort({ name: 1 }));

// ❌ $match is not allowed
const invalid3 = m.pipe($match(() => ({ name: "test" })));
```

## Executing a Migration

The migration pipeline is used with `updateMany` to apply the transformation to all documents:

```typescript
// Apply migration to all documents in the collection
await collection.updateMany(
  () => ({}),                    // match all documents
  $ => migration,               // apply the migration pipeline
).execute();
```

## Allowed Stages

| Stage | Purpose |
|---|---|
| `$set` / `$addFields` | Add or overwrite fields |
| `$unset` | Remove fields |
| `$project` | Reshape documents |
| `$replaceRoot` | Replace the entire document |
| `$replaceWith` | Alias for `$replaceRoot` |
