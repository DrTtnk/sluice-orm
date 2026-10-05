---
sidebar_position: 1
---

# Schemas

Sluice is **schema-agnostic** - it works with any validation library or plain TypeScript types. Choose the approach that fits your project.

## Effect Schema (Recommended)

Effect Schema provides excellent TypeScript inference and runtime validation:

```typescript
import { Schema as S } from "@effect/schema";

const UserSchema = S.Struct({
  _id: S.String,
  name: S.String,
  email: S.String,
  age: S.Number,
  department: S.String,
  createdAt: S.Date,
  tags: S.Array(S.String),
  profile: S.Struct({
    avatar: S.String,
    bio: S.optional(S.String),
  }),
});

// Inferred type: {
//   _id: string;
//   name: string;
//   email: string;
//   age: number;
//   department: string;
//   createdAt: Date;
//   tags: string[];
//   profile: {
//     avatar: string;
//     bio?: string | undefined;
//   };
// }
```

## Zod

Zod is another excellent choice for schema validation:

```typescript
import { z } from "zod";

const UserSchema = z.object({
  _id: z.string(),
  name: z.string(),
  email: z.string(),
  age: z.number(),
  department: z.string(),
  createdAt: z.date(),
  tags: z.array(z.string()),
  profile: z.object({
    avatar: z.string(),
    bio: z.string().optional(),
  }),
});

// Inferred type is the same as Effect Schema
```

## Plain TypeScript Types

For projects that don't need runtime validation:

```typescript
const UserSchema = {
  Type: null! as {
    _id: string;
    name: string;
    email: string;
    age: number;
    department: string;
    createdAt: Date;
    tags: string[];
    profile: {
      avatar: string;
      bio?: string;
    };
  }
};
```

## Schema Requirements

### Required Properties

- **Must have a `Type` property** that represents the TypeScript type
- **Type must be assignable to `Document`** (MongoDB's base document type)

### Optional Properties

- **Runtime validation** - Invoke your schema library explicitly when handling untrusted data
- **Custom parsing/serialization** - Schemas can include transformation logic

## Advanced Schema Patterns

### Union Types

```typescript
const EventSchema = S.Union(
  S.Struct({
    type: S.Literal("click"),
    elementId: S.String,
    coordinates: S.Struct({ x: S.Number, y: S.Number }),
  }),
  S.Struct({
    type: S.Literal("purchase"),
    productId: S.String,
    amount: S.Number,
  }),
);

// Type: { type: "click"; elementId: string; coordinates: { x: number; y: number } }
//     | { type: "purchase"; productId: string; amount: number }
```

### Recursive Schemas

```typescript
const CategorySchema: S.Schema<Category> = S.Struct({
  _id: S.String,
  name: S.String,
  parent: S.optional(S.suspend(() => CategorySchema)),
});

// Recursive type definition
interface Category {
  _id: string;
  name: string;
  parent?: Category;
}
```

### Enums and Literals

```typescript
const StatusSchema = S.Literal("active", "inactive", "pending");

const UserSchema = S.Struct({
  _id: S.String,
  status: StatusSchema, // Only allows these three values
});
```

## Schema Validation

Sluice extracts TypeScript document types from schemas. It does **not** automatically
parse inputs, decode query results, or install database validators. Effect and Zod
can perform runtime validation when called explicitly in your application.

```typescript
// Validate external data before submitting it to Sluice.
const user = UserSchema.parse(untrustedInput); // Zod example
await users.insertOne(user).execute();

// Plain markers provide compile-time checks only.
// All writes are builders: awaiting insertOne(...) without .execute() does not write.
```

Existing database documents must match the declared schema. Use MongoDB validators
or application validation if that must also be enforced at runtime.

## Next Steps

- **Advanced Typings** - Explore discriminated unions and complex type inference
- **API Reference** - Complete documentation of all operators and methods