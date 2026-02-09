---
sidebar_position: 3
---

# Quick Start

Get up and running with Sluice in minutes.

## 1. Set Up Your Project

```bash
mkdir my-sluice-app && cd my-sluice-app
npm init -y
npm install sluice-orm mongodb @effect/schema
npm install --save-dev typescript @types/node tsx
```

Create a `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true
  }
}
```

## 2. Define Your Schemas

```typescript
// schemas.ts
import { Schema as S } from "@effect/schema";

export const UserSchema = S.Struct({
  _id: S.String,
  name: S.String,
  email: S.String,
  age: S.Number,
  department: S.String,
  createdAt: S.Date,
});

export const OrderSchema = S.Struct({
  _id: S.String,
  userId: S.String,
  amount: S.Number,
  items: S.Array(S.Struct({
    productId: S.String,
    quantity: S.Number,
    price: S.Number,
  })),
  createdAt: S.Date,
});
```

## 3. Create a Registry

```typescript
// db.ts
import { MongoClient } from "mongodb";
import { registry } from "sluice-orm";
import { UserSchema, OrderSchema } from "./schemas.js";

export const dbRegistry = registry("8.0", {
  users: UserSchema,
  orders: OrderSchema,
});

export async function connectDB() {
  const client = await MongoClient.connect("mongodb://localhost:27017");
  return dbRegistry(client.db("myapp"));
}
```

## 4. Write Type-Safe Pipelines

```typescript
// app.ts
import { $match, $group, $sort, $project, $unwind } from "sluice-orm";
import { connectDB } from "./db.js";

async function main() {
  const { users, orders } = await connectDB();

  // Type-safe aggregation — every type is inferred
  const topSpenders = await orders
    .aggregate(
      $group($ => ({
        _id: "$userId",
        totalSpent: $.sum("$amount"),
        orderCount: $.sum(1),
      })),
      $match($ => ({ totalSpent: { $gt: 100 } })),
      $sort({ totalSpent: -1 }),
      $project($ => ({
        userId: "$_id",
        totalSpent: 1,
        orderCount: 1,
        _id: 0,
      })),
    )
    .toList();

  console.log("Top spenders:", topSpenders);
  // topSpenders: { userId: string; totalSpent: number; orderCount: number }[]

  // Type-safe CRUD
  const adults = await users
    .find($ => ({ age: { $gte: 18 } }), { sort: { name: 1 } })
    .toList();

  // Type-safe updates
  await users.updateOne(
    $ => ({ _id: "user1" }),
    { $set: { department: "Engineering Lead" } },
  ).execute();
}

main().catch(console.error);
```

## 5. Run

```bash
npx tsx app.ts
```

## What You Built

- **Type-safe schemas** — Compile-time validation of data structures
- **Type-safe queries** — Field references validated against schemas
- **Type-safe aggregations** — Each stage's output type flows to the next
- **Full IntelliSense** — Autocomplete for all operators and field paths

## Next Steps

- **[Core Concepts](./core-concepts/schemas.md)** — Deep dive into schemas
- **[Advanced Type Inference](./advanced-typings.md)** — Complex pipeline type flow
- **[API Reference](./api.md)** — Complete API documentation
