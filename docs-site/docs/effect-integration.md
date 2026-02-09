---
sidebar_position: 9
---

# Effect Integration

Sluice provides first-class [Effect](https://effect.website/) integration for functional programming with dependency injection, tagged errors, and composable operations.

## Why Use Effect?

The Effect integration provides:

- **Tagged errors** — `MongoError` with operation context instead of thrown exceptions
- **Layer-based DI** — `MongoDbClient` service for testable, composable database access
- **Effect.gen** — Sequential async operations with automatic error propagation
- **Type-safe composition** — All operations return `Effect.Effect<T, MongoError>`

## Setup

Install Effect alongside Sluice:

```bash
npm install sluice-orm mongodb effect
```

### Create the Layer

Use `makeMongoDbClientLayer` to create a Layer that provides the `MongoDbClient` service:

```typescript
import { MongoClient } from "mongodb";
import { makeMongoDbClientLayer } from "sluice-orm";

const client = await MongoClient.connect("mongodb://localhost:27017");
const mongoLayer = makeMongoDbClientLayer(client.db("myapp"));
```

### Define Your Registry

```typescript
import { Schema as S } from "effect/Schema";
import { registryEffect } from "sluice-orm";

const UserSchema = S.Struct({
  _id: S.String,
  name: S.String,
  age: S.Number,
  active: S.Boolean,
});

const OrderSchema = S.Struct({
  _id: S.String,
  userId: S.String,
  amount: S.Number,
  status: S.String,
});

// registryEffect returns an Effect that yields the registry
const makeRegistry = registryEffect("8.0", {
  users: UserSchema,
  orders: OrderSchema,
});
```

## Basic CRUD Operations

All CRUD operations return `Effect.Effect<T, MongoError>`:

```typescript
import { Effect } from "effect";

const program = Effect.gen(function* () {
  const registry = yield* makeRegistry;

  // Insert
  const insertResult = yield* registry.users
    .insertOne({ _id: "u1", name: "Alice", age: 30, active: true })
    .execute();

  // Find
  const user = yield* registry.users.find(() => ({ _id: "u1" })).toOne();
  console.log(user); // { _id: "u1", name: "Alice", age: 30, active: true }

  // Update
  yield* registry.users
    .updateOne(() => ({ _id: "u1" }), { $set: { age: 31 } })
    .execute();

  // Delete
  yield* registry.users.deleteOne(() => ({ _id: "u1" })).execute();

  return "success";
});

const result = await Effect.runPromise(program.pipe(Effect.provide(mongoLayer)));
```

### Find Operations

```typescript
const program = Effect.gen(function* () {
  const registry = yield* makeRegistry;

  // Find many
  const activeUsers = yield* registry.users
    .find(() => ({ active: true }))
    .toList();

  // Find one
  const user = yield* registry.users
    .find(() => ({ _id: "u1" }))
    .toOne();

  // With options
  const sorted = yield* registry.users
    .find(() => ({ age: { $gte: 18 } }), { sort: { name: 1 }, limit: 10 })
    .toList();

  return { activeUsers, user, sorted };
});
```

### Insert Operations

```typescript
const program = Effect.gen(function* () {
  const registry = yield* makeRegistry;

  // Insert one
  yield* registry.users
    .insertOne({ _id: "u2", name: "Bob", age: 25, active: true })
    .execute();

  // Insert many
  yield* registry.users
    .insertMany([
      { _id: "u3", name: "Carol", age: 35, active: false },
      { _id: "u4", name: "Dave", age: 40, active: true },
    ])
    .execute();

  return "inserted";
});
```

### Update Operations

```typescript
const program = Effect.gen(function* () {
  const registry = yield* makeRegistry;

  // Update one
  yield* registry.users
    .updateOne(() => ({ _id: "u1" }), { $inc: { age: 1 } })
    .execute();

  // Update many
  yield* registry.users
    .updateMany(() => ({ active: false }), { $set: { active: true } })
    .execute();

  // Replace one
  yield* registry.users
    .replaceOne(
      () => ({ _id: "u1" }),
      { _id: "u1", name: "Alice Smith", age: 31, active: true },
    )
    .execute();

  return "updated";
});
```

### Bulk Operations

```typescript
const program = Effect.gen(function* () {
  const registry = yield* makeRegistry;

  const bulkResult = yield* registry.users
    .bulkWrite([
      { insertOne: { document: { _id: "u5", name: "Eve", age: 28, active: true } } },
      { updateOne: { filter: { _id: "u1" }, update: { $inc: { age: 1 } } } },
      { deleteOne: { filter: { _id: "u2" } } },
    ])
    .execute();

  console.log(`Inserted: ${bulkResult.insertedCount}`);
  console.log(`Modified: ${bulkResult.modifiedCount}`);
  console.log(`Deleted: ${bulkResult.deletedCount}`);

  return bulkResult;
});
```

## Aggregation Pipelines

Aggregation pipelines work exactly like the regular registry, but return `Effect.Effect`:

```typescript
import { $match, $group, $project, $sort } from "sluice-orm";

const program = Effect.gen(function* () {
  const registry = yield* makeRegistry;

  const results = yield* registry.users
    .aggregate(
      $match(() => ({ active: true })),
      $group($ => ({
        _id: "$age",
        count: $.sum(1),
        avgAge: $.avg("$age"),
      })),
      $sort({ count: -1 }),
      $project($ => ({
        ageGroup: "$_id",
        count: $.include,
        avgAge: $.include,
        _id: $.exclude,
      })),
    )
    .toList();

  // results: { ageGroup: number; count: number; avgAge: number | null }[]
  return results;
});

await Effect.runPromise(program.pipe(Effect.provide(mongoLayer)));
```

### Debug Pipelines

`.toMQL()` works the same way:

```typescript
const program = Effect.gen(function* () {
  const registry = yield* makeRegistry;

  const pipeline = registry.users.aggregate(
    $match(() => ({ status: "active" })),
    $group($ => ({ _id: "$role", count: $.sum(1) })),
  );

  console.log(pipeline.toMQL());
  // [
  //   { "$match": { "status": "active" } },
  //   { "$group": { "_id": "$role", "count": { "$sum": 1 } } }
  // ]

  return yield* pipeline.toList();
});
```

## Error Handling

All operations that fail produce a **tagged** `MongoError`:

```typescript
import { Effect } from "effect";

const program = Effect.gen(function* () {
  const registry = yield* makeRegistry;

  // This will fail if the connection is closed
  const user = yield* registry.users.find(() => ({ _id: "nonexistent" })).toOne();

  return user;
});

// Handle errors with Effect.either
const result = await Effect.runPromise(
  program.pipe(Effect.provide(mongoLayer), Effect.either),
);

if (result._tag === "Left") {
  console.error("Operation failed:", result.left);
  console.log("Error tag:", result.left._tag); // "MongoError"
  console.log("Operation:", result.left.operation); // "find.toOne"
  console.log("Message:", result.left.message);
} else {
  console.log("Success:", result.right);
}
```

### MongoError Structure

```typescript
{
  _tag: "MongoError",
  operation: string,  // e.g., "find.toList", "insertOne.execute"
  cause: unknown,     // The original error
  message: string,    // Human-readable message
}
```

### Error Recovery

Use Effect's error handling combinators:

```typescript
const program = Effect.gen(function* () {
  const registry = yield* makeRegistry;

  // Fallback to default value on error
  const user = yield* registry.users
    .find(() => ({ _id: "u1" }))
    .toOne()
    .pipe(Effect.catchAll(() => Effect.succeed(null)));

  // Retry on failure
  const withRetry = yield* registry.users
    .find(() => ({ _id: "u1" }))
    .toList()
    .pipe(Effect.retry({ times: 3 }));

  return { user, withRetry };
});
```

## Composing Multiple Operations

Effect.gen makes it easy to chain database operations:

```typescript
const program = Effect.gen(function* () {
  const registry = yield* makeRegistry;

  // Create a user
  yield* registry.users
    .insertOne({ _id: "u1", name: "Alice", age: 30, active: true })
    .execute();

  // Create an order for that user
  yield* registry.orders
    .insertOne({ _id: "o1", userId: "u1", amount: 100, status: "pending" })
    .execute();

  // Update user's age
  yield* registry.users
    .updateOne(() => ({ _id: "u1" }), { $inc: { age: 1 } })
    .execute();

  // Fetch both
  const user = yield* registry.users.find(() => ({ _id: "u1" })).toOne();
  const orders = yield* registry.orders.find(() => ({ userId: "u1" })).toList();

  return { user, orders };
});

const result = await Effect.runPromise(program.pipe(Effect.provide(mongoLayer)));
// result: {
//   user: { _id: "u1", name: "Alice", age: 31, active: true },
//   orders: [{ _id: "o1", userId: "u1", amount: 100, status: "pending" }]
// }
```

## Testing with Layers

Layers make it easy to swap implementations for testing:

```typescript
import { Effect, Layer } from "effect";
import { MongoDbClient, registryEffect } from "sluice-orm";

// Production layer
const prodLayer = makeMongoDbClientLayer(realClient.db("prod"));

// Test layer with in-memory MongoDB or mock
const testLayer = makeMongoDbClientLayer(testClient.db("test"));

const program = Effect.gen(function* () {
  const registry = yield* registryEffect("8.0", { users: UserSchema });
  return yield* registry.users.find(() => ({})).toList();
});

// Run with production layer
await Effect.runPromise(program.pipe(Effect.provide(prodLayer)));

// Run with test layer
await Effect.runPromise(program.pipe(Effect.provide(testLayer)));
```

### Mock MongoDbClient Service

```typescript
const mockDb = {
  collection: (name: string) => ({
    find: () => ({
      toArray: async () => [{ _id: "mock", name: "Mock User", age: 99, active: true }],
    }),
    // ... other mock methods
  }),
};

const mockLayer = Layer.succeed(MongoDbClient, { db: mockDb as any });

const result = await Effect.runPromise(program.pipe(Effect.provide(mockLayer)));
// Uses mock data instead of real MongoDB
```

## Comparison: Regular vs Effect Registry

| Feature                | `registry()`              | `registryEffect()`                      |
|------------------------|---------------------------|-----------------------------------------|
| Return type            | `Promise<T>`              | `Effect.Effect<T, MongoError>`          |
| Error handling         | `try/catch`               | Tagged `MongoError` + Effect combinators |
| Dependency injection   | Direct `Db` argument      | Layer-based `MongoDbClient` service     |
| Composability          | `async/await`             | `Effect.gen` + Effect combinators       |
| Testing                | Manual mocking            | Swap layers                             |
| Error recovery         | Manual                    | `Effect.retry`, `Effect.catchAll`, etc. |

### When to Use Effect

Use `registryEffect` if you:

- Already use Effect.ts in your codebase
- Want Layer-based dependency injection
- Need tagged errors instead of exceptions
- Want composable error recovery (retry, fallback, etc.)
- Need testability via Layer swapping

Use `registry` if you:

- Prefer simple `async/await`
- Don't need functional programming patterns
- Want minimal setup (just pass the `Db` directly)

## Full Example

```typescript
import { Effect } from "effect";
import { MongoClient } from "mongodb";
import { Schema as S } from "effect/Schema";
import { registryEffect, makeMongoDbClientLayer, $match, $group } from "sluice-orm";

// Schemas
const UserSchema = S.Struct({
  _id: S.String,
  name: S.String,
  age: S.Number,
  active: S.Boolean,
});

const OrderSchema = S.Struct({
  _id: S.String,
  userId: S.String,
  amount: S.Number,
  status: S.String,
});

// Setup
const client = await MongoClient.connect("mongodb://localhost:27017");
const mongoLayer = makeMongoDbClientLayer(client.db("myapp"));

// Program
const program = Effect.gen(function* () {
  const registry = yield* registryEffect("8.0", {
    users: UserSchema,
    orders: OrderSchema,
  });

  // Insert users
  yield* registry.users
    .insertMany([
      { _id: "u1", name: "Alice", age: 30, active: true },
      { _id: "u2", name: "Bob", age: 25, active: true },
    ])
    .execute();

  // Insert orders
  yield* registry.orders
    .insertMany([
      { _id: "o1", userId: "u1", amount: 100, status: "complete" },
      { _id: "o2", userId: "u1", amount: 200, status: "pending" },
      { _id: "o3", userId: "u2", amount: 150, status: "complete" },
    ])
    .execute();

  // Aggregate: total spending per active user
  const spending = yield* registry.orders
    .aggregate(
      $match(() => ({ status: "complete" })),
      $group($ => ({
        _id: "$userId",
        totalSpent: $.sum("$amount"),
        orderCount: $.sum(1),
      })),
    )
    .toList();

  // Find users matching the aggregated data
  const userIds = spending.map(s => s._id);
  const users = yield* registry.users
    .find(() => ({ _id: { $in: userIds }, active: true }))
    .toList();

  return { spending, users };
});

// Run
const result = await Effect.runPromise(program.pipe(Effect.provide(mongoLayer)));
console.log(result);
// {
//   spending: [
//     { _id: "u1", totalSpent: 300, orderCount: 2 },
//     { _id: "u2", totalSpent: 150, orderCount: 1 },
//   ],
//   users: [
//     { _id: "u1", name: "Alice", age: 30, active: true },
//     { _id: "u2", name: "Bob", age: 25, active: true },
//   ]
// }
```

## API Reference

### `registryEffect(version, schemas)`

Creates an Effect that yields a typed registry.

**Parameters:**
- `version`: MongoDB version (e.g., `"8.0"`)
- `schemas`: Record of collection names to schemas

**Returns:** `Effect.Effect<Registry, never, MongoDbClient>`

---

### `makeMongoDbClientLayer(db)`

Creates a Layer that provides the `MongoDbClient` service.

**Parameters:**
- `db`: MongoDB `Db` instance

**Returns:** `Layer.Layer<MongoDbClient>`

---

### `MongoError`

Tagged error type for all database operations.

**Fields:**
- `_tag`: `"MongoError"`
- `operation`: Operation name (e.g., `"find.toList"`)
- `cause`: Original error
- `message`: Human-readable error message

---

### Collection Methods (Effect)

All methods return `Effect.Effect<T, MongoError>`:

| Method                   | Returns                                  |
|--------------------------|------------------------------------------|
| `.find().toList()`       | `Effect.Effect<T[], MongoError>`         |
| `.find().toOne()`        | `Effect.Effect<T \| null, MongoError>`   |
| `.insertOne().execute()` | `Effect.Effect<InsertOneResult, MongoError>` |
| `.insertMany().execute()`| `Effect.Effect<InsertManyResult, MongoError>` |
| `.updateOne().execute()` | `Effect.Effect<UpdateResult, MongoError>` |
| `.updateMany().execute()`| `Effect.Effect<UpdateResult, MongoError>` |
| `.deleteOne().execute()` | `Effect.Effect<DeleteResult, MongoError>` |
| `.deleteMany().execute()`| `Effect.Effect<DeleteResult, MongoError>` |
| `.aggregate().toList()`  | `Effect.Effect<T[], MongoError>`         |
