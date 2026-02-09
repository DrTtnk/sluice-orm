---
sidebar_position: 7
---

# CRUD Operations

Sluice wraps every MongoDB CRUD operation with full type safety. All operations return **builder objects** — call `.execute()` for mutations and `.toList()` / `.toOne()` for reads.

## Setup

```typescript
import { Schema as S } from "@effect/schema";
import { registry } from "sluice-orm";

const UserSchema = S.Struct({
  _id: S.String,
  name: S.String,
  email: S.String,
  age: S.Number,
  active: S.Boolean,
  tags: S.Array(S.String),
});

const db = registry("8.0", { users: UserSchema });
const { users } = db(client.db("myapp"));
```

## Find

### find()

Returns multiple documents as an array.

```typescript
// No filter — find all
const allUsers = await users.find().toList();
// Type: User[]

// With filter
const adults = await users
  .find($ => ({ age: { $gte: 18 }, active: true }))
  .toList();
// Type: User[]

// With options (sort, limit, skip)
const page = await users
  .find($ => ({ active: true }), {
    sort: { name: 1 },
    limit: 10,
    skip: 20,
  })
  .toList();
```

### findOne()

Returns a single document or null.

```typescript
const user = await users
  .findOne($ => ({ _id: "user123" }))
  .toOne();
// Type: User | null
```

## Insert

### insertOne()

Inserts a single document. The document must match the schema type exactly.

```typescript
await users.insertOne({
  _id: "user1",
  name: "Alice",
  email: "alice@example.com",
  age: 30,
  active: true,
  tags: ["admin"],
}).execute();

// TypeScript error if schema doesn't match:
// await users.insertOne({ name: "Alice" }).execute();
// ❌ Missing _id, email, age, active, tags
```

### insertMany()

Inserts multiple documents.

```typescript
await users.insertMany([
  { _id: "user1", name: "Alice", email: "alice@example.com", age: 30, active: true, tags: ["admin"] },
  { _id: "user2", name: "Bob", email: "bob@example.com", age: 25, active: false, tags: [] },
]).execute();
```

## Update

### updateOne() / updateMany()

Updates documents using MongoDB update operators (`$set`, `$inc`, `$unset`, etc). The update operators are type-checked against the schema.

```typescript
// Update one document
await users.updateOne(
  $ => ({ _id: "user1" }),
  { $set: { name: "Alice Smith" }, $inc: { age: 1 } },
).execute();

// Update many documents
await users.updateMany(
  $ => ({ active: false }),
  { $set: { active: true } },
).execute();
```

### replaceOne()

Replaces an entire document. The replacement must match the schema.

```typescript
await users.replaceOne(
  $ => ({ _id: "user1" }),
  { _id: "user1", name: "Alice New", email: "new@example.com", age: 31, active: true, tags: [] },
).execute();
```

## Delete

### deleteOne() / deleteMany()

```typescript
await users.deleteOne($ => ({ _id: "user1" })).execute();

await users.deleteMany($ => ({ active: false })).execute();
```

## Find and Modify

Atomic find-and-modify operations. Return the document (or null).

### findOneAndUpdate()

```typescript
const updated = await users.findOneAndUpdate(
  $ => ({ _id: "user1" }),
  { $set: { lastLogin: new Date() } },
).execute();
// Type: User | null
```

### findOneAndReplace()

```typescript
const replaced = await users.findOneAndReplace(
  $ => ({ _id: "user1" }),
  { _id: "user1", name: "Replaced", email: "r@example.com", age: 0, active: false, tags: [] },
).execute();
// Type: User | null
```

### findOneAndDelete()

```typescript
const deleted = await users.findOneAndDelete(
  $ => ({ _id: "user1" }),
).execute();
// Type: User | null
```

## Count and Distinct

### countDocuments()

```typescript
// Count all
const total = await users.countDocuments().execute();
// Type: number

// Count with filter
const activeCount = await users
  .countDocuments($ => ({ active: true }))
  .execute();
// Type: number
```

### estimatedDocumentCount()

Faster than `countDocuments()` but less accurate — uses collection metadata.

```typescript
const estimated = await users.estimatedDocumentCount().execute();
// Type: number
```

### distinct()

Returns distinct values for a field. The field name is type-checked and the return type matches the field's type.

```typescript
const names = await users.distinct("name").execute();
// Type: string[]

const ages = await users.distinct("age").execute();
// Type: number[]

// With filter
const activeNames = await users
  .distinct("name", $ => ({ active: true }))
  .execute();
// Type: string[]
```

## Bulk Write

Performs multiple write operations in a single command.

```typescript
const result = await users.bulkWrite([
  { insertOne: { document: { _id: "u1", name: "A", email: "a@x.com", age: 20, active: true, tags: [] } } },
  { updateOne: { filter: { _id: "u2" }, update: { $set: { age: 30 } } } },
  { updateMany: { filter: { active: false }, update: { $set: { active: true } } } },
  { deleteOne: { filter: { _id: "u3" } } },
  { replaceOne: {
    filter: { _id: "u4" },
    replacement: { _id: "u4", name: "Z", email: "z@x.com", age: 0, active: false, tags: [] },
  }},
]).execute();

// Ordered execution (operations run sequentially, stop on first error)
await users.bulkWrite([
  { insertOne: { document: { _id: "seq1", name: "First", email: "1@x.com", age: 1, active: true, tags: [] } } },
  { updateOne: { filter: { _id: "seq1" }, update: { $set: { age: 42 } } } },
]).execute({ ordered: true });
```

## Effect Integration

With the Effect registry, all CRUD operations return `Effect<T, MongoError>`:

```typescript
import { registryEffect } from "sluice-orm";
import { Effect } from "effect";

const program = Effect.gen(function* () {
  const reg = yield* registryEffect("8.0", { users: UserSchema });

  // All operations are effectful
  const allUsers = yield* reg.users.find().toList();

  yield* reg.users
    .updateOne(() => ({ _id: "1" }), { $set: { age: 31 } })
    .execute();

  const deleted = yield* reg.users
    .deleteOne(() => ({ _id: "1" }))
    .execute();
});
```
