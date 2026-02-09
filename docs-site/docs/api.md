---
sidebar_position: 11
---

# API Reference

Complete API reference for Sluice ORM.

## Core Exports

### Registry

```typescript
import { registry } from "sluice-orm";

const dbRegistry = registry("8.0", {
  users: UserSchema,
  orders: OrderSchema,
});
```

Creates a registry bound to a specific MongoDB version and schemas.

**Parameters:**
- `version`: MongoDB version string (e.g., `"8.0"`)
- `schemas`: Object mapping collection names to schemas

**Returns:** A function that accepts a MongoDB `Db` instance and returns typed collections.

### Collection Binding

```typescript
const { users, orders } = dbRegistry(client.db("myapp"));
```

Binds the registry to a specific database, returning typed collection instances. Each collection exposes both CRUD and aggregation methods.

## Aggregation Pipeline Stages

All stages are imported from the main package:

```typescript
import {
  $match, $group, $project, $sort, $limit, $skip,
  $unwind, $lookup, $addFields, $set, $unset,
  $facet, $bucket, $count, $sortByCount, $densify,
  $fill, $geoNear, $setWindowFields,
} from "sluice-orm";
```

### $match

Filters documents based on conditions. The callback receives `$` (expression builder) for `$expr` usage.

```typescript
$match($ => ({ age: { $gte: 18 }, status: "active" }))
$match($ => ({ "profile.name": { $regex: "John" } }))

// With $expr for cross-field comparisons
$match($ => ({
  $expr: $.gt("$salary", "$targetSalary"),
}))
```

### $group

Groups documents by a key and performs accumulations. The callback receives `$` (accumulator builder) with access to both accumulators and expression operators.

```typescript
$group($ => ({
  _id: "$department",
  totalSalary: $.sum("$salary"),          // Type: number
  avgAge: $.avg("$age"),                  // Type: number | null
  employeeCount: $.sum(1),               // Type: number
  names: $.push("$name"),               // Type: string[]
  uniqueRoles: $.addToSet("$role"),      // Type: string[]
}))
```

### $project

Reshapes documents by including/excluding fields and computing new ones.

```typescript
$project($ => ({
  name: $.include,      // or 1
  age: $.include,       // or 1
  _id: $.exclude,       // or 0
  fullName: $.concat("$firstName", " ", "$lastName"),
}))
```

### $sort

Sorts documents by specified fields.

```typescript
$sort({ age: -1, name: 1 })   // Descending age, ascending name
$sort({ createdAt: -1 })      // Most recent first
```

### $addFields / $set

Adds new fields to documents. `$set` is an alias.

```typescript
$addFields($ => ({
  fullName: $.concat("$firstName", " ", "$lastName"),
  ageInMonths: $.multiply("$age", 12),
  isAdult: $.gte("$age", 18),
}))
```

### $unset

Removes fields from documents.

```typescript
$unset("temporaryField", "debugInfo")
```

### $unwind

Deconstructs an array field into multiple documents.

```typescript
$unwind("$tags")
$unwind({ path: "$items", includeArrayIndex: "itemIndex" })
```

### $lookup

Performs left outer joins with other collections. `from` takes a **typed collection reference** from your bound registry.

```typescript
// Simple lookup
$lookup({
  from: boundRegistry.orders,
  localField: "userId",
  foreignField: "customerId",
  as: "userOrders",
})

// With sub-pipeline
$lookup({
  from: boundRegistry.orders,
  localField: "_id",
  foreignField: "userId",
  as: "recentOrders",
  pipeline: $ => $.pipe(
    $match($ => ({ status: "paid" })),
    $sort({ createdAt: -1 }),
    $limit(5),
  ),
})
```

### $facet

Processes multiple aggregation pipelines within a single stage. Each branch uses `$.pipe(...)`.

```typescript
$facet($ => ({
  totalCount: $.pipe(
    $count("total"),
  ),
  byCategory: $.pipe(
    $group($ => ({ _id: "$category", count: $.sum(1) })),
    $sort({ count: -1 }),
  ),
}))
```

### $limit / $skip

```typescript
$limit(10)    // Take first 10 documents
$skip(20)     // Skip first 20 documents
```

### $count

```typescript
$count("total")   // { total: number }
```

### $sortByCount

```typescript
$sortByCount("$category")   // { _id: string; count: number }
```

### $bucket

```typescript
$bucket({
  groupBy: "$price",
  boundaries: [0, 50, 100, 200],
  default: "other",
  output: $ => ({
    count: $.sum(1),
    avgPrice: $.avg("$price"),
  }),
})
```

## Expression Operators

All operators are accessed via the `$` callback argument in stages.

### Arithmetic Operators

```typescript
$.add("$price", "$tax")                    // Type: number
$.subtract("$total", "$discount")          // Type: number
$.multiply("$quantity", "$price")          // Type: number
$.divide("$total", "$count")              // Type: number
$.mod("$number", 2)                       // Type: number
$.abs("$balance")                         // Type: number
$.ceil("$price")                          // Type: number
$.floor("$price")                         // Type: number
$.round("$price", 2)                      // Type: number
```

### Comparison Operators

```typescript
$.eq("$status", "active")                 // Type: boolean
$.ne("$role", "admin")                    // Type: boolean
$.gt("$age", 18)                          // Type: boolean
$.gte("$score", 80)                       // Type: boolean
$.lt("$price", 100)                       // Type: boolean
$.lte("$quantity", 10)                    // Type: boolean
$.cmp("$a", "$b")                         // Type: -1 | 0 | 1
```

### Logical Operators

All logical operators take **expression operator calls** as arguments — not raw strings.

```typescript
$.and($.gt("$age", 18), $.eq("$status", "active"))   // Type: boolean
$.or($.lt("$age", 18), $.gt("$age", 65))             // Type: boolean
$.not("$active")                                      // Type: boolean

// Nested composition
$.and(
  $.gte("$age", 18),
  $.or(
    $.eq("$role", "admin"),
    $.gt("$score", 90),
  ),
)
```

### String Operators

```typescript
$.concat("$firstName", " ", "$lastName")  // Type: `${string} ${string}`
$.substr("$name", 0, 5)                  // Type: string
$.toLower("$email")                       // Type: string
$.toUpper("$department")                  // Type: string
$.strLenCP("$name")                       // Type: number
$.trim("$input")                          // Type: string
$.ltrim("$input")                         // Type: string
$.rtrim("$input")                         // Type: string
```

### Array Operators

```typescript
$.size("$tags")                            // Type: number
$.in("$status", ["active", "pending"])     // Type: boolean
$.arrayElemAt("$scores", 0)               // Type: element type
$.slice("$items", 0, 5)                   // Type: array type
$.concatArrays("$arr1", "$arr2")           // Type: merged array
$.reverseArray("$items")                   // Type: array type
$.sortArray({ input: "$scores", sortBy: -1 }) // Type: array type
```

### Conditional Operators

```typescript
// $cond — object syntax with if/then/else
$.cond({
  if: $.gte("$age", 18),
  then: "adult",
  else: "minor",
})
// Type: "adult" | "minor"

// $ifNull — fallback for nullable fields
$.ifNull("$nickname", "$name", "Anonymous")
// Type: string (narrowed from string | null)

// $switch — multi-branch with operator calls in case
$.switch({
  branches: [
    { case: $.lte("$score", 20), then: "low" },
    { case: $.lte("$score", 50), then: "medium" },
    { case: $.lte("$score", 80), then: "high" },
  ],
  default: "excellent",
})
// Type: "low" | "medium" | "high" | "excellent"
```

### Array Transformation Operators

```typescript
// $map — transform each element
$.map({
  input: "$items",
  as: "item",
  in: "$$item.name",            // or in: $ => $.multiply("$$item.price", 2)
})

// $filter — keep matching elements (cond MUST be a callback)
$.filter({
  input: "$items",
  as: "item",
  cond: $ => $.gte("$$item.price", 100),
})

// $reduce — fold array (in MUST be a callback)
$.reduce({
  input: "$items",
  initialValue: 0,
  in: $ => $.add("$$value", "$$this.quantity"),
})
```

### Object Operators

```typescript
// $mergeObjects — merge with null removal and last-wins override
$.mergeObjects("$defaults", "$overrides", { extra: true })
```

## Accumulators (inside $group)

```typescript
$.sum(1)                    // Count documents        → number
$.sum("$quantity")          // Sum field values        → number
$.avg("$price")             // Average                → number | null
$.min("$price")             // Minimum                → number
$.max("$price")             // Maximum                → number
$.first("$value")           // First in group          → field type
$.last("$value")            // Last in group           → field type
$.push("$item")             // Collect all             → field type[]
$.addToSet("$tag")          // Collect unique          → field type[]
$.stdDevPop("$score")       // Population std dev      → number | null
$.stdDevSamp("$score")      // Sample std dev          → number | null
```

## CRUD Operations

All CRUD operations return builder objects. Call `.execute()` for mutations and `.toList()` / `.toOne()` for reads.

### Find

```typescript
// Find all (no filter)
const users = await collection.find().toList();

// Find with filter
const adults = await collection
  .find($ => ({ age: { $gte: 18 } }))
  .toList();

// Find with options
const page = await collection
  .find($ => ({ department: "Engineering" }), {
    sort: { name: 1 },
    limit: 10,
    skip: 5,
  })
  .toList();

// Find one
const user = await collection
  .findOne($ => ({ _id: userId }))
  .toOne();
```

### Insert

```typescript
// Insert one — must match schema exactly
await collection.insertOne({
  _id: "user123",
  name: "John Doe",
  email: "john@example.com",
}).execute();

// Insert many
await collection.insertMany([
  { _id: "user1", name: "Alice", score: 10, tags: [], active: true },
  { _id: "user2", name: "Bob", score: 20, tags: ["a"], active: false },
]).execute();
```

### Update

```typescript
// Update one
await collection.updateOne(
  $ => ({ _id: "user123" }),
  { $set: { name: "John Smith" }, $inc: { age: 1 } },
).execute();

// Update many
await collection.updateMany(
  $ => ({ department: "Engineering" }),
  { $set: { building: "A" } },
).execute();
```

### Replace

```typescript
await collection.replaceOne(
  $ => ({ _id: "user123" }),
  { _id: "user123", name: "John Doe", department: "Sales" },
).execute();
```

### Delete

```typescript
await collection.deleteOne($ => ({ _id: "user123" })).execute();
await collection.deleteMany($ => ({ status: "inactive" })).execute();
```

### Find and Modify

```typescript
// Find one and update — returns the document or null
const user = await collection.findOneAndUpdate(
  $ => ({ _id: "user123" }),
  { $set: { lastLogin: new Date() } },
).execute();

// Find one and replace
const old = await collection.findOneAndReplace(
  $ => ({ _id: "user123" }),
  { _id: "user123", name: "New Name", age: 25 },
).execute();

// Find one and delete
const deleted = await collection.findOneAndDelete(
  $ => ({ _id: "user123" }),
).execute();
```

### Count and Distinct

```typescript
// Count all documents
const total = await collection.countDocuments().execute();

// Count with filter
const active = await collection.countDocuments($ => ({ active: true })).execute();

// Estimated count (faster, no filter)
const estimated = await collection.estimatedDocumentCount().execute();

// Distinct values — returns typed array
const names = await collection.distinct("name").execute();
// Type: string[]

const names2 = await collection.distinct("name", $ => ({ active: true })).execute();
```

### Bulk Write

```typescript
await collection.bulkWrite([
  { insertOne: { document: { _id: id1, name: "A", count: 1, active: true } } },
  { updateOne: { filter: { name: "X" }, update: { $set: { count: 100 } } } },
  { updateMany: { filter: { active: true }, update: { $inc: { count: 5 } } } },
  { deleteOne: { filter: { _id: id2 } } },
  { replaceOne: { filter: { _id: id3 }, replacement: { _id: id3, name: "Z", count: 0, active: false } } },
]).execute();

// Ordered execution
await collection.bulkWrite([
  { insertOne: { document: { _id: id, name: "seq", count: 1, active: true } } },
  { updateOne: { filter: { _id: id }, update: { $set: { count: 42 } } } },
]).execute({ ordered: true });
```

## Aggregation Result Methods

### .toList()

Returns all documents as an array.

```typescript
const users = await collection
  .aggregate($match($ => ({ age: { $gte: 18 } })))
  .toList();
```

### .toOne()

Returns the first document or null.

```typescript
const user = await collection
  .aggregate($match($ => ({ _id: "user123" })))
  .toOne();
```

## Effect Integration

### Effect Registry

```typescript
import { registryEffect } from "sluice-orm";
import { Effect } from "effect";

const program = Effect.gen(function* () {
  const registry = yield* registryEffect("8.0", { users: UserSchema });

  const users = yield* registry.users.find().toList();

  yield* registry.users
    .updateOne(() => ({ _id: "1" }), { $set: { age: 31 } })
    .execute();

  const deleted = yield* registry.users
    .deleteOne(() => ({ _id: "1" }))
    .execute();

  return users;
});
```

All Effect CRUD operations return `Effect<T, MongoError>`.

## TypeScript Configuration

Recommended `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "strict": true,
    "exactOptionalPropertyTypes": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true,
    "noUncheckedIndexedAccess": true
  }
}
```
