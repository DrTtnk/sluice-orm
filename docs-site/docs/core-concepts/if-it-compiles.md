---
sidebar_position: 4
---

# If It Compiles, It Works

Sluice is designed around a simple principle: **make the type system do the work so you don't have to debug at runtime**.

MongoDB's aggregation framework is powerful but unforgiving. A typo in a field name, a wrong operator argument, or a mismatched pipeline output won't throw an error — MongoDB will silently produce wrong results. Sluice moves these failure modes to compile time.

## What the Compiler Prevents

### Wrong field references

MongoDB silently returns `null` for nonexistent fields. Sluice catches them at compile time:

```typescript
// In raw MongoDB:
{ $group: { _id: "$departmnet", count: { $sum: 1 } } }
//                  ^^^^^^^^^^^ typo → _id is always null → one giant group
//                  No error. Wrong results. Good luck debugging.

// In Sluice:
$group($ => ({
  _id: "$departmnet",
  //   ~~~~~~~~~~~~~ Error: not a valid path
  count: $.sum(1),
}));
```

### Type mismatches in operators

`$multiply` on a string doesn't throw — it returns `null`. For every document. Silently:

```typescript
// In raw MongoDB:
{ $multiply: ["$userId", 2] }
// userId is a string → result is null for every document

// In Sluice:
$addFields($ => ({
  doubled: $.multiply("$userId", 2),
  //                  ~~~~~~~~~ Error: expects numeric field ref, got string
}));
```

### Forgotten `$` prefix

One of the most common MongoDB bugs — using `"fieldName"` when you meant `"$fieldName"`:

```typescript
// In raw MongoDB:
{ $group: { _id: "$dept", total: { $sum: "amount" } } }
//                                       ^^^^^^^^ This is the literal string "amount"
//                                       Not the field — every doc gets "amount" summed
//                                       $sum of a string → 0. Silent. Wrong.

// In Sluice:
$group($ => ({
  _id: "$dept",
  total: $.sum("amount"),
  //          ~~~~~~~~ Error: Bare string "amount" is not valid
  //                   in accumulator position — did you mean "$amount"?
}));
```

### Array filter consistency

MongoDB requires `arrayFilters` when you use `$[identifier]` in update paths. Forget them and the update silently does nothing:

```typescript
// In raw MongoDB:
db.orders.updateOne(
  { _id: "123" },
  { $set: { "items.$[elem].price": 29.99 } }
  // Missing arrayFilters → update silently does nothing
);

// In Sluice:
await orders.updateOne(
  () => ({ _id: "123" }),
  { $set: { "items.$[elem].price": 29.99 } },
  // ❌ Compile error: arrayFilters is required when using $[elem]
  //    The options parameter becomes non-optional
);

// ✅ Fixed:
await orders.updateOne(
  () => ({ _id: "123" }),
  { $set: { "items.$[elem].price": 29.99 } },
  { arrayFilters: [{ "elem.name": "Widget" }] },
);
```

### Pipeline output shapes

The aggregation return type is **computed**, not annotated. You can't accidentally annotate a wrong type:

```typescript
const result = await orders
  .aggregate(
    $group($ => ({
      _id: "$status",
      total: $.sum("$amount"),
    })),
  )
  .toList();

// result: { _id: "pending" | "paid" | "shipped"; total: number }[]
// This type is inferred — not a manual annotation you might get wrong
```

### `$inc` on non-numeric fields

```typescript
// In raw MongoDB:
{ $inc: { name: 1 } }
// name is a string → MongoDB throws at runtime

// In Sluice:
await users.updateOne(
  () => ({ _id: "123" }),
  { $inc: { name: 1 } },
  //       ~~~~ Error: name resolves to string, $inc requires numeric
);
```

### `$push` on non-array fields

```typescript
// In raw MongoDB:
{ $push: { age: 5 } }
// age is a number → MongoDB throws at runtime

// In Sluice:
await users.updateOne(
  () => ({ _id: "123" }),
  { $push: { age: 5 } },
  //        ~~~ Error: age resolves to number, $push requires array
);
```

### Conflicting update paths

```typescript
// In raw MongoDB:
{ $set: { "profile": { theme: "dark" }, "profile.avatar": "new.png" } }
// Conflicting paths — MongoDB behavior is undefined

// In Sluice:
await users.updateOne(
  () => ({ _id: "123" }),
  { $set: { profile: { theme: "dark" }, "profile.avatar": "new.png" } },
  // Error: conflicting paths — "profile" and "profile.avatar"
);
```

### Mixed projection modes

```typescript
// In raw MongoDB:
{ $project: { name: 1, age: 0 } }
// MongoDB throws: "Cannot do inclusion on field name in exclusion projection"

// In Sluice:
$project({ name: 1, age: 0 }),
// Error: cannot mix inclusion (1) and exclusion (0) in $project
```

### Wrong `$bucket` boundary order

```typescript
// In raw MongoDB:
{ $bucket: { groupBy: "$age", boundaries: [50, 30, 18] } }
// MongoDB throws at runtime — boundaries must be ascending

// In Sluice:
$bucket({ groupBy: "$age", boundaries: [50, 30, 18] as const }),
// Error: boundaries must be in strictly ascending order
```

## The `any` Firewall

In most TypeScript libraries, a single `any` silently infects the entire type chain. Sluice has a built-in circuit breaker:

```typescript
// If any stage produces `any` (from a loosely typed input),
// the pipeline doesn't silently become untyped.
// Instead, CheckedResult catches it:
// OpaqueError: "Unexpected 'any' type detected"
```

This means a bug in one stage can't silently disable type checking for all subsequent stages.

## Null Honesty

Operators that can produce `null` say so in their return type:

```typescript
$group($ => ({
  _id: "$department",
  avgAge: $.avg("$age"),
  // Type: number | null  ← avg of zero documents is null
}));
```

And `$.ifNull` narrows it away:

```typescript
$addFields($ => ({
  safeAvg: $.ifNull("$avgAge", 0),
  // Type: number  ← null is gone
}));
```

The type system tracks nullability honestly through the pipeline, so you handle it explicitly rather than discovering `null` in production logs.

## What This Doesn't Replace

Types can't catch everything:
- **Logic errors** — `$.gt("$age", 18)` vs `$.gte("$age", 18)` are both valid
- **Data quality** — if `age` contains garbage data, types can't help
- **Performance** — missing indexes won't show up at compile time
- **MongoDB version differences** — some operators are version-specific

Sluice's type safety covers the **structural** correctness of your pipelines. For the rest, you still need tests and monitoring. But the most common class of aggregation bugs — the typos, the wrong types, the missing fields — are gone before you run a single query.
