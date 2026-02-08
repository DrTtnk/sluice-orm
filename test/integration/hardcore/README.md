# Hardcore Test Suite

Extreme MongoDB aggregation tests that push sluice to its limits.

## Test Style Guide

All tests in this suite follow a strict pattern to ensure consistency and clarity:

1. **Define Result Schema** - Create an Effect Schema using S.Tuple with exact count
2. **Insert Test Data** - Seed the database with test data using the registry
3. **Execute Raw MongoDB Query** - Run the aggregation with $sort at the end for deterministic ordering
4. **Validate Raw Query** - Call `assertSync` to validate schema compliance
5. **Execute Sluice Query** - Run the equivalent query using sluice's builder (with matching $sort)
6. **Validate Type** - Use `expectType` to verify TypeScript inference
7. **Validate Equivalence** - Use ONE `expect` to verify sluice returns same results as raw query

## Example Pattern

```typescript
it("should do something extreme", async () => {
  // 1. Define Result Schema (use S.Tuple for exact count)
  const ResultSchema = S.Tuple(
    S.Struct({
      _id: ObjectIdSchema,
      computed: S.Number,
    }),
    S.Struct({
      _id: ObjectIdSchema,
      computed: S.Number,
    }),
  );

  // 2. Insert Test Data
  await collection(db).items.insertMany([...]).execute();

  // 3. Execute Raw MongoDB Query (add $sort for deterministic ordering)
  const rawResults = await db.collection("items")
    .aggregate([
      // ... stages ...
      { $sort: { _id: 1 } }  // Always sort for deterministic results
    ])
    .toArray();

  // 4. Validate Raw Query (ONE expect with assertSync)
  assertSync(ResultSchema, rawResults);

  // 5. Execute Sluice Query (with matching $sort)
  const sluiceResults = await collection(db)
    .items.aggregate(
      // ... stages ...
      $sort({ _id: 1 })
    )
    .toList();

  // 6. Validate Type
  assertSync(ResultSchema, sluiceResults);
  expectType<typeof ResultSchema.Type>({} as (typeof sluiceResults)[number]);

  // 7. Validate Equivalence (ONE expect)
  expect(sluiceResults).toEqual(rawResults);
});
```

## Critical Rules

✅ Always use S.Tuple with exact element count (not S.Array)  
✅ Add $sort stage at end of pipeline for deterministic ordering  
✅ Use assertSync for both raw and sluice results  
✅ Use ONE expect for sluice equivalence check  
✅ Avoid type casts - let schemas do the work  
✅ Use the typed `$accumulator` API (arrow functions are supported and converted)

## Anti-Patterns

❌ Using S.Array instead of S.Tuple
❌ Multiple expect statements  
❌ Checking individual properties instead of full objects  
❌ Omitting $sort (causes flaky tests due to random ordering)  
❌ Type casts like `as Record<string, unknown>`  
❌ Un-typed accumulator function strings
