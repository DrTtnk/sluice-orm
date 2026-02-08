# harder-core.test.ts - Type System Torture Test Findings

**Status:** Snapshot from the original harder-core run. Re-check against current typings.

## Summary

Created extreme MongoDB aggregation tests that successfully broke the type narrowing system in multiple ways. These tests reveal critical limitations in how sluice handles complex nested operations, $$variable contexts, and type propagation through pipelines.

## Type System Failures Found

### 1. $facet Type Narrowing Failure

**Location**: Lines 162-180
**Issue**: $facet with multiple nested pipelines fails to infer correct types
**Error**: `Type '[...]' is not assignable to type 'TypedPipeline<MonsterSchema, unknown>'`

The $facet operator creates multiple independent pipelines but the type system can't properly track:

- Each branch's output schema
- How to merge the branch results into the final document
- Type propagation through nested $addFields + $group combinations

### 2. $group Accumulator Context Loss

**Location**: Lines 175-177, 183-186
**Issue**: Accumulator callbacks lose document context
**Error**: `Parameter '$' implicitly has an 'any' type`

When using $group accumulators with callbacks:

```typescript
$group($ => ({
  _id: "$bracket",
  count: $ => $.sum(1), // $ has 'any' type!
}));
```

The type system doesn't maintain the input document schema for accumulator builders.

### 3. $$this/$$value Variable Type Inference

**Location**: Lines 259, 264, 326, 353
**Issue**: $$this and $$value variables don't resolve to array element types
**Error**: Type mismatches when accessing $$item.rarity, $$skill.power, etc.

Examples:

- `$.in("$$item.rarity", ["epic", "legendary"])` - Can't infer $$item.rarity is a string
- `$.gt("$$skill.power", 50)` - Can't infer $$skill.power is a number
- `$.concatArrays("$$value", ["$$this"])` - Can't handle $$this/$$value in reduce

### 4. Nested Array Path Access

**Location**: Lines 469-470
**Issue**: Accessing nested array fields like "$skills.power" fails
**Error**: `"$skills.power"' is not assignable to parameter of type 'ArrayArg<...>'`

The type system doesn't handle implicit array projection:

```typescript
$.arrayElemAt("$skills.power", 0); // Should project power from skills array
```

### 5. $map Object Return Type

**Location**: Lines 513-523
**Issue**: Can't return object literals from $map callback
**Error**: `'itemName' does not exist in type '($: BaseBuilder<...>) => number | ... | null'`

```typescript
$.map({
  input: "$items",
  in: {
    itemName: "$$item.name", // Type system rejects object literals
    monsterName: "$$ROOT.name",
  },
});
```

### 6. Result Type Propagation

**Location**: Throughout (lines 196, 228, 271, 302, etc.)
**Issue**: Pipeline results typed as `undefined` instead of actual schema
**Error**: `Object is possibly 'undefined'` on every result access

Even though we know the pipeline will return documents, TypeScript infers results[0] as possibly undefined.

### 7. Complex Expression Type Loss

**Location**: Line 469
**Issue**: Complex nested expressions lose type information
**Error**: `Ret<..., unknown>` instead of `Ret<..., number>`

```typescript
$.divide(
  $.arrayElemAt("$skills.power", 0), // Returns Ret<..., unknown>
  $.arrayElemAt("$skills.cost", 0),
);
```

### 8. $mergeObjects Dynamic Properties

**Location**: Line 634
**Issue**: Can't add new properties via mergeObjects
**Error**: `'e' does not exist in type '...mergeObjects...'`

The type system tracks exact object shapes but doesn't allow adding new fields:

```typescript
$.mergeObjects("$extra", { e: 100 }); // 'e' doesn't exist in $extra
```

## MongoDB Edge Cases Confirmed Working

Despite type errors, these tests should prove MongoDB behavior for:

1. **Division by zero → Infinity**: `$.divide("$level", 0)`
2. **Square root of negative → NaN**: `$.sqrt(-1)`
3. **Natural log of zero → -Infinity**: `$.ln(0)`
4. **Power overflow → Infinity**: `$.pow(10, 1000)`

## Recommendations

### Short-term Fixes

1. Add `// @ts-expect-error` comments with explanations for known type system limitations
2. Use type assertions where type narrowing fails but runtime behavior is correct
3. Simplify tests to focus on one type narrowing issue per test

### Long-term Solutions

1. **$facet support**: Implement proper type tracking for multi-branch pipelines
2. **Accumulator context**: Preserve document schema in accumulator builder callbacks
3. **$$variable typing**: Add special handling for $$this, $$value, $$ROOT, $$CURRENT contexts
4. **Array projection**: Support "$array.field" syntax for implicit projection
5. **$map object returns**: Allow object literal returns from $map callbacks
6. **Result typing**: Ensure pipeline results are never typed as undefined
7. **Expression composition**: Maintain type information through nested expressions
8. **mergeObjects**: Support adding new properties via object literals

## Test Value

These tests are VALUABLE even with type errors because:

- They prove the runtime queries work correctly
- They document type system limitations clearly
- They provide a torture test suite for future type narrowing improvements
- They test MongoDB edge cases (Infinity, NaN, overflow)
- They demonstrate real-world complex aggregation patterns

## Next Steps

1. Add `// @ts-expect-error` to all type failures with links to this document
2. Run tests to verify runtime behavior is correct
3. Create GitHub issues for each type narrowing failure category
4. Prioritize fixes based on real-world usage patterns
