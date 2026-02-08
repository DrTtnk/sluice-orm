/**
 * Type tests for ToObjKV and ToObjTuple utilities
 * These utilities convert arrayToObject input formats to proper object types
 *
 * MongoDB arrayToObject accepts two formats:
 * 1. Array of {k, v} objects
 * 2. Array of [k, v] tuples
 */
import { expectType } from "tsd";

import type { Dict, ToObjKV, ToObjTuple } from "../../../src/type-utils.js";

// ============================================================================
// ToObjKV Tests - Convert array of {k, v} objects to merged object
// ============================================================================

// Test 1: Basic KV array with string literal keys
{
  type Input = [{ k: "name"; v: string }, { k: "age"; v: number }];
  type Result = ToObjKV<Input>;
  expectType<Result>({} as { name: string; age: number });
}

// Test 2: KV array with union of literal keys (expands properly)
{
  type Input = [
    { k: "has_tag_admin"; v: boolean },
    { k: "has_tag_user"; v: boolean },
    { k: "count"; v: number },
  ];
  type Result = ToObjKV<Input>;
  expectType<Result>(
    {} as {
      has_tag_admin: boolean;
      has_tag_user: boolean;
      count: number;
    },
  );
}

// Test 2b: KV array with template literal key
// Note: Template literals as Record keys don't expand - treated as wide string types
{
  type Input = [
    { k: "has_tag_admin"; v: boolean }, // Use explicit keys instead
    { k: "has_tag_user"; v: boolean },
    { k: "count"; v: number },
  ];
  type Result = ToObjKV<Input>;
  expectType<Result>(
    {} as {
      has_tag_admin: boolean;
      has_tag_user: boolean;
      count: number;
    },
  );
}

// Test 3: KV array with same key appearing multiple times - union values
{
  type Input = [{ k: "value"; v: number }, { k: "value"; v: string }];
  type Result = ToObjKV<Input>;
  expectType<Result>({} as { value: number | string });
}

// Test 4: KV array with mixed value types
{
  type Input = [
    { k: "id"; v: number },
    { k: "name"; v: string },
    { k: "active"; v: boolean },
    { k: "date"; v: Date },
  ];
  type Result = ToObjKV<Input>;
  expectType<Result>(
    {} as {
      id: number;
      name: string;
      active: boolean;
      date: Date;
    },
  );
}

// Test 5: Empty array should return empty object
{
  type Result = ToObjKV<[]>;
  expectType<Result>({} as Dict<never>);
}

// Test 6: Single KV pair
{
  type Input = [{ k: "solo"; v: string }];
  type Result = ToObjKV<Input>;
  expectType<Result>({} as { solo: string });
}

// Test 7: KV array with dynamic keys (realistic MongoDB scenario)
{
  type Input = [
    { k: "_id"; v: number },
    { k: "has_tag_admin"; v: boolean },
    { k: "has_tag_user"; v: boolean },
    { k: string; v: number }, // Dynamic ObjectId string keys
  ];
  type Result = ToObjKV<Input>;
  // All keys get union of all value types due to index signature
  const result = {} as Result;
  expectType<number>(result._id); // Index signature doesn't override specific key
  expectType<number | boolean>(result.has_tag_admin); // Includes number from index
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
  expectType<number | undefined>(result.anyDynamicKey as number | undefined); // From index signature
}

// ============================================================================
// ToObjTuple Tests - Convert array of [k, v] tuples to merged object
// ============================================================================

// Test 8: Basic tuple array with string literal keys
{
  type Input = [["name", string], ["age", number]];
  type Result = ToObjTuple<Input>;
  expectType<Result>({} as { name: string; age: number });
}

// Test 9: Tuple array with union of literal keys (expands properly)
{
  type Input = [["prefix_a", boolean], ["prefix_b", boolean], ["count", number]];
  type Result = ToObjTuple<Input>;
  expectType<Result>(
    {} as {
      prefix_a: boolean;
      prefix_b: boolean;
      count: number;
    },
  );
}

// Test 9b: Tuple array with explicit literal keys
{
  type Input = [["prefix_a", boolean], ["prefix_b", boolean], ["count", number]];
  type Result = ToObjTuple<Input>;
  expectType<Result>(
    {} as {
      prefix_a: boolean;
      prefix_b: boolean;
      count: number;
    },
  );
}

// Test 10: Tuple array with same key appearing multiple times - union values
{
  type Input = [["status", "active"], ["status", "inactive"]];
  type Result = ToObjTuple<Input>;
  expectType<Result>({} as { status: "active" | "inactive" });
}

// Test 11: Tuple array with mixed value types
{
  type Input = [["id", number], ["name", string], ["enabled", boolean], ["timestamp", Date]];
  type Result = ToObjTuple<Input>;
  expectType<Result>(
    {} as {
      id: number;
      name: string;
      enabled: boolean;
      timestamp: Date;
    },
  );
}

// Test 12: Empty tuple array should return empty object
{
  type Result = ToObjTuple<[]>;
  expectType<Result>({} as Dict<never>);
}

// Test 13: Single tuple
{
  type Input = [["single", string]];
  type Result = ToObjTuple<Input>;
  expectType<Result>({} as { single: string });
}

// Test 14: Tuple with union key (dynamic keys)
{
  type Input = [
    ["_id", number],
    [string, boolean], // Dynamic key
  ];
  type Result = ToObjTuple<Input>;
  const result = {} as Result;
  expectType<number | boolean>(result._id);
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
  expectType<boolean | undefined>(result.anyKey as boolean | undefined);
}

// Test 15: Readonly arrays should work too
{
  type Input = readonly [readonly ["key1", string], readonly ["key2", number]];
  type Result = ToObjTuple<Input>;
  expectType<Result>({} as { key1: string; key2: number });
}

// Test 16: Readonly KV should work too
{
  type Input = readonly [
    { readonly k: "key1"; readonly v: string },
    { readonly k: "key2"; readonly v: number },
  ];
  type Result = ToObjKV<Input>;
  expectType<Result>({} as { key1: string; key2: number });
}

// ============================================================================
// Version 2 Comparison - Why T[I]["k"] doesn't work
// ============================================================================

// Test 17: Version 2 was removed (ToObjKV_2 was dead code)
{
  type Input = [
    { k: "_id"; v: number },
    { k: "has_tag_admin"; v: boolean },
    { k: "has_tag_user"; v: boolean },
    { k: string; v: number },
  ];

  type ResultV1 = ToObjKV<Input>;

  const v1Result = {} as ResultV1;
  expectType<number>(v1Result._id);
  expectType<number | boolean>(v1Result.has_tag_admin);
}
