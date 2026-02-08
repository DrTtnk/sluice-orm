/**
 * Type tests for ShallowMergeObjects utility
 * Tests shallow merging of object types with various edge cases
 */
import { expectType } from "tsd";

import type { Dict, ShallowMergeObjects } from "../../../src/type-utils.js";

// Test 1: Basic merge of two objects
{
  type Result = ShallowMergeObjects<[{ a: number }, { b: string }]>;
  expectType<Result>({} as { a: number; b: string });
}

// Test 2: Merge with overlapping keys - union types
{
  type Result = ShallowMergeObjects<[{ a: number }, { a: string }]>;
  expectType<Result>({} as { a: number | string });
}

// Test 3: Merge with string literal keys
{
  type Result = ShallowMergeObjects<[{ has_tag_admin: boolean }, { has_tag_developer: boolean }]>;
  expectType<Result>({} as { has_tag_admin: boolean; has_tag_developer: boolean });
}

// Test 4: Merge with template literal keys
{
  type Result = ShallowMergeObjects<
    [{ [K in `has_tag_${"admin" | "developer"}`]: boolean }, { other: string }]
  >;
  expectType<Result>(
    {} as {
      has_tag_admin: boolean;
      has_tag_developer: boolean;
      other: string;
    },
  );
}

// Test 5: Merge with Dict<T> (index signature)
{
  type Result = ShallowMergeObjects<[{ _id: number }, Dict<boolean>]>;
  // Should have _id + index signature - _id type should be number | boolean
  const result = {} as Result;
  expectType<number | boolean>(result._id);
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
  expectType<boolean | undefined>(result.someKey as boolean | undefined);
}

// Test 6: Optional vs required keys - union without undefined when both present
{
  type Result = ShallowMergeObjects<[{ a?: number }, { a: string }]>;
  // When merging optional and required, result keeps optional semantics
  expectType<Result>({} as { a?: number | string });
}

// Test 7: Both keys optional - should remain optional with union
{
  type Result = ShallowMergeObjects<[{ a?: number }, { a?: string }]>;
  // When both are optional, result remains optional with union (exact optional semantics)
  expectType<Result>({} as { a?: number | string });
}

// Test 8: Multiple objects merge
{
  type Result = ShallowMergeObjects<[{ a: number }, { b: string }, { c: boolean }]>;
  expectType<Result>({} as { a: number; b: string; c: boolean });
}

// Test 9: Multiple objects with overlapping keys
{
  type Result = ShallowMergeObjects<
    [{ a: number; b: string }, { a: boolean; c: number }, { a: null; d: Date }]
  >;
  expectType<Result>(
    {} as {
      a: number | boolean | null;
      b: string;
      c: number;
      d: Date;
    },
  );
}

// Test 10: Empty array should return never or empty object
{
  type Result = ShallowMergeObjects<[]>;
  expectType<Result>({} as Dict<never>);
}

// Test 11: Single object should return itself
{
  type Result = ShallowMergeObjects<[{ a: number; b: string }]>;
  expectType<Result>({} as { a: number; b: string });
}

// Test 12: Merge with ObjectIdString branded type (realistic scenario)
{
  type ObjectIdString = string & { __brand: "ObjectIdString" };
  type Result = ShallowMergeObjects<
    [
      { _id: number },
      { [K in `has_tag_${"admin" | "developer"}`]: boolean },
      Record<ObjectIdString, number>,
    ]
  >;

  const result = {} as Result;
  expectType<number>(result._id);
  expectType<boolean>(result.has_tag_admin);
  expectType<boolean>(result.has_tag_developer);
}

// Test 13: String literal keys should enable intellisense (string & {})
{
  type Result = ShallowMergeObjects<
    [{ specific_key: number; another_key: string }, { [x: string]: boolean }]
  >;

  const result = {} as Result;
  // Specific key should merge with index signature
  expectType<number | boolean>(result.specific_key);
  expectType<string | boolean>(result.another_key);
  // Index signature should work for any key
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
  expectType<boolean | undefined>(result.any_key as boolean | undefined);
}
