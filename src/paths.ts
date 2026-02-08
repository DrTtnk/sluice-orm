/**
 * Path type utilities for MongoDB dot-notation access
 *
 * This module provides type-safe path utilities for MongoDB operations:
 * - Path<T>: Generate all valid dot-notation paths (using type-fest)
 * - ResolveValue<T, P>: Resolve the type at a given path
 * - PathValueArrayProjection<T, P>: MongoDB array projection semantics
 * - Type-filtered paths (NumericPath, StringPath, etc.)
 * - Positional update paths ($[identifier])
 *
 * Uses type-fest's Paths with BsonStop to handle BSON types as leaf nodes.
 */

import type { BSONValue } from "bson";
import type * as tf from "type-fest";

import type { OpaqueError } from "./type-errors.js";
import type { Dict } from "./type-utils.js";

// ============================================
// Core Helper Types
// ============================================

type BsonGuard<T> =
  // eslint-disable-next-line @typescript-eslint/no-redundant-type-constituents
  T extends Date | BSONValue ? T & never
  : T extends readonly (infer E)[] ? readonly BsonGuard<E>[]
  : { [K in keyof T]: BsonGuard<T[K]> };

// ============================================
// Path Generation (MongoDB Style)
// ============================================

type RawPath<T> = tf.Paths<BsonGuard<T>, { maxRecursionDepth: 10 }> & string;

type ReplaceDotNumber<T, With extends string> =
  T extends `${infer Head}.${infer Rest}` ?
    Rest extends `${number}${infer Tail}` ?
      `${Head}${With}${ReplaceDotNumber<Tail, With>}`
    : `${Head}.${ReplaceDotNumber<Rest, With>}`
  : T;

export type PathType<T> = ReplaceDotNumber<RawPath<T>, "" | `.${number}`>;
export type UpdatePathType<T> = ReplaceDotNumber<
  RawPath<T>,
  `.${number}` | `.$` | `.$[]` | `.$[${string}]`
>;

/**
 * Normalize arrayFilters syntax to positional operator
 * Transforms $[] and $[identifier] to $ for type checking
 */
export type NormalizeArrayFilterPath<T extends string> =
  T extends `${infer Before}.${infer Rest}` ?
    `${NormalizeSegment<Before>}.${NormalizeArrayFilterPath<Rest>}`
  : NormalizeSegment<T>;

type NormalizeSegment<T extends string> =
  // Check if it's a numeric index
  T extends `${number}` ? "$"
  : // Check if it's $[] or $[identifier]
  T extends "$[]" | `$[${string}]` ? "$"
  : T;

// ============================================
// Path Value Resolution
// ============================================

type ArrayIndex = `${number}` | `$` | `$[]` | `$[${string}]`;

type _PathValue<T, P extends string> =
  P extends `${infer Key}.${infer Rest}` ?
    T extends readonly (infer E)[] ?
      Key extends ArrayIndex ?
        _PathValue<E, Rest>
      : _PathValue<E, P>
    : Key extends keyof T ? _PathValue<T[Key], Rest>
    : never
  : T extends readonly (infer E)[] ?
    P extends ArrayIndex ?
      E
    : _PathValue<E, P>
  : P extends keyof T ? T[P]
  : never;

/**
 * ResolveValue<T, P> - Get the value type at a given path P in type T
 *
 * MongoDB-compatible path resolution supporting:
 * - Direct property access: ResolveValue<{name: string}, "name"> = string
 * - Nested access: ResolveValue<{a: {b: string}}, "a.b"> = string
 * - Array access: ResolveValue<{items: Item[]}, "items"> = Item[]
 * - Array element via dot: ResolveValue<{items: {id: string}[]}, "items.id"> = string
 * - Numeric index: ResolveValue<{items: string[]}, "items.0"> = string
 * - Numeric index + nested: ResolveValue<{items: {id: string}[]}, "items.0.id"> = string
 *
 * Returns `never` for invalid paths.
 */
export type ResolveValue<T, P extends string> = _PathValue<T, P>;
export type ResolvePath<T, P extends PathType<T>> =
  P extends PathType<T> ? ResolveValue<T, P> : never;
export type ResolveUpdatePath<T, P extends UpdatePathType<T>> =
  P extends UpdatePathType<T> ? ResolveValue<T, P> : never;

// ============================================
// Type-Filtered Paths
// ============================================

export type FilteredPath<T, Target> = {
  [K in PathType<T>]: [ResolvePath<T, K>] extends [never] ? never
  : ResolvePath<T, K> extends Target ? K
  : never;
}[PathType<T>];

export type UpdateFilteredPath<T, Target> = {
  [K in UpdatePathType<T>]: [ResolveValue<T, K>] extends [never] ? never
  : ResolveValue<T, K> extends Target ? K
  : never;
}[UpdatePathType<T>];

export type NumericPath<T> = FilteredPath<T, number | null | undefined>;
export type StringPath<T> = FilteredPath<T, string | null | undefined>;
export type BooleanPath<T> = FilteredPath<T, boolean | null | undefined>;
export type DatePath<T> = FilteredPath<T, Date | null | undefined>;
export type ArrayPath<T> = FilteredPath<T, readonly unknown[]>;
export type ObjectPath<T> = FilteredPath<T, Dict<unknown>>;
export type ComparablePath<T> = FilteredPath<T, number | string | boolean | Date | BSONValue | null | undefined>;

export type UpdateNumericPath<T> = UpdateFilteredPath<T, number | null | undefined>;
export type UpdateStringPath<T> = UpdateFilteredPath<T, string | null | undefined>;
export type UpdateBooleanPath<T> = UpdateFilteredPath<T, boolean | null | undefined>;
export type UpdateDatePath<T> = UpdateFilteredPath<T, Date | null | undefined>;
export type UpdateArrayPath<T> = UpdateFilteredPath<T, readonly unknown[]>;
export type UpdateObjectPath<T> = UpdateFilteredPath<T, Dict<unknown>>;

// ============================================
// Validated Spec Types - for update operators
// ============================================
// These types validate keys at assignment time instead of enumerating
// This is needed because mapped types can't enumerate all $[${string}] combinations

/**
 * UpdateSpecOfType<T, Target, Value> - A spec type that validates paths resolve to Target
 *
 * Uses the 'as' clause to filter paths during iteration. This works because:
 * - TypeScript's pattern matching allows specific strings like "$[x]" to match "$[${string}]"
 * - The filter happens at assignment time, not enumeration time
 * - Invalid paths (wrong type or doesn't exist) are excluded via `as ... never`
 */
export type UpdateSpecOfType<T, Target, Value> = {
  [K in UpdatePathType<T> as ResolveValue<T, K> extends Target ? ValidPositionalPath<K>
  : never]?: Value;
};

/**
 * Specific validated spec types for common use cases
 */
export type NumericUpdateSpec<T, Value> = UpdateSpecOfType<T, number | null | undefined, Value>;
export type StringUpdateSpec<T, Value> = UpdateSpecOfType<T, string | null | undefined, Value>;
export type DateUpdateSpec<T, Value> = UpdateSpecOfType<T, Date | null | undefined, Value>;
export type ComparableUpdateSpec<T, Value> = UpdateSpecOfType<
  T,
  string | number | Date | null | undefined,
  Value
>;

export type HasPrefix<Target extends string, Union extends string> =
  Union extends unknown ?
    Target extends `${Union}.${string}` ?
      true
    : never
  : never;

type _ArrayRootPath<T extends string, All extends string = T> =
  T extends unknown ?
    true extends HasPrefix<T, Exclude<All, T>> ?
      never
    : T
  : never;

export type IsPrefix<Needle extends string, Union extends string> =
  Union extends unknown ?
    Union extends `${Needle}.${string}` ?
      true
    : never
  : never;

type _ArrayLeafPaths<T extends string, All extends string = T> =
  T extends unknown ?
    true extends IsPrefix<T, Exclude<All, T>> ?
      never
    : T
  : never;

export type ArrayRootPath<T> = _ArrayRootPath<ArrayPath<T>>;
export type ArrayLeafPath<T> = _ArrayLeafPaths<ArrayPath<T>>;

/**
 * ArrayProjectionPath<T> - Paths that resolve to arrays via MongoDB array projection semantics
 * This includes:
 * - Direct array paths (e.g., "tags" where tags is string[])
 * - Projection paths through arrays (e.g., "items.name" where items is {name: string}[] - projects to string[])
 */
export type ArrayProjectionPath<T> = {
  [K in PathType<T>]: PathValueArrayProjection<T, K> extends readonly unknown[] ? K : never;
}[PathType<T>];

export type ResolveArrayPath<T, P extends ArrayPath<T>, V = _PathValue<T, P>> =
  V extends unknown[] ? V : never;

// ============================================
// Array Element Type Extraction
// ============================================

/**
 * ArrayElementType<T, P> - Get the element type of array at path P
 * Uses direct path resolution (ResolvePath).
 * Returns never if the path doesn't resolve to an array
 */
export type ArrayElementType<T, P extends PathType<T>> =
  ResolvePath<T, P> extends readonly (infer E)[] ? E : never;

export type UpdateArrayElementType<T, P extends UpdatePathType<T>> =
  ResolveUpdatePath<T, P> extends readonly (infer E)[] ? E : never;

/**
 * ArrayProjectionElementType<T, P> - Get the element type of array at path P using projection semantics
 * Uses PathValueArrayProjection which handles paths through arrays (e.g., items.name -> string[]).
 * Returns never if the path doesn't project to an array
 */
export type ArrayProjectionElementType<T, P extends PathType<T>> =
  PathValueArrayProjection<T, P> extends readonly (infer E)[] ? E : never;

// ============================================
// Path Value Array Projection (MongoDB Semantics)
// ============================================

/**
 * PathValueArrayProjection<T, P> - Get the value type at path P with MongoDB array projection semantics
 *
 * In MongoDB, when you reference a path through an array like $items.name where items is an array
 * of documents with a name field, it returns an array of all the name values (not a single value).
 *
 * Examples:
 * - PathValueArrayProjection<{info: {name: string}[]}, "info.name"> = string[]
 * - PathValueArrayProjection<{info: {name: string}[]}, "info"> = {name: string}[]
 * - PathValueArrayProjection<{name: string}, "name"> = string
 * - PathValueArrayProjection<{items: {discounts: {code: string}[]}[]}, "items.discounts.code"> = string[][]
 */
export type PathValueArrayProjection<T, P extends PathType<T>> = _PathValueArrayProjection<T, P>;

type _PathValueArrayProjection<T, P extends string> =
  P extends `${infer Key}.${infer Rest}` ?
    T extends readonly (infer E)[] ?
      Key extends `${number}` ?
        _PathValueArrayProjection<E, Rest>
      : _PathValueArrayProjection<E, P>[]
    : Key extends keyof T ? _PathValueArrayProjection<T[Key], Rest>
    : never
  : T extends readonly (infer E)[] ?
    P extends `${number}` ?
      E
    : _PathValueArrayProjection<E, P>[]
  : P extends keyof T ? T[P]
  : never;

// ============================================
// Field Reference Types ($ prefixed paths)
// ============================================

export type FieldRef<T> = `$${PathType<T>}`;
export type NumericFieldRef<T> = `$${NumericPath<T>}`;
export type StringFieldRef<T> = `$${StringPath<T>}`;
export type DateFieldRef<T> = `$${DatePath<T>}`;
export type BooleanFieldRef<T> = `$${BooleanPath<T>}`;
export type ArrayFieldRef<T> = `$${ArrayPath<T>}`;
export type ObjectFieldRef<T> = `$${ObjectPath<T>}`;

// ==========================================
// Positional Path Validation
// ==========================================

/**
 * Validates that positional paths don't contain double $ (.$. pattern) and don't start with $
 * Returns the path if valid, OpaqueError if invalid
 */
export type ValidPositionalPath<P extends string> =
  P extends `$${string}` ? OpaqueError<"Paths cannot start with $">
  : P extends `${string}.$.${infer Rest1}` ?
    Rest1 extends `${string}.$.${string}` ?
      OpaqueError<"Paths cannot contain double $"> // Can remove twice = 2+ occurrences of .$
    : P // Can remove once but not twice = exactly 1 occurrence of .$
  : P; // Cannot remove at all = 0 occurrences of .$
