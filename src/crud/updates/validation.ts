/**
 * Validation types for MongoDB update operations
 * Provides three categories of compile-time errors:
 * 1. Invalid path (including $, $[], $[id] syntax)
 * 2. Invalid value type for the path
 * 3. Invalid or missing arrayFilters
 */

import type { IsPrefix, NormalizeArrayFilterPath } from "../../paths.js";
import type { OpaqueError } from "../../type-errors.js";

// ==========================================
// ArrayFilters Validation
// ==========================================

/**
 * Extract all $[identifier] from a path string
 */
type ExtractIdentifier<P extends string> =
  P extends `${string}$[${infer Id}]${infer Rest}` ?
    Id extends "" ?
      ExtractIdentifier<Rest>
    : Id | ExtractIdentifier<Rest>
  : never;

/**
 * Extract all identifiers from all paths in an operator spec
 */
type ExtractIdentifiersFromSpec<Spec extends object> = {
  [K in keyof Spec]: K extends string ? ExtractIdentifier<K> : never;
}[keyof Spec];

/**
 * Extract all identifiers from entire update spec
 */
export type ExtractRequiredIdentifiers<UpdateSpec> =
  UpdateSpec extends object ?
    {
      [Op in keyof UpdateSpec]: Op extends `$${string}` ?
        UpdateSpec[Op] extends object ?
          ExtractIdentifiersFromSpec<UpdateSpec[Op]>
        : never
      : never;
    }[keyof UpdateSpec]
  : never;

/**
 * Check if update spec requires arrayFilters
 */
export type RequiresArrayFilters<UpdateSpec> =
  ExtractRequiredIdentifiers<UpdateSpec> extends never ? false : true;

// ==========================================
// Update Path Conflict Validation
// ==========================================

type NormalizePaths<Paths extends readonly string[]> =
  Paths extends readonly [infer Head extends string, ...infer Tail extends readonly string[]] ?
    readonly [NormalizeArrayFilterPath<Head>, ...NormalizePaths<Tail>]
  : readonly [];

type HasDuplicates<T extends readonly string[]> =
  T extends readonly [infer Head extends string, ...infer Tail extends readonly string[]] ?
    Head extends Tail[number] ?
      true
    : HasDuplicates<Tail>
  : false;

type HasPathConflict<T extends readonly string[], U extends T[number] = T[number]> =
  HasDuplicates<T> extends true ? true
  : (U extends string ? IsPrefix<U, Exclude<T[number], U>> : never) extends infer Result ?
    [Result] extends [never] ?
      false
    : true // If Result is anything other than never (including true or a union with true), there's a conflict
  : never;

export type ValidatePathConflicts<Paths extends readonly string[]> =
  [HasPathConflict<NormalizePaths<Paths>>] extends [true] ?
    OpaqueError<"Conflicting update paths (duplicate or parent/child)">
  : Paths;

/**
 * Validate arrayFilters object structure
 * Each identifier must:
 * 1. Exist in arrayFilters
 * 2. Be a valid MongoDB query filter for the element type
 */
export type ValidateArrayFilters<T, UpdateSpec, ArrayFilters> =
  ExtractRequiredIdentifiers<UpdateSpec> extends infer RequiredIds extends string ?
    RequiredIds extends never ?
      // No identifiers required
      ArrayFilters extends undefined ?
        true
      : true // Allow arrayFilters even if not required
    : // Identifiers required
    ArrayFilters extends undefined ?
      OpaqueError<`Missing arrayFilters for identifiers: ${RequiredIds}`>
    : ArrayFilters extends readonly object[] ?
      // Check each required identifier exists
      RequiredIds extends string ?
        HasIdentifierKey<ArrayFilters, RequiredIds> extends true ?
          true
        : OpaqueError<`Missing arrayFilter for identifier: ${RequiredIds}`>
      : true
    : OpaqueError<"arrayFilters must be an array of filter objects">
  : never;

/**
 * Check if any object in the array has a key matching the identifier pattern
 * Accepts both "identifier" and "identifier.field" forms
 */
type HasIdentifierKey<Arr extends readonly object[], Id extends string> =
  Arr extends readonly [infer First, ...infer Rest extends readonly object[]] ?
    First extends object ?
      keyof First extends infer Keys ?
        // Check if any key starts with the identifier
        Keys extends `${Id}.${string}` ? true
        : Keys extends Id ? true
        : Rest extends readonly object[] ? HasIdentifierKey<Rest, Id>
        : false
      : Rest extends readonly object[] ? HasIdentifierKey<Rest, Id>
      : false
    : Rest extends readonly object[] ? HasIdentifierKey<Rest, Id>
    : false
  : false;

// ==========================================
// Positional Path Validation
// ==========================================

export type { ValidPositionalPath } from "../../paths.js";
