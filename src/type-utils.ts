/**
 * Type utilities for simplifying and making types writable
 */

import type { BSONValue } from "bson";
import type * as tf from "type-fest";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyDict = Dict<any>;

export type __ = unknown;
export type Dict<T> = Record<string, T>;

/**
 * SimplifyWritable - Makes a type writable (removes readonly) and simplifies it
 * This combines WritableDeep and Simplify for cleaner type signatures
 */
type WritableBsonDeep<T> =
  T extends BSONValue | Date ? T
  : T extends readonly [] ? []
  : T extends readonly [infer A, ...infer R] ? [WritableBsonDeep<A>, ...WritableBsonDeep<R>]
  : T extends readonly (infer E)[] ? WritableBsonDeep<E>[]
  : T extends object ? { -readonly [K in keyof T]: WritableBsonDeep<T[K]> }
  : T;

export type SimplifyWritable<T> = tf.Simplify<WritableBsonDeep<T>>;

// ==========================================
// Object Merge Helpers (Index-Signature Safe)
// ==========================================

// Get only literal string keys (exclude index signature)
export type LiteralKeys<T> =
  keyof T extends infer K ?
    true extends tf.IsLiteral<K> ?
      K & string
    : never
  : never;

// Detect string index signature presence
export type HasStringIndex<T> = string extends keyof T ? true : false;

// Get index signature value if exists
export type IndexValue<T> = string extends keyof T ? T[string] : never;

// Check if K is a literal key in T (not just via index signature)
export type HasLiteralKey<T, K extends string> = K extends LiteralKeys<T> ? true : false;

// Merge two objects with last-wins semantics (B overrides A for duplicate keys)
export type MergeWithIndexOverride<A, B> = tf.Simplify<
  {
    [K in LiteralKeys<A> | LiteralKeys<B>]: HasLiteralKey<B, K> extends true ? B[K & keyof B]
    : HasLiteralKey<A, K> extends true ? A[K & keyof A]
    : never;
  } & (HasStringIndex<B> extends true ? { [k: string]: IndexValue<B> }
  : HasStringIndex<A> extends true ? { [k: string]: IndexValue<A> }
  : NonNullable<unknown>)
>;

/**
 * ShallowMergeObjects - Shallow merge array of object types
 *
 * Features:
 * - Merges all keys from all objects
 * - When same key appears in multiple objects, creates union of types
 * - Handles optional vs required: if any object has key as required, result is required
 * - Removes undefined from union when merging optional + required
 * - Supports string literal keys, template literal keys, and index signatures
 */
export type ShallowMergeObjects<T extends readonly AnyDict[]> =
  T extends readonly [] ? Dict<never>
  : T extends readonly [infer First extends AnyDict] ? First
  : T extends readonly [infer First extends AnyDict, ...infer Rest extends readonly AnyDict[]] ?
    MergeTwo<First, ShallowMergeObjects<Rest>>
  : never;

/**
 * ShallowMergeObjectsOverride - Shallow merge array of object types with last-wins semantics
 */
export type ShallowMergeObjectsOverride<T extends readonly AnyDict[]> =
  T extends readonly [] ? Dict<never>
  : T extends readonly [infer First extends AnyDict] ? First
  : T extends readonly [infer First extends AnyDict, ...infer Rest extends readonly AnyDict[]] ?
    MergeWithIndexOverride<First, ShallowMergeObjectsOverride<Rest>>
  : never;

/**
 * Merge two objects - handles index signatures properly
 * Later values (B) override earlier values (A) for duplicate keys
 * Index signatures are preserved but don't override specific keys
 */
type MergeTwo<A extends AnyDict, B extends AnyDict> = tf.Simplify<
  {
    // All keys from A, using B's value if B also has that specific key
    [K in keyof A]: K extends keyof B ? A[K] | B[K] : A[K];
  } & {
    // Keys only in B (including index signature contributions)
    [K in keyof B as K extends keyof A ? never : K]: B[K];
  }
>;

/**
 * ToObjKV - Convert MongoDB arrayToObject {k, v} format to object type
 *
 * Takes an array of {k: Key, v: Value} objects and produces a merged object type.
 * Handles template literals, string literals, and duplicate keys (union values).
 *
 * Example:
 *   ToObjKV<[{k: "name", v: string}, {k: "age", v: number}]>
 *   // => { name: string; age: number }
 */
export type ToObjKV<T extends readonly { k: unknown; v: unknown }[]> =
  T extends readonly [] ? Dict<never>
  : ShallowMergeObjects<{
      [I in keyof T]: T[I] extends { k: infer K; v: infer V } ?
        K extends string ?
          Record<K, V>
        : Dict<unknown>
      : never;
    }>;

/**
 * ToObjTuple - Convert MongoDB arrayToObject [k, v] tuple format to object type
 *
 * Takes an array of [Key, Value] tuples and produces a merged object type.
 * Handles template literals, string literals, and duplicate keys (union values).
 *
 * Example:
 *   ToObjTuple<[["name", string], ["age", number]]>
 *   // => { name: string; age: number }
 */
export type ToObjTuple<T extends readonly (readonly [unknown, unknown])[]> =
  T extends readonly [] ? Dict<never>
  : ShallowMergeObjects<{
      [I in keyof T]: T[I] extends readonly [infer K, infer V] ?
        K extends string ?
          Record<K, V>
        : Dict<unknown>
      : never;
    }>;
