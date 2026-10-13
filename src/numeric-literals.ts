/**
 * Compile-time checks for number literals with a documented range.
 *
 * Each type is `unknown` when the value is fine, so `T & PositiveInteger<T>` is just `T`.
 * It is an `OpaqueError` when the value is a literal outside the range, so the argument is rejected.
 * A value typed `number` always passes: only a literal shows the mistake.
 */
import type { OpaqueError } from "./type-errors.js";

type Integer<N extends number> =
  `${N}` extends `${string}.${string}` | `${string}e${string}` ? OpaqueError<"The value must be an integer"> : unknown;

/** 1, 2, 3, ... (for example `$limit`, `$sample.size`, `$bucketAuto.buckets`, `n` of `$firstN`) */
export type PositiveInteger<N> =
  N extends number ?
    number extends N ? unknown
    : `${N}` extends `-${string}` | "0" ? OpaqueError<"The value must be a positive integer">
    : Integer<N>
  : unknown;

/** 0, 1, 2, ... (for example `$skip`, `$graphLookup.maxDepth`) */
export type NonNegativeInteger<N> =
  N extends number ?
    number extends N ? unknown
    : `${N}` extends `-${string}` ? OpaqueError<"The value must be a non-negative integer">
    : Integer<N>
  : unknown;

/** ..., -2, -1, 1, 2, ... (for example the step of `$range`) */
export type NonZeroInteger<N> =
  N extends number ?
    number extends N ? unknown
    : `${N}` extends "0" ? OpaqueError<"The value must be a non-zero integer">
    : Integer<N>
  : unknown;

/** Any whole number (for example the operands of `$bitAnd`) */
export type WholeNumber<N> =
  N extends number ?
    number extends N ?
      unknown
    : Integer<N>
  : unknown;
