/* eslint-disable @typescript-eslint/no-empty-object-type */
/**
 * Strict typing for MongoDB update operations
 * Ensures type safety for update specs, array filters, and positional operators
 */

import type { Simplify, UnionToTuple, ValueOf } from "type-fest";

import type { PathType, ResolvePath, ResolveUpdatePath, UpdatePathType } from "../../paths.js";
import type { OpaqueError } from "../../type-errors.js";
import type { Dict } from "../../type-utils.js";
import type {
  AddToSetSpec,
  BitSpec,
  CurrentDateSpec,
  IncSpec,
  MinMaxSpec,
  MulSpec,
  PopSpec,
  PullAllSpec,
  PullSpec,
  PushSpec,
  RenameSpec,
  SetSpec,
  UnsetSpec,
} from "./operators.js";
import type {
  ExtractRequiredIdentifiers,
  RequiresArrayFilters,
  ValidatePathConflicts,
} from "./validation.js";

// Re-export for backward compatibility
export type { RequiresArrayFilters };
export type ExtractArrayFilterIdentifiers<Spec> = ExtractRequiredIdentifiers<Spec>;

// ==========================================
// Core Update Spec Type
// ==========================================

/**
 * StrictUpdateSpec - Type-safe MongoDB update document
 * Each operator validates paths and values against the document schema
 */
export type StrictUpdateSpec<T> = {
  $set?: SetSpec<T>;
  $setOnInsert?: SetSpec<T>;
  $unset?: UnsetSpec<T>;
  $inc?: IncSpec<T>;
  $mul?: MulSpec<T>;
  $min?: MinMaxSpec<T>;
  $max?: MinMaxSpec<T>;
  $rename?: RenameSpec<T>;
  $currentDate?: CurrentDateSpec<T>;
  $push?: PushSpec<T>;
  $addToSet?: AddToSetSpec<T>;
  $pull?: PullSpec<T>;
  $pop?: PopSpec<T>;
  $pullAll?: PullAllSpec<T>;
  $bit?: BitSpec<T>;
};

// Simplified validation: Just check that all keys in Spec exist in Expected
type ValidateOperatorSpec<Spec, Expected> = {
  [P in keyof Spec]: P extends keyof Expected ? Spec[P] : never;
};

export type ValidateUpdateSpec<T, Spec extends StrictUpdateSpec<T>> = {
  [K in keyof Spec]: K extends "$set" ? ValidateOperatorSpec<Spec[K], SetSpec<T>>
  : K extends "$setOnInsert" ? ValidateOperatorSpec<Spec[K], SetSpec<T>>
  : K extends "$unset" ? ValidateOperatorSpec<Spec[K], UnsetSpec<T>>
  : K extends "$inc" ? ValidateOperatorSpec<Spec[K], IncSpec<T>>
  : K extends "$mul" ? ValidateOperatorSpec<Spec[K], MulSpec<T>>
  : K extends "$min" ? ValidateOperatorSpec<Spec[K], MinMaxSpec<T>>
  : K extends "$max" ? ValidateOperatorSpec<Spec[K], MinMaxSpec<T>>
  : K extends "$rename" ? ValidateOperatorSpec<Spec[K], RenameSpec<T>>
  : K extends "$currentDate" ? ValidateOperatorSpec<Spec[K], CurrentDateSpec<T>>
  : K extends "$push" ? ValidateOperatorSpec<Spec[K], PushSpec<T>>
  : K extends "$addToSet" ? ValidateOperatorSpec<Spec[K], AddToSetSpec<T>>
  : K extends "$pull" ? ValidateOperatorSpec<Spec[K], PullSpec<T>>
  : K extends "$pop" ? ValidateOperatorSpec<Spec[K], PopSpec<T>>
  : K extends "$pullAll" ? ValidateOperatorSpec<Spec[K], PullAllSpec<T>>
  : K extends "$bit" ? ValidateOperatorSpec<Spec[K], BitSpec<T>>
  : never;
} & (ValidatePathConflicts<ExtractPaths<Spec>> extends infer Conflict ?
  Conflict extends OpaqueError<string> ?
    Conflict
  : unknown
: unknown);

type RecordToTupleType<T extends Dict<any>> = UnionToTuple<ValueOf<{ [K in keyof T]: [K, T[K]] }>>;

type FlattenPaths<T extends readonly any[], Acc extends readonly string[] = readonly []> =
  T extends readonly [readonly [any, infer Paths extends readonly string[]], ...infer Rest] ?
    FlattenPaths<Rest, readonly [...Acc, ...Paths]>
  : Acc;

type ExtractPaths<UpdateSpec extends Dict<any>> = FlattenPaths<
  RecordToTupleType<{ [Op in keyof UpdateSpec]: UnionToTuple<keyof UpdateSpec[Op] & string> }>
>;

type Alpha<T extends string> = Lowercase<T> extends Uppercase<T> ? never : T;
type Digit<T extends string> = T extends `${0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9}` ? T : never;

type MatchAlphanumeric<T extends string> =
  T extends `${infer F}${infer R}` ?
    Alpha<F> extends never ?
      Digit<F> extends never ?
        never
      : MatchAlphanumeric<R>
    : MatchAlphanumeric<R>
  : T;

type ValidIdentifier<T extends string> = T extends "" ? T : MatchAlphanumeric<T>;

type UnionToIntersection<U> =
  (U extends unknown ? (value: U) => void : never) extends (value: infer I) => void ? I : never;

type ArrayIndexPath<Elem> =
  Elem extends readonly (infer E)[] ? `${number}` | `${number}.${PathType<E>}` : never;

type ArrayFilterKey<Elem, Id extends string> =
  Elem extends Dict<unknown> ? Id | `${Id}.${PathType<Elem>}`
  : Elem extends readonly unknown[] ? Id | `${Id}.${ArrayIndexPath<Elem>}`
  : Id;

type ArrayFilterOperators<T> = {
  $eq?: T;
  $ne?: T;
  $gt?: T extends number | Date | string ? T : never;
  $gte?: T extends number | Date | string ? T : never;
  $lt?: T extends number | Date | string ? T : never;
  $lte?: T extends number | Date | string ? T : never;
  $in?: readonly T[];
  $nin?: readonly T[];
  $regex?: T extends string ? string | RegExp : never;
  $exists?: boolean;
  $type?: string | number;
  $size?: T extends readonly unknown[] ? number : never;
};

type ArrayFilterValue<Elem, Id extends string, K extends string> =
  K extends `${Id}.${infer P}` ?
    P extends PathType<Elem> ?
      ResolvePath<Elem, P> | ArrayFilterOperators<ResolvePath<Elem, P>>
    : never
  : Elem | ArrayFilterOperators<Elem>;

type ArrayFilterSpecForId<Elem, Id extends string> = {
  [K in ArrayFilterKey<Elem, Id>]?: ArrayFilterValue<Elem, Id, K>;
};

// ==========================================
// Update Options with Array Filters
// ==========================================

/**
 * Extract array filter ID to path mapping recursively
 * e.g., "tags.$[i]" => { i: "tags.$[i]" }
 *       "items.$[i].$[j]" => { i: "items.$[i]", j: "items.$[i].$[j]" }
 */
type ExtractArrayFilterIdPaths<
  P extends string,
  Prefix extends string = "",
  Acc extends Dict<string> = {},
> =
  P extends `${infer Before}.$[${infer Id}]${infer After}` ?
    Id extends "" ?
      ExtractArrayFilterIdPaths<After, `${Prefix}${Before}.$[]`, Acc>
    : ExtractArrayFilterIdPaths<
        After,
        `${Prefix}${Before}.$[${Id}]`,
        Acc & { [K in Id]: `${Prefix}${Before}.$[${Id}]` }
      >
  : Acc;

/**
 * Extract all update paths from a StrictUpdateSpec
 */
type ExtractPathsFromUpdateSpec<T, Spec extends StrictUpdateSpec<T>> = {
  [K in keyof Spec]: K extends `$${string}` ?
    Spec[K] extends object ?
      keyof Spec[K] & string
    : never
  : never;
}[keyof Spec] &
  UpdatePathType<T>;

/**
 * Convert ID->Path map to ID->Type map
 * Handles union of paths by merging all ID->Type mappings via UnionToIntersection
 */
type ArrayFilterTypes<C, P extends UpdatePathType<C>> = Simplify<
  UnionToIntersection<
    P extends infer Path extends string ?
      ExtractArrayFilterIdPaths<Path> extends infer M ?
        { [Id in keyof M]: ResolveUpdatePath<C, M[Id] & UpdatePathType<C>> }
      : never
    : never
  >
>;

/**
 * Infer array filter types from update spec
 */
export type InferArrayFilters<T, Spec extends StrictUpdateSpec<T>> = ArrayFilterTypes<
  T,
  ExtractPathsFromUpdateSpec<T, Spec>
>;

/**
 * Required array filters when positional operators are used
 */
type RequiredArrayFilters<T, Spec extends StrictUpdateSpec<T>> =
  ExtractRequiredIdentifiers<Spec> extends never ? {}
  : {
      arrayFilters: InferArrayFilters<T, Spec> extends infer AF extends Dict<unknown> ?
        keyof AF extends never ?
          never
        : readonly Partial<
            { [Id in keyof AF]: ArrayFilterSpecForId<AF[Id], Id & string> }[keyof AF]
          >[]
      : never;
    };

/**
 * UpdateOptions - Options for update operations with type-safe arrayFilters
 */
export type UpdateOptions<T, Spec extends StrictUpdateSpec<T>> = {
  upsert?: boolean;
  hint?: string | Dict<1 | -1>;
} & RequiredArrayFilters<T, Spec>;
