/**
 * Strict type definitions for MongoDB update operators
 * Each operator has precise path and value constraints
 */

import type {
  ArrayRootPath,
  ResolveUpdatePath,
  ResolveValue,
  UpdateArrayElementType,
  UpdateArrayPath,
  UpdatePathType,
  UpdateSpecOfType,
} from "../../paths.js";
import type { Ret } from "../../sluice.js";
import type { ValidPositionalPath } from "./validation.js";

type Nil = null | undefined;

// Helper to accept both raw values and Ret expressions
type ValueOrExpr<C, T> = T | Ret<C, T>;

type EmptyArrayLiteral = readonly [] | never[];

type AllowEmptyArray<T> = [T] extends [readonly unknown[]] ? T | EmptyArrayLiteral : T;

// Helper to get all possible paths (string, number, boolean, Date, arrays)
export type AllPaths<T> = UpdatePathType<T> | ArrayRootPath<T>;

// Helper to filter paths that are valid (no double $)
type ValidPath<T, P extends AllPaths<T>> = P extends string ? ValidPositionalPath<P> : P;

export type SetSpec<T> = {
  [P in AllPaths<T> as ValidPath<T, P>]?: ValueOrExpr<T, AllowEmptyArray<ResolveValue<T, P>>>;
};

export type UnsetSpec<T> = { [P in AllPaths<T> as ValidPath<T, P>]?: "" | 1 | true };

export type IncSpec<T> = UpdateSpecOfType<T, number | Nil, ValueOrExpr<T, number>>;
export type MulSpec<T> = UpdateSpecOfType<T, number | Nil, ValueOrExpr<T, number>>;

export type MinMaxSpec<T> = {
  [P in UpdatePathType<T> as ResolveValue<T, P> extends string | number | Date ? P
  : never]?: ValueOrExpr<T, ResolveUpdatePath<T, P>>;
};

export type RenameSpec<T> = { [P in AllPaths<T>]?: string };

export type CurrentDateSpec<T> = UpdateSpecOfType<
  T,
  Date | Nil,
  true | { $type: "date" | "timestamp" }
>;

type PushModifiers<C, Elem> = {
  $each: readonly ValueOrExpr<C, Elem>[];
  $position?: ValueOrExpr<C, number>;
  $slice?: ValueOrExpr<C, number>;
  $sort?: 1 | -1 | (Elem extends object ? Partial<Record<keyof Elem, 1 | -1>> : never);
};

export type PushSpec<T> = {
  [P in UpdateArrayPath<T>]?:
    | ValueOrExpr<T, UpdateArrayElementType<T, P>>
    | (UpdateArrayElementType<T, P> extends infer Elem ?
        { $each: readonly ValueOrExpr<T, Elem>[] } | Partial<PushModifiers<T, Elem>>
      : never);
};

export type AddToSetSpec<T> = {
  [P in UpdateArrayPath<T>]?:
    | ValueOrExpr<T, UpdateArrayElementType<T, P>>
    | { $each: readonly ValueOrExpr<T, UpdateArrayElementType<T, P>>[] };
};

type PullMatch<T, V> = {
  $eq?: ValueOrExpr<T, V>;
  $ne?: ValueOrExpr<T, V>;
  $gt?: ValueOrExpr<T, V>;
  $gte?: ValueOrExpr<T, V>;
  $lt?: ValueOrExpr<T, V>;
  $lte?: ValueOrExpr<T, V>;
  $in?: readonly ValueOrExpr<T, V>[];
  $nin?: readonly ValueOrExpr<T, V>[];
  $regex?: string | RegExp;
  $exists?: boolean;
};

type PullCondition<T, Elem> = {
  [K in keyof Elem]?:
    | ValueOrExpr<T, Elem[K]>
    | PullMatch<T, Elem[K]>
    | (Elem[K] extends readonly (infer Item)[] ? ValueOrExpr<T, Item> | PullMatch<T, Item> : never);
} & {
  $and?: readonly PullCondition<T, Elem>[];
  $or?: readonly PullCondition<T, Elem>[];
  $nor?: readonly PullCondition<T, Elem>[];
};

export type PullSpec<T> = {
  [P in UpdateArrayPath<T>]?:
    | ValueOrExpr<T, UpdateArrayElementType<T, P>>
    | PullMatch<T, UpdateArrayElementType<T, P>>
    | (UpdateArrayElementType<T, P> extends object ? PullCondition<T, UpdateArrayElementType<T, P>>
      : never);
};

export type PopSpec<T> = { [P in UpdateArrayPath<T>]?: 1 | -1 };

export type PullAllSpec<T> = {
  [P in UpdateArrayPath<T>]?: readonly ValueOrExpr<T, UpdateArrayElementType<T, P>>[];
};

export type BitSpec<T> = UpdateSpecOfType<
  T,
  number | Nil,
  { and: ValueOrExpr<T, number> } | { or: ValueOrExpr<T, number> } | { xor: ValueOrExpr<T, number> }
>;
