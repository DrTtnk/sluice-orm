/* eslint-disable @typescript-eslint/unified-signatures */
/* eslint-disable @typescript-eslint/no-empty-object-type */
// ==========================================
// Pipeline Stages - All $stage declarations
// ==========================================
import type { ChangeStreamDocument, Document, ObjectId } from "mongodb";
import type { GreaterThan, IsAny, IsUnknown, Simplify } from "type-fest";

import { AccumulatorBuilder, WindowBuilder } from "./builder.js";
import type { ArrayRootPath, ComparablePath, PathType } from "./paths.js";
import { pushStage, resolveStage, unwrapRet } from "./runtime-utils.js";
import {
  type Agg,
  type CallbackOnlyError,
  type Collection,
  type CollectionType,
  type ExprBuilder as ExprBuilder,
  type ExpressionIn,
  type ForeignType,
  type NumericIn,
  type ResolveSpec,
  type ResolveType,
  type Ret,
  type SmartAnyPathFieldRef,
  type TimeUnit,
  type TypedPipeline,
  type UpdateStageFunction,
  type ValidExpressionValue,
  type ValidMatchFilterWithBuilder,
} from "./sluice.js";
import type { OpaqueError } from "./type-errors.js";
import type { __, AnyDict, Dict } from "./type-utils.js";

type RuntimeAgg = { stages?: Dict<unknown>[] } & Dict<unknown>;

type DotPathToObject<Path extends string, Value> =
  Path extends `${infer Head}.${infer Tail}` ? { [K in Head]: DotPathToObject<Tail, Value> }
  : { [K in Path]: Value };

type DeepMerge<A, B> =
  A extends object ?
    B extends object ?
      {
        [K in keyof A | keyof B]: K extends keyof B ?
          K extends keyof A ?
            DeepMerge<A[K], B[K]>
          : B[K]
        : K extends keyof A ? A[K]
        : never;
      }
    : A | B
  : A | B;

// ==========================================
// Stage Helper Types
// ==========================================

type ValidateSpec<C, Spec> = {
  [K in keyof Spec]: Spec[K] extends 0 | 1 ? Spec[K]
  : Spec[K] extends Ret<any, any> ? Spec[K]
  : ValidExpressionValue<C, Spec[K]>;
};

type ProjectSpec<C> =
  | ({ [K in keyof C & string]?: 1 } & { _id?: 0 | 1 } & Dict<ExpressionIn<C> | Ret<C>>)
  | ({ [K in keyof C & string]?: 0 } & { _id?: 0 | 1 } & Dict<ExpressionIn<C> | Ret<C>>)
  | (Dict<ExpressionIn<C> | Ret<C>> & { _id?: 0 | 1 });

type HasKey<Spec, K, V> = { [P in keyof Spec]: Spec[P] extends V ? true : false }[keyof Spec];
type OnlyIdMixed<Spec> =
  HasKey<Spec, any, 1> extends true ?
    HasKey<Spec, any, 0> extends true ?
      keyof Spec & string extends "_id" | `_${string}` ?
        true
      : false
    : true
  : true;

type ValidateProjectRules<Spec> =
  OnlyIdMixed<Spec> extends true ? Spec
  : CallbackOnlyError<"$project cannot mix inclusion (1) and exclusion (0) except for _id">;

type ProjectIdResult<C, Spec> =
  Spec extends { _id: 0 } ? {}
  : Spec extends { _id: 1 } ?
    C extends { _id: infer Id } ?
      { _id: Id }
    : OpaqueError<"$project field does not exist: _id">
  : C extends { _id: infer Id } ? { _id: Id }
  : {};

type ProjectResult<C, Spec> = Simplify<
  ProjectIdResult<C, Spec> & {
    [K in Exclude<keyof Spec, "_id">]: Spec[K] extends 1 ?
      K extends keyof C ?
        C[K]
      : OpaqueError<`$project field does not exist: ${K & string}`>
    : Spec[K] extends 0 ? never
    : ResolveType<C, Spec[K]>;
  }
>;

// ==========================================
// Window Function Types
// ==========================================

type WindowBoundary = "unbounded" | "current" | number;
type WindowDef = {
  documents?: [WindowBoundary, WindowBoundary];
  range?: [WindowBoundary, WindowBoundary];
  unit?: TimeUnit;
};

type AccumulatorOp<C, T = ExpressionIn<C>> = { window?: WindowDef } & (
  | { $sum: T }
  | { $avg: T }
  | { $min: T }
  | { $max: T }
  | { $first: T }
  | { $last: T }
  | { $count: Dict<never> }
  | { $push: T }
  | { $addToSet: T }
);

type WindowOperatorSpec<C> =
  | AccumulatorOp<C>
  | Ret<C, any>
  | { $rank: Dict<never> }
  | { $denseRank: Dict<never> }
  | { $documentNumber: Dict<never> }
  | { $percentRank: Dict<never> }
  | { $shift: { output: ExpressionIn<C>; by: number; default?: ExpressionIn<C> } }
  | { $expMovingAvg: { input: ExpressionIn<C>; N?: number; alpha?: number } }
  | { $linearFill: ExpressionIn<C> }
  | { $derivative: { input: ExpressionIn<C>; unit?: TimeUnit }; window: WindowDef }
  | { $integral: { input: ExpressionIn<C>; unit?: TimeUnit } };

type HasNullish<T> = [Extract<T, null | undefined>] extends [never] ? false : true;
type NullishResult<C, T, Output> =
  HasNullish<ResolveType<C, T>> extends true ? Output | null : Output;

type WindowOperatorResult<C, Op> =
  Op extends Ret<C, infer T> ? T
  : Op extends { $sum: NumericIn<C> } ? number
  : Op extends { $avg: infer A } ? NullishResult<C, A, number>
  : Op extends { $min: ExpressionIn<C> } ? ResolveType<C, Op["$min"]> | null
  : Op extends { $max: ExpressionIn<C> } ? ResolveType<C, Op["$max"]> | null
  : Op extends { $first: ExpressionIn<C> } ? ResolveType<C, Op["$first"]>
  : Op extends { $last: ExpressionIn<C> } ? ResolveType<C, Op["$last"]>
  : Op extends { $count: Dict<never> } ? number
  : Op extends { $push: ExpressionIn<C> } ? ResolveType<C, Op["$push"]>[]
  : Op extends { $addToSet: ExpressionIn<C> } ? ResolveType<C, Op["$addToSet"]>[]
  : Op extends { $rank: Dict<never> } ? number
  : Op extends { $denseRank: Dict<never> } ? number
  : Op extends { $documentNumber: Dict<never> } ? number
  : Op extends { $percentRank: Dict<never> } ? number
  : Op extends { $shift: { output: infer Output; default?: infer Default } } ?
    Default extends ExpressionIn<C> ?
      ResolveType<C, Output> | ResolveType<C, Default>
    : ResolveType<C, Output> | null
  : Op extends { $expMovingAvg: any } ? number | null
  : Op extends { $linearFill: ExpressionIn<C> } ? ResolveType<C, Op["$linearFill"]> | null
  : Op extends { $derivative: any } ? number | null
  : Op extends { $integral: any } ? number | null
  : unknown; // ToDo - use never or error type

type SortBySpec<C> = Partial<Record<keyof C & string, 1 | -1>>;
type SortBySingle<C> = { [K in keyof C & string]: { [P in K]: 1 | -1 } }[keyof C & string];
type SortPath<C> = ComparablePath<C> | "_id" | `_id.${string}`;
type SortValue = 1 | -1 | { $meta: "textScore" | "searchScore" };
type SortSpec<C> =
  | Partial<Record<SortPath<C>, SortValue>>
  | { $meta: "textScore" | "searchScore" }
  | (Partial<Record<SortPath<C>, SortValue>> & { $meta: "textScore" | "searchScore" });

type SortByHasDate<C, S> =
  [S] extends [undefined] ? false
  : true extends (
    {
      [K in keyof S]: K extends keyof C ?
        C[K] extends Date ?
          true
        : false
      : false;
    }[keyof S]
  ) ?
    true
  : false;

type HasRangeWindowWithoutUnit<Output> =
  true extends (
    {
      [K in keyof Output]: Output[K] extends { window: { range: any } } ?
        Output[K] extends { window: { unit: any } } ?
          false
        : true
      : false;
    }[keyof Output]
  ) ?
    true
  : false;

// Check if output contains rank-based operators that require sortBy
type RequiresSortBy<C, Output extends Dict<WindowOperatorSpec<C>>> =
  true extends (
    {
      [K in keyof Output]: Output[K] extends (
        { $rank: any } | { $denseRank: any } | { $percentRank: any }
      ) ?
        true
      : false;
    }[keyof Output]
  ) ?
    true
  : false;

type RemoveNever<T> = { [K in keyof T as T[K] extends never ? never : K]: T[K] };

// UnwindResult - transform array field to its element type
type UnwindPath<T extends string> = T extends `$${infer Field}` ? Field : never;
type UnwindElement<T> =
  T extends readonly (infer E)[] ? E
  : T extends (infer E)[] ? E
  : T;
type UnwindResult<C, P extends string> = {
  [K in keyof C]: K extends UnwindPath<P> ? UnwindElement<C[K]> : C[K];
};

type UnwindResultWithPreserve<C, P extends string, Preserve extends boolean | undefined> = {
  [K in keyof C]: K extends UnwindPath<P> ?
    Preserve extends true ?
      UnwindElement<C[K]> | null | undefined
    : UnwindElement<C[K]>
  : C[K];
};

// IndexStats result type
type IndexStatsResult = {
  name: string;
  key: Dict<number>;
  host: string;
  accesses: { ops: number; since: Date };
  shard?: string;
  spec?: AnyDict;
};

// SortByCount result type
type SortByCountResult<T> = { _id: T; count: number };

// $unset result - remove specified fields
type UnsetResult<C, Fields extends string[]> = Omit<C, Fields[number]>;

type CollectionOf<T> =
  T extends Collection ? T
  : T extends { coll: infer C } ?
    C extends Collection ?
      C
    : never
  : never;

type CollectionConstraint<C, TTarget extends Collection> =
  C extends CollectionType<TTarget> ? {} : CallbackOnlyError<"Collection type mismatch">;

type MergeIntoConstraint<C, TInto extends Collection> = CollectionConstraint<C, TInto>;
type OutIntoConstraint<C, T> = CollectionConstraint<C, CollectionOf<T>>;

// Helper to validate that a record doesn't contain raw MongoDB operators
type ValidateNoRawOperators<T extends Dict<any>> = {
  [K in keyof T]: T[K] extends { readonly __context: any; readonly __type: any } ? T[K]
  : T[K] extends { [key: `$${string}`]: any } ?
    OpaqueError<"Invalid MongoDB operator object - use builder API instead (e.g., $.sum(...))">
  : T[K];
};

// Type for accumulator expressions in $bucket output
// Enforce sortBy requirement for rank-based operators
type WindowFieldsOptions<
  C,
  T extends {
    partitionBy?: ExpressionIn<C> | Dict<ExpressionIn<C> | Ret<C>> | Ret<C>;
    sortBy?: SortBySpec<C>;
    output: Dict<WindowOperatorSpec<C>>;
  },
> =
  SortByHasDate<C, T["sortBy"]> extends true ?
    HasRangeWindowWithoutUnit<T["output"]> extends true ?
      OpaqueError<"$setWindowFields range window with date sortBy requires unit"> & T
    : RequiresSortBy<C, T["output"]> extends true ? T & { sortBy: SortBySingle<C> }
    : T
  : RequiresSortBy<C, T["output"]> extends true ? T & { sortBy: SortBySingle<C> }
  : T;

// ReplaceRootScalar: validated field refs (no bare strings), expressions, system vars
type ReplaceRootScalar<C> = SmartAnyPathFieldRef<C> | Ret<C> | "$$ROOT" | "$$CURRENT";

type ReplaceRootValue<C> = ReplaceRootScalar<C> | { [K in string]: ReplaceRootValue<C> };

type ReplaceRootResolved<C, R> =
  R extends "$$ROOT" | "$$CURRENT" ? C
  : R extends Ret<any, infer T> ? T
  : R extends object ? ResolveSpec<C, R>
  : ResolveType<C, R>;

type IsStrictlyAscending<T extends readonly number[]> =
  T extends (
    readonly [infer A extends number, infer B extends number, ...infer Rest extends number[]]
  ) ?
    number extends A | B ? true
    : GreaterThan<B, A> extends false ? false
    : IsStrictlyAscending<[B, ...Rest]>
  : true;

type AscendingBoundaries<T extends readonly number[]> =
  IsStrictlyAscending<T> extends true ? T
  : OpaqueError<"$bucket boundaries must be strictly increasing">;

// ==========================================
// Stage Declarations
// ==========================================

/** Adds new fields to documents. Alias for `$set`.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/addFields/ */
export const $addFields = pushStage(fieldsBuilder => ({
  $addFields: resolveStage(fieldsBuilder),
})) as unknown as <C, const T>(
  fieldsBuilder: ($: ExprBuilder<Simplify<C>>) => T,
) => UpdateStageFunction<C, Simplify<C & ResolveSpec<C, T>>>;

/** Groups documents into discrete buckets by a specified expression and boundaries.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/bucket/ */
export const $bucket = pushStage((options: any) => {
  const resolved = resolveStage(options) as { output?: unknown } & Dict<unknown>;
  if (resolved.output && typeof options.output === "function") {
    // resolveStage handles the top level, but for bucket output specifically,
    // if options is an object, resolveStage returns it as is (if not function).
    // EXCEPT resolveStage doesn't deep resolve properties.
    // I need to resolve 'output' property if it is a function.
    resolved.output = resolveStage(options.output);
  }
  return { $bucket: resolved };
}) as unknown as {
  // Overload 1: With callback-based output (full accumulator support)
  <
    C,
    const B extends readonly number[],
    const D extends string | number = never,
    const O extends AnyDict = Dict<never>,
  >(options: {
    groupBy: ExpressionIn<C>;
    boundaries: B & AscendingBoundaries<B>;
    default?: D;
    output: ($: AccumulatorBuilder<Simplify<C>>) => O & ValidateNoRawOperators<O>;
  }): <TIn>(agg: Agg<TIn, C>) => Agg<TIn, { _id: B[number] | D } & ResolveSpec<C, O>>;
  // Overload 2: No output (produces only _id and count)
  <C, const B extends readonly number[], const D extends string | number = never>(options: {
    groupBy: ExpressionIn<C>;
    boundaries: B & AscendingBoundaries<B>;
    default?: D;
  }): <TIn>(agg: Agg<TIn, C>) => Agg<TIn, { _id: B[number] | D; count: number }>;
};

/** Automatically groups documents into a specified number of buckets.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/bucketAuto/ */
export const $bucketAuto = pushStage(options => ({
  $bucketAuto: resolveStage(options),
})) as unknown as <
  C,
  const G extends ExpressionIn<C>,
  const O extends Dict<ExpressionIn<C>> | undefined = undefined,
>(options: {
  groupBy: G;
  buckets: number;
  output?: O;
  granularity?:
    | "R5"
    | "R10"
    | "R20"
    | "R40"
    | "R80"
    | "1-2-5"
    | "E6"
    | "E12"
    | "E24"
    | "E48"
    | "E96"
    | "E192"
    | "POWERSOF2";
}) => <TIn>(agg: Agg<TIn, C>) => Agg<
  TIn,
  {
    _id: { min: ResolveType<C, G>; max: ResolveType<C, G> };
    count: number;
  } & (O extends AnyDict ? { [K in keyof O]: ResolveType<C, O[K]> } : {})
>;

/** Returns statistics about the collection (latency, storage, count, query exec stats).
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/collStats/ */
export const $collStats = pushStage(options => ({
  $collStats: resolveStage(options),
})) as unknown as {
  <
    C,
    const T extends {
      latencyStats?: { histograms?: boolean };
      storageStats?: { scale?: number };
      count: Dict<never>;
      queryExecStats?: Dict<never>;
    },
  >(
    options: T,
  ): <TIn>(agg: Agg<TIn, C>) => Agg<
    TIn,
    {
      ns: string;
      localTime: Date;
    } & (T extends { latencyStats: any } ?
      { latencyStats: { reads: Dict<number>; writes: Dict<number>; commands: Dict<number> } }
    : { latencyStats?: { reads: Dict<number>; writes: Dict<number>; commands: Dict<number> } }) &
      (T extends { storageStats: any } ?
        { storageStats: { size: number; count: number; avgObjSize: number; storageSize: number } }
      : {
          storageStats?: { size: number; count: number; avgObjSize: number; storageSize: number };
        }) & { count: number } & (T extends { queryExecStats: any } ?
        { queryExecStats: Dict<number> }
      : { queryExecStats?: Dict<number> })
  >;
  <
    C,
    const T extends {
      latencyStats?: { histograms?: boolean };
      storageStats?: { scale?: number };
      count?: Dict<never>;
      queryExecStats?: Dict<never>;
    },
  >(
    options: T,
  ): <TIn>(agg: Agg<TIn, C>) => Agg<
    TIn,
    {
      ns: string;
      localTime: Date;
    } & (T extends { latencyStats: any } ?
      { latencyStats: { reads: Dict<number>; writes: Dict<number>; commands: Dict<number> } }
    : { latencyStats?: { reads: Dict<number>; writes: Dict<number>; commands: Dict<number> } }) &
      (T extends { storageStats: any } ?
        { storageStats: { size: number; count: number; avgObjSize: number; storageSize: number } }
      : {
          storageStats?: { size: number; count: number; avgObjSize: number; storageSize: number };
        }) & { count?: number } & (T extends { queryExecStats: any } ?
        { queryExecStats: Dict<number> }
      : { queryExecStats?: Dict<number> })
  >;
};

/** Replaces the input documents with a count of the documents at this stage.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/count/ */
export const $count = pushStage(field => ({ $count: field })) as unknown as <
  C,
  const T extends string,
>(
  field: T,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, { [K in T]: number }>;

/** Creates new documents in a sequence of documents where gaps exist.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/densify/ */
export const $densify = pushStage(options => ({ $densify: resolveStage(options) })) as unknown as <
  C,
  const T extends {
    field: keyof C & string;
    partitionByFields?: (keyof C & string)[];
    range: {
      step: number;
      unit?: TimeUnit;
      bounds: "full" | "partition" | [__, __];
    };
  },
>(
  options: NoInfer<T>,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, C>;

/** Returns literal documents from input values (no collection required).
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/documents/ */
export const $documents = ((documents: __[]) => ({
  stages: [{ $documents: documents }],
})) as unknown as <const T extends readonly Dict<unknown>[]>(
  documents: T,
) => Agg<T[number], T[number]>;

/** Processes multiple aggregation pipelines on the same input documents in a single stage.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/facet/ */
export const $facet = pushStage(pipelines => {
  const facetSpec = resolveStage(pipelines) as Dict<unknown>;
  const transformedSpec: any = {};
  for (const [key, val] of Object.entries(facetSpec)) {
    if ((val as any).stages) {
      transformedSpec[key] = (val as any).stages;
    } else if (Array.isArray(val)) {
      transformedSpec[key] = val;
    } else {
      transformedSpec[key] = val; // fallback
    }
  }
  return { $facet: transformedSpec };
}) as unknown as <C, const T extends Dict<TypedPipeline<C, __>>>(
  pipelines: ($: ExprBuilder<Simplify<C>>) => T,
) => <TIn>(agg: Agg<TIn, C>) => Agg<
  TIn,
  {
    [K in keyof T]: T[K] extends TypedPipeline<any, infer R> ? R[]
    : T[K] extends readonly [...any[], (agg: any) => Agg<any, infer R>] ? R[]
    : __[];
  }
>;

/** Populates null and missing field values using linear interpolation or last-observed carry-forward.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/fill/ */
export const $fill = pushStage(options => ({ $fill: resolveStage(options) })) as unknown as <
  C,
  const T extends {
    partitionBy?: ExpressionIn<C>;
    partitionByFields?: (keyof C & string)[];
    sortBy: SortBySpec<C>;
    output: { [K in keyof C]?: { method: "linear" | "locf" } | { value: ExpressionIn<C> } };
  },
>(
  options: T,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, C>;

/** Returns documents ordered by proximity to a geospatial point.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/ */
export const $geoNear = pushStage(options => ({ $geoNear: resolveStage(options) })) as unknown as <
  C,
  const TDistanceField extends string,
  const TIncludeLocs extends string | undefined = undefined,
>(options: {
  near: { type: "Point"; coordinates: [number, number] };
  distanceField: TDistanceField;
  spherical?: boolean;
  maxDistance?: number;
  minDistance?: number;
  query?: AnyDict;
  distanceMultiplier?: number;
  includeLocs?: TIncludeLocs;
  uniqueDocs?: boolean;
  key?: string;
}) => <TIn>(
  agg: Agg<TIn, C>,
) => Agg<
  TIn,
  C &
    DeepMerge<
      DotPathToObject<TDistanceField, number>,
      TIncludeLocs extends string ?
        DotPathToObject<TIncludeLocs, { type: "Point"; coordinates: [number, number] }>
      : {}
    >
>;

/** Performs a recursive search on a collection following references between documents.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/graphLookup/ */
export const $graphLookup = pushStage(
  (options: {
    from: Collection;
    startWith: unknown;
    connectFromField: string;
    connectToField: string;
    as: string;
    maxDepth?: number;
    depthField?: string;
    restrictSearchWithMatch?: AnyDict;
  }) => ({
    $graphLookup: {
      ...(resolveStage(options) as Dict<unknown>),
      from: options.from.__collectionName,
    },
  }),
) as unknown as <
  C,
  const TFrom extends Collection,
  const TAs extends string,
  const TDF extends string | undefined = undefined,
>(options: {
  from: TFrom;
  startWith: ExpressionIn<C>;
  connectFromField: keyof ForeignType<TFrom> & string;
  connectToField: keyof ForeignType<TFrom> & string;
  as: TAs;
  maxDepth?: number;
  depthField?: TDF;
  restrictSearchWithMatch?: AnyDict;
}) => <TIn>(
  agg: Agg<TIn, C>,
) => Agg<TIn, C & Record<TAs, (ForeignType<TFrom> & Record<TDF & string, number>)[]>>;

// GroupIdSpec - _id can be null, a field ref, a Ret expression, or a compound object of field refs / expressions
type GroupIdSpec<C> = ExpressionIn<C> | Ret<C> | null | Dict<ExpressionIn<C> | Ret<C>>;

type ValidateGroupOutput<C, T extends { _id: GroupIdSpec<C> }> = {
  [K in keyof T]: K extends "_id" ? T[K]
  : T[K] extends Ret<C, any> ? T[K]
  : OpaqueError<"Invalid $group output - use builder API (e.g., $.sum(...))">;
};

/** Groups documents by a specified expression and applies accumulator expressions.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/ */
export const $group = pushStage(spec => ({
  $group: unwrapRet(typeof spec === "function" ? spec(new AccumulatorBuilder<unknown>()) : spec),
})) as unknown as <C, const T extends { _id: GroupIdSpec<C> }>(
  spec: ($: AccumulatorBuilder<Simplify<C>>) => ValidateGroupOutput<C, T>,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, ResolveSpec<C, T>>;

/** Returns statistics about index usage for the collection.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexStats/ */
export const $indexStats = pushStage(() => ({ $indexStats: {} })) as unknown as <C>() => <TIn>(
  agg: Agg<TIn, C>,
) => Agg<TIn, IndexStatsResult>;

/** Limits the number of documents passed to the next stage.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/limit/ */
export const $limit = pushStage(n => ({ $limit: n })) as unknown as <C, const T extends number>(
  n: T,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, C>;

type LetToContext<C, Let extends Dict<ExpressionIn<C>>> = {
  [K in keyof Let as `$${K & string}`]: ResolveType<C, Let[K]>;
};

type StripLookupVars<T> = {
  [K in keyof T as K extends `$${string}` ? never : K]: T[K];
};

// Helper: Augment foreign context with let vars if provided
type LookupContext<TFrom extends Collection, C, TLet extends Dict<ExpressionIn<C>> | undefined> =
  TLet extends Dict<ExpressionIn<C>> ? ForeignType<TFrom> & LetToContext<C, TLet>
  : ForeignType<TFrom>;

/** Performs a left outer join with another collection.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/ */
export const $lookup = pushStage((options: { from: Collection } & Dict<unknown>) => {
  const spec: Dict<unknown> & { from: string; pipeline?: unknown } = {
    ...options,
    from: options.from.__collectionName,
  };
  if (typeof spec.pipeline === "function") {
    spec.pipeline = resolveStage(spec.pipeline);
  }
  if (spec.pipeline && typeof spec.pipeline === "object" && "stages" in spec.pipeline) {
    const pipeline = spec.pipeline as { stages?: unknown };
    if (typeof pipeline.stages !== "undefined") {
      spec.pipeline = pipeline.stages;
    }
  }
  return { $lookup: spec };
}) as unknown as {
  // Callback-based pipeline (typed): $ => $.pipe($match(...), $project(...))
  <
    C,
    TFrom extends Collection,
    const TAs extends string,
    const TLet extends Dict<ExpressionIn<C>> | undefined = undefined,
    TPipelineOut = ForeignType<TFrom>,
  >(options: {
    from: TFrom;
    localField?: PathType<C>;
    foreignField?: PathType<ForeignType<TFrom>>;
    let?: TLet;
    pipeline?: (
      $: ExprBuilder<LookupContext<TFrom, C, TLet>>,
    ) => TypedPipeline<LookupContext<TFrom, C, TLet>, TPipelineOut>;
    as: TAs;
  }): <TIn>(
    agg: Agg<TIn, C>,
  ) => Agg<TIn, Simplify<C & Record<TAs, StripLookupVars<TPipelineOut>[]>>>;
  // Array of raw objects (rejected - use callback form)
  <C, TFrom extends Collection, const TAs extends string>(options: {
    from: TFrom;
    localField?: PathType<C>;
    foreignField?: PathType<ForeignType<TFrom>>;
    pipeline: readonly object[];
    as: TAs;
  }): <TIn>(agg: Agg<TIn, C>) => Agg<TIn, CallbackOnlyError<"$lookup.pipeline">>;
};

/** Filters documents to pass only those that match the specified conditions.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/match/ */
export const $match = pushStage(filter => ({ $match: resolveStage(filter) })) as unknown as <
  C,
  const R extends NoInfer<ValidMatchFilterWithBuilder<C>>,
>(
  filter: ($: ExprBuilder<Simplify<C>>) => R,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, C>;

/** Writes pipeline results to a collection, merging with existing documents. Terminal stage.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/merge/ */
export const $merge = pushStage((options: { into: Collection } & Dict<unknown>) => ({
  $merge: {
    ...(resolveStage(options) as Dict<unknown>),
    into: options.into.__collectionName,
  },
})) as unknown as <
  C,
  const T extends {
    into: Collection;
    on?: (keyof C & string) | readonly (keyof C & string)[];
    let?: Dict<ExpressionIn<C>>;
    whenMatched?: "replace" | "keepExisting" | "merge" | "fail" | readonly AnyDict[];
    whenNotMatched?: "insert" | "discard" | "fail";
  },
>(
  options: T & MergeIntoConstraint<C, T["into"]>,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, never>; // $merge is terminal - no documents flow out

/** Writes pipeline results to a collection, replacing existing content. Terminal stage.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/out/ */
export const $out = pushStage(options => {
  const resolved = resolveStage(options) as Collection | { db: string; coll: Collection };

  if ("coll" in resolved) {
    return { $out: { db: resolved.db, coll: resolved.coll.__collectionName } };
  }

  return { $out: resolved.__collectionName };
}) as unknown as <C, const T extends Collection | { db: string; coll: Collection }>(
  options: T & OutIntoConstraint<C, T>,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, never>; // $out is terminal - no documents flow out

/** Reshapes documents by including, excluding, or computing new fields.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/project/ */
export const $project = pushStage(spec => ({ $project: resolveStage(spec) })) as unknown as <
  C,
  const T extends Dict<0 | 1 | ExpressionIn<C> | Ret<C>>,
>(
  spec: ($: ExprBuilder<Simplify<C>>) => ValidateProjectRules<ValidateSpec<C, T>>,
) => UpdateStageFunction<C, RemoveNever<ProjectResult<C, T>>>;

/** Restricts document content based on access-control expressions ($$DESCEND, $$PRUNE, $$KEEP).
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/redact/ */
export const $redact = pushStage(expression => ({
  $redact: resolveStage(expression),
})) as unknown as {
  // Overload 1: Callback returning redact action
  <C>(
    expression: ($: ExprBuilder<Simplify<C>>) => Ret<C, "$$DESCEND" | "$$PRUNE" | "$$KEEP">,
  ): <TIn>(agg: Agg<TIn, C>) => Agg<TIn, C>;
  // Overload 2: Literal or expression value
  <
    C,
    const T extends "$$DESCEND" | "$$PRUNE" | "$$KEEP" | Ret<C, "$$DESCEND" | "$$PRUNE" | "$$KEEP">,
  >(
    expression: T,
  ): <TIn>(agg: Agg<TIn, C>) => Agg<TIn, C>;
};

/** Replaces the input document with the specified document (via `newRoot`).
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceRoot/ */
export const $replaceRoot = pushStage((options: unknown) => {
  const resolved = resolveStage(options);
  // If resolved value has 'newRoot' property, use it as-is (options overloads)
  // Otherwise, wrap it in newRoot (expression overloads)
  const spec =
    resolved !== null && typeof resolved === "object" && "newRoot" in resolved ?
      resolved
    : { newRoot: resolved };
  return { $replaceRoot: spec };
}) as unknown as {
  <C, const T extends { newRoot: ReplaceRootValue<C> }>(
    options: T,
  ): UpdateStageFunction<C, ReplaceRootResolved<C, T["newRoot"]>>;
  <C, const T extends { newRoot: ReplaceRootValue<Simplify<C>> }>(
    options: ($: ExprBuilder<Simplify<C>>) => T,
  ): UpdateStageFunction<C, ReplaceRootResolved<C, T["newRoot"]>>;
  <C, const T extends ReplaceRootValue<C>>(
    expression: T,
  ): UpdateStageFunction<C, ReplaceRootResolved<C, T>>;
  <C, const T extends ReplaceRootValue<Simplify<C>>>(
    expression: ($: ExprBuilder<Simplify<C>>) => T,
  ): UpdateStageFunction<C, ReplaceRootResolved<C, T>>;
};

/** Replaces the input document with the specified expression. Shorthand for `$replaceRoot`.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceWith/ */
export const $replaceWith = pushStage(expression => ({
  $replaceWith: resolveStage(expression),
})) as unknown as {
  <C, const T extends ReplaceRootValue<C>>(
    expression: T,
  ): UpdateStageFunction<C, ReplaceRootResolved<C, T>>;
  <C, const T extends ReplaceRootValue<Simplify<C>>>(
    expression: ($: ExprBuilder<Simplify<C>>) => T,
  ): UpdateStageFunction<C, ReplaceRootResolved<C, T>>;
};

/** Randomly selects the specified number of documents from input.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/sample/ */
export const $sample = pushStage(options => ({ $sample: resolveStage(options) })) as unknown as <
  C,
  const T extends number,
>(options: {
  size: T;
}) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, C>;

/** Adds new fields to documents. Alias for `$addFields`.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/set/ */
export const $set = pushStage(fields => ({ $set: resolveStage(fields) })) as unknown as {
  <C, const T extends Dict<ExpressionIn<C> | Ret<C>>>(
    fields: NoInfer<T>,
  ): UpdateStageFunction<C, Simplify<C & { [K in keyof T]: ResolveType<C, T[K]> }>>;
  <C, const T extends Dict<ExpressionIn<Simplify<C>> | Ret<Simplify<C>>>>(
    fields: ($: ExprBuilder<Simplify<C>>) => T,
  ): UpdateStageFunction<C, Simplify<C & { [K in keyof T]: ResolveType<C, T[K]> }>>;
};

/** Performs window function calculations across a specified span of documents.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/setWindowFields/ */
export const $setWindowFields = pushStage(options => ({
  $setWindowFields: unwrapRet(
    typeof options === "function" ? options(new WindowBuilder<unknown>()) : options,
  ),
})) as unknown as <
  C,
  const T extends {
    partitionBy?: ExpressionIn<C> | Dict<ExpressionIn<C> | Ret<C>> | Ret<C>;
    sortBy?: SortBySpec<C>;
    output: Dict<WindowOperatorSpec<C>>;
  },
>(
  options: ($: WindowBuilder<Simplify<C>>) => WindowFieldsOptions<C, T>,
) => <TIn>(
  agg: Agg<TIn, C>,
) => Agg<TIn, C & { [K in keyof T["output"]]: WindowOperatorResult<C, T["output"][K]> }>;

/** Skips the first N documents.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/skip/ */
export const $skip = pushStage(n => ({ $skip: n })) as unknown as <C, const T extends number>(
  n: T,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, C>;

/** Sorts all input documents and returns them in the specified order.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/sort/ */
export const $sort = pushStage(spec => ({ $sort: resolveStage(spec) })) as unknown as <C>(
  spec: SortSpec<C>,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, C>;

/** Groups documents by an expression and sorts by count.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/sortByCount/ */
export const $sortByCount = pushStage(expression => ({
  $sortByCount: resolveStage(expression),
})) as unknown as <C, const T extends ExpressionIn<C>>(
  expression: T,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, SortByCountResult<ResolveType<C, T>>>;

/** Combines pipeline results from another collection into the current pipeline.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/unionWith/ */
export const $unionWith = pushStage((options: { coll: Collection } & Dict<unknown>) => {
  const spec: Dict<unknown> & { coll: string; pipeline?: unknown } = {
    ...options,
    coll: options.coll.__collectionName,
  };
  if (typeof spec.pipeline === "function") {
    spec.pipeline = resolveStage(spec.pipeline);
  }
  if (spec.pipeline && typeof spec.pipeline === "object" && "stages" in spec.pipeline) {
    const pipeline = spec.pipeline as { stages?: unknown };
    if (typeof pipeline.stages !== "undefined") {
      spec.pipeline = pipeline.stages;
    }
  }
  return { $unionWith: spec };
}) as unknown as <
  C,
  const T extends {
    coll: Collection;
    pipeline?:
      | TypedPipeline<ForeignType<T["coll"]>, __>
      | readonly ((agg: Agg<ForeignType<T["coll"]>, ForeignType<T["coll"]>>) => any)[];
  },
>(
  options: T,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, C | ForeignType<T["coll"]>>;

/** Removes specified fields from documents.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/unset/ */
export const $unset = ((...fields: unknown[]) =>
  (agg: RuntimeAgg) => ({
    ...agg,
    stages: [...(agg.stages ?? []), { $unset: fields.length === 1 ? fields[0] : fields }],
  })) as unknown as <C, const T extends readonly (keyof C & string)[]>(
  ...fields: T
) => UpdateStageFunction<C, UnsetResult<C, T extends readonly string[] ? [...T] : never>>;

/** Deconstructs an array field, outputting one document per array element.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/ */
export const $unwind = pushStage(pathOrOptions => ({
  $unwind: resolveStage(pathOrOptions),
})) as unknown as {
  // Simple string path overload
  <C, const P extends `$${ArrayRootPath<C>}`>(
    path: P,
  ): <TIn>(agg: Agg<TIn, C>) => Agg<TIn, UnwindResult<C, P>>;

  // Options object overload
  <
    C,
    const P extends `$${ArrayRootPath<C>}`,
    const TPreserve extends boolean | undefined = undefined,
    const TIndexField extends string | undefined = undefined,
  >(options: {
    path: P;
    includeArrayIndex?: TIndexField;
    preserveNullAndEmptyArrays?: TPreserve;
  }): <TIn>(
    agg: Agg<TIn, C>,
  ) => Agg<
    TIn,
    UnwindResultWithPreserve<C, P, TPreserve> &
      (TIndexField extends string ? Record<TIndexField, number> : {})
  >;
};

// ==========================================
// Missing Stages Implementation
// ==========================================

type SanitizeAny<T> =
  IsAny<T> extends true ? never
  : IsUnknown<T> extends true ? never
  : T extends Date ? T
  : T extends readonly (infer E)[] ? readonly SanitizeAny<E>[]
  : T extends object ? { [K in keyof T]: SanitizeAny<T[K]> }
  : T;

type ChangeStreamDocumentSafe<T extends Document> = SanitizeAny<ChangeStreamDocument<T>>;

/** Opens a change stream cursor on a collection. Must be the first stage.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/changeStream/ */
export const $changeStream = pushStage(options => ({
  $changeStream: resolveStage(options),
})) as unknown as <
  C extends Document,
  const T extends {
    allChangesForCluster?: boolean;
    fullDocument?: "default" | "required" | "updateLookup" | "whenAvailable";
    fullDocumentBeforeChange?: "off" | "whenAvailable" | "required";
    resumeAfter?: AnyDict;
    startAfter?: AnyDict;
    startAtOperationTime?: Date;
    showExpandedEvents?: boolean;
  },
>(
  options: NoInfer<T>,
) => <TIn>(agg: Agg<TIn, C>) => Agg<TIn, Simplify<ChangeStreamDocumentSafe<C>>>;

/** Returns information on active and queued operations. For `db.aggregate()` only.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/currentOp/ */
export const $currentOp = pushStage(options => ({
  $currentOp: resolveStage(options),
})) as unknown as <
  C,
  const T extends {
    allUsers?: boolean;
    idleConnections?: boolean;
    idleCursors?: boolean;
    idleSessions?: boolean;
    localOps?: boolean;
  },
>(
  options: NoInfer<T>,
) => <TIn>(agg: Agg<TIn, C>) => Agg<
  TIn,
  {
    host: string;
    desc: string;
    connectionId: number;
    client: string;
    appName?: string;
    clientMetadata?: AnyDict;
    active: boolean;
    currentOpTime: Date;
    opid: number;
    secs_running: number;
    microsecs_running: number;
    op: string;
    ns: string;
    command: AnyDict;
    planSummary?: string;
    cursor?: { cursorId: number; createdDate: Date; lastAccessDate: Date };
    lsid?: { id: ObjectId; uid: string };
    transaction?: AnyDict;
    locking?: AnyDict;
    waitingForLock?: boolean;
    msg?: string;
    progress?: { done: number; total: number };
    killPending?: boolean;
    numYields?: number;
    dataThroughputLastSecond?: number;
    dataThroughputAverage?: number;
  }
>;

/** Lists sessions cached in memory by the `mongod` or `mongos` instance.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/listLocalSessions/ */
export declare const $listLocalSessions: <
  C,
  const T extends {
    allUsers?: boolean;
    users?: { user: string; db: string }[];
  },
>(
  options: NoInfer<T>,
) => <TIn>(agg: Agg<TIn, C>) => Agg<
  TIn,
  {
    _id: { id: ObjectId; uid: string };
    lastUse: Date;
    user?: { user: string; db: string };
    client?: string;
    connections?: number;
    activeTransactions?: AnyDict;
  }
>;

/** Lists all sessions stored in the `system.sessions` collection.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/listSessions/ */
export declare const $listSessions: <
  C,
  const T extends {
    allUsers?: boolean;
    users?: { user: string; db: string }[];
  },
>(
  options: NoInfer<T>,
) => <TIn>(agg: Agg<TIn, C>) => Agg<
  TIn,
  {
    _id: { id: ObjectId; uid: string };
    lastUse: Date;
    user?: { user: string; db: string };
    client?: string;
    connections?: number;
    activeTransactions?: AnyDict;
  }
>;

/** Returns plan cache information for a collection.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/planCacheStats/ */
export declare const $planCacheStats: <C>() => <TIn>(agg: Agg<TIn, C>) => Agg<
  TIn,
  {
    version: string;
    created: Date;
    planCacheKey: string;
    queryHash: string;
    planCacheKeyWithQueryHash: string;
    isActive: boolean;
    works: number;
    timeOfCreation: Date;
    timeOfCreationMicros: number;
    lastSeenDate: Date;
    lastSeenDateMicros: number;
    creationExecStats?: AnyDict;
    cachedPlan?: AnyDict;
    winningPlan?: AnyDict;
    candidatePlans?: AnyDict[];
  }
>;

/** Returns data distribution information for sharded collections.
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/shardedDataDistribution/ */
export declare const $shardedDataDistribution: <C>() => <TIn>(agg: Agg<TIn, C>) => Agg<
  TIn,
  {
    ns: string;
    shards: Record<
      string,
      {
        numOrphanedDocs: number;
        numOwnedDocuments: number;
        ownedSizeBytes: number;
        orphanedSizeBytes: number;
      }
    >;
    numShards: number;
    collections: Record<
      string,
      {
        numOrphanedDocs: number;
        numOwnedDocuments: number;
        ownedSizeBytes: number;
        orphanedSizeBytes: number;
      }
    >;
    totalSizeBytes: number;
    totalOrphanedSizeBytes: number;
    totalOwnedSizeBytes: number;
    totalNumOrphanedDocs: number;
    totalNumOwnedDocuments: number;
  }
>;

// Atlas-only stages (commented out as not available in standard MongoDB)
// export declare const $search: <C, const T extends { ... }> => ...
// export declare const $searchMeta: <C, const T extends { ... }> => ...
// export declare const $vectorSearch: <C, const T extends { ... }> => ...
