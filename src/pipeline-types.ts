import type { Collection } from "mongodb";
import type * as tf from "type-fest";

import type { OpaqueError } from "./type-errors.js";
import type { SimplifyWritable } from "./type-utils.js";

/**
 * Pipeline type utilities for type-safe stage chaining
 *
 * This module provides a unified PipelineBuilder that works for both
 * aggregation pipelines and update pipelines.
 */

// ==========================================
// Deep Error Checking
// ==========================================

type CheckedResult<T> =
  tf.IsAny<T> extends true ?
    OpaqueError<"Pipeline stage produced or propagated 'any' or 'unknown' type. Please ensure all expressions are fully typed."> & {
      __inferredType: T;
    }
  : T;

// ==========================================
// Core Types
// ==========================================

/**
 * Core Agg type - represents a pipeline at a point with input and current document types
 */
export type Agg<TIn, TCurrent> = {
  readonly _in: TIn;
  readonly _current: SimplifyWritable<TCurrent>;
  readonly stages: readonly object[];
  pipe<TOut>(stage: (agg: Agg<TIn, TCurrent>) => Agg<TIn, TOut>): Agg<TIn, TOut>;
  toList(collection?: Collection): Promise<SimplifyWritable<CheckedResult<TCurrent>>[]>;
  /** Returns the accumulated pipeline stages as formatted JSON */
  toMQL(): string;
};

/**
 * Typed pipeline marker - used for sub-pipelines in $facet, $lookup, update pipelines, etc.
 */
export type TypedPipeline<TIn, TOut> = {
  readonly __pipelineIn: TIn;
  readonly __pipelineOut: TOut;
  readonly stages: readonly unknown[];
};

// ==========================================
// Stage Function Type
// ==========================================

/**
 * A stage function transforms an Agg<TIn, TFrom> to Agg<TIn, TTo>
 * The TIn parameter is the original input type (for expression context)
 *
 * Used by both aggregation stages and update stages.
 */
export type StageFunction<TIn, TFrom, TTo> = (agg: Agg<TIn, TFrom>) => Agg<TIn, TTo>;

/**
 * Brand for stages that are allowed in update pipelines.
 * Only $set, $unset, $addFields, $project, $replaceRoot, $replaceWith are branded.
 *
 * IMPORTANT: UpdateStageFunction uses a callable object type with a call signature
 * (`{ (agg): result; [brand]: true }`) rather than a function-object intersection
 * (`((agg) => result) & { [brand]: true }`). TypeScript's inference engine cannot
 * propagate generic parameters through function-object intersections in multi-stage
 * overload resolution, causing C to default to {}.
 */
declare const __updateStage: unique symbol;
export type UpdateStageBrand = { readonly [__updateStage]: true };
export type UpdateStageFunction<TFrom, TTo> = {
  <TIn>(agg: Agg<TIn, TFrom>): Agg<TIn, TTo>;
  readonly [__updateStage]: true;
};

// ==========================================
// Unified Pipeline Builder
// ==========================================

/**
 * Output type resolver for all pipeline builder variants.
 *
 * - "agg": returns Agg (for collection.aggregate)
 * - "pipeline": returns TypedPipeline (for sub-pipelines, pipe())
 * - "update": validates Last extends Target, wraps in CheckedResult
 * - "migration": validates Simplify<Last> extends Target
 */
type PipelineOutput<Mode extends "agg" | "pipeline" | "update" | "migration", TIn, Last, Target> =
  Mode extends "agg" ? Agg<TIn, Last>
  : Mode extends "pipeline" ? TypedPipeline<TIn, Last>
  : Mode extends "update" ?
    Last extends Target ?
      TypedPipeline<TIn, CheckedResult<Last>>
    : OpaqueError<"Update pipeline output must be assignable to collection type"> &
        TypedPipeline<TIn, Last>
  : tf.Simplify<Last> extends Target ? TypedPipeline<TIn, Last>
  : OpaqueError<"Migration output does not match target schema"> & TypedPipeline<TIn, Last>;

/**
 * Branded stage: bare StageFunction for standard builders,
 * callable-object UpdateStageFunction for update/migration builders.
 */
type BStage<TIn, Brand, From, To> =
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  [{}] extends [Brand] ? StageFunction<TIn, From, To> : UpdateStageFunction<From, To>;

/**
 * GenericPipelineBuilder — single set of 15 overloads parameterized by Brand and Mode.
 * All exported pipeline builder types derive from this.
 */
type GenericPipelineBuilder<
  TIn,
  Brand,
  Mode extends "agg" | "pipeline" | "update" | "migration",
  Target = unknown,
> = {
  <A>(s1: BStage<TIn, Brand, TIn, A>): PipelineOutput<Mode, TIn, A, Target>;

  <A, B>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
  ): PipelineOutput<Mode, TIn, B, Target>;

  <A, B, C>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
  ): PipelineOutput<Mode, TIn, C, Target>;

  <A, B, C, D>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
  ): PipelineOutput<Mode, TIn, D, Target>;

  <A, B, C, D, E>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
    s5: BStage<TIn, Brand, D, E>,
  ): PipelineOutput<Mode, TIn, E, Target>;

  <A, B, C, D, E, F>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
    s5: BStage<TIn, Brand, D, E>,
    s6: BStage<TIn, Brand, E, F>,
  ): PipelineOutput<Mode, TIn, F, Target>;

  <A, B, C, D, E, F, G>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
    s5: BStage<TIn, Brand, D, E>,
    s6: BStage<TIn, Brand, E, F>,
    s7: BStage<TIn, Brand, F, G>,
  ): PipelineOutput<Mode, TIn, G, Target>;

  <A, B, C, D, E, F, G, H>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
    s5: BStage<TIn, Brand, D, E>,
    s6: BStage<TIn, Brand, E, F>,
    s7: BStage<TIn, Brand, F, G>,
    s8: BStage<TIn, Brand, G, H>,
  ): PipelineOutput<Mode, TIn, H, Target>;

  <A, B, C, D, E, F, G, H, I>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
    s5: BStage<TIn, Brand, D, E>,
    s6: BStage<TIn, Brand, E, F>,
    s7: BStage<TIn, Brand, F, G>,
    s8: BStage<TIn, Brand, G, H>,
    s9: BStage<TIn, Brand, H, I>,
  ): PipelineOutput<Mode, TIn, I, Target>;

  <A, B, C, D, E, F, G, H, I, J>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
    s5: BStage<TIn, Brand, D, E>,
    s6: BStage<TIn, Brand, E, F>,
    s7: BStage<TIn, Brand, F, G>,
    s8: BStage<TIn, Brand, G, H>,
    s9: BStage<TIn, Brand, H, I>,
    s10: BStage<TIn, Brand, I, J>,
  ): PipelineOutput<Mode, TIn, J, Target>;

  <A, B, C, D, E, F, G, H, I, J, K>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
    s5: BStage<TIn, Brand, D, E>,
    s6: BStage<TIn, Brand, E, F>,
    s7: BStage<TIn, Brand, F, G>,
    s8: BStage<TIn, Brand, G, H>,
    s9: BStage<TIn, Brand, H, I>,
    s10: BStage<TIn, Brand, I, J>,
    s11: BStage<TIn, Brand, J, K>,
  ): PipelineOutput<Mode, TIn, K, Target>;

  <A, B, C, D, E, F, G, H, I, J, K, L>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
    s5: BStage<TIn, Brand, D, E>,
    s6: BStage<TIn, Brand, E, F>,
    s7: BStage<TIn, Brand, F, G>,
    s8: BStage<TIn, Brand, G, H>,
    s9: BStage<TIn, Brand, H, I>,
    s10: BStage<TIn, Brand, I, J>,
    s11: BStage<TIn, Brand, J, K>,
    s12: BStage<TIn, Brand, K, L>,
  ): PipelineOutput<Mode, TIn, L, Target>;

  <A, B, C, D, E, F, G, H, I, J, K, L, M>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
    s5: BStage<TIn, Brand, D, E>,
    s6: BStage<TIn, Brand, E, F>,
    s7: BStage<TIn, Brand, F, G>,
    s8: BStage<TIn, Brand, G, H>,
    s9: BStage<TIn, Brand, H, I>,
    s10: BStage<TIn, Brand, I, J>,
    s11: BStage<TIn, Brand, J, K>,
    s12: BStage<TIn, Brand, K, L>,
    s13: BStage<TIn, Brand, L, M>,
  ): PipelineOutput<Mode, TIn, M, Target>;

  <A, B, C, D, E, F, G, H, I, J, K, L, M, N>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
    s5: BStage<TIn, Brand, D, E>,
    s6: BStage<TIn, Brand, E, F>,
    s7: BStage<TIn, Brand, F, G>,
    s8: BStage<TIn, Brand, G, H>,
    s9: BStage<TIn, Brand, H, I>,
    s10: BStage<TIn, Brand, I, J>,
    s11: BStage<TIn, Brand, J, K>,
    s12: BStage<TIn, Brand, K, L>,
    s13: BStage<TIn, Brand, L, M>,
    s14: BStage<TIn, Brand, M, N>,
  ): PipelineOutput<Mode, TIn, N, Target>;

  <A, B, C, D, E, F, G, H, I, J, K, L, M, N, O>(
    s1: BStage<TIn, Brand, TIn, A>,
    s2: BStage<TIn, Brand, A, B>,
    s3: BStage<TIn, Brand, B, C>,
    s4: BStage<TIn, Brand, C, D>,
    s5: BStage<TIn, Brand, D, E>,
    s6: BStage<TIn, Brand, E, F>,
    s7: BStage<TIn, Brand, F, G>,
    s8: BStage<TIn, Brand, G, H>,
    s9: BStage<TIn, Brand, H, I>,
    s10: BStage<TIn, Brand, I, J>,
    s11: BStage<TIn, Brand, J, K>,
    s12: BStage<TIn, Brand, K, L>,
    s13: BStage<TIn, Brand, L, M>,
    s14: BStage<TIn, Brand, M, N>,
    s15: BStage<TIn, Brand, N, O>,
  ): PipelineOutput<Mode, TIn, O, Target>;
};

// ==========================================
// Derived Pipeline Builder Types
// ==========================================

/** For sub-pipeline composition (pipe(), $facet, etc.) — no output validation */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export type PipelineBuilder<TIn> = GenericPipelineBuilder<TIn, {}, "pipeline">;

/** For collection.aggregate() — returns Agg instead of TypedPipeline */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export type AggregateBuilder<T> = GenericPipelineBuilder<T, {}, "agg">;

/** For update pipelines — only UpdateStageBrand stages, validates output extends C */
export type UpdatePipelineBuilder<C> = GenericPipelineBuilder<C, UpdateStageBrand, "update", C>;

/** For schema migration — only UpdateStageBrand stages, validates Simplify<output> extends TTo */
export type MigrationPipelineBuilder<TFrom, TTo> = GenericPipelineBuilder<
  TFrom,
  UpdateStageBrand,
  "migration",
  TTo
>;
