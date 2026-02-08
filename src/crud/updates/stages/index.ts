/**
 * Update pipeline stages - unified with aggregation pipeline stages
 *
 * All stages are imported from sluice-stages.ts for single source of truth.
 * Update pipelines use the exact same StageFunction types as aggregation pipelines.
 */

import type {
  TypedPipeline,
  UpdatePipelineBuilder,
  UpdateStageFunction,
} from "../../../pipeline-types.js";
import type { Agg } from "../../../sluice.js";
import {
  $addFields,
  $project,
  $replaceRoot,
  $replaceWith,
  $set,
  $unset,
} from "../../../sluice-stages.js";

/**
 * Type for the context-aware update operators passed as `$` to update pipeline callbacks.
 *
 * Members are named without the `$` prefix so callback usage reads naturally:
 * `$ => $.pipe($.set(...), $.unset(...))`
 */
export type UpdateOperators<C> = {
  set: typeof $set;
  unset: typeof $unset;
  replaceRoot: typeof $replaceRoot;
  replaceWith: typeof $replaceWith;
  addFields: typeof $addFields;
  project: typeof $project;
  pipe: UpdatePipelineBuilder<C>;
};

/**
 * Callback type for update pipelines that receives context-aware operators
 */
export type UpdatePipelineCallback<C> = ($: UpdateOperators<C>) => TypedPipeline<C, unknown>;

/**
 * Creates context-aware update pipeline operators for a collection type.
 * Used internally by `updateOne`/`updateMany` to build the `$` callback context.
 */
export const update = <C>(): UpdateOperators<C> => {
  const pipe = (...stages: UpdateStageFunction<unknown, unknown>[]): TypedPipeline<C, unknown> => {
    const fakeAgg: Agg<C, unknown> = {
      _in: undefined as never,
      _current: undefined as never,
      stages: [] as object[],
      pipe: undefined as never,
      toList: undefined as never,
      toMQL: undefined as never,
    };
    const result = stages.reduce((agg, stage) => stage(agg), fakeAgg);
    return {
      __pipelineIn: undefined as unknown as C,
      __pipelineOut: undefined as unknown,
      stages: result.stages,
    };
  };

  return {
    set: $set,
    unset: $unset,
    replaceRoot: $replaceRoot,
    replaceWith: $replaceWith,
    addFields: $addFields,
    project: $project,
    pipe: pipe as unknown as UpdatePipelineBuilder<C>,
  };
};
