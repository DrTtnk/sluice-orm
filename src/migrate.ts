/**
 * Migration tool — type-safe collection schema migrations via update pipelines.
 *
 * Usage:
 *   const migration = migrate<OldUser, NewUser>();
 *   const pipeline = migration.pipe(
 *     $set({ newField: "default" }),
 *     $unset("deprecatedField"),
 *   );
 *   // pipeline is only valid if the output matches NewUser
 *   await collection.updateMany(() => ({}), $ => pipeline).execute();
 */

import type { Agg, MigrationPipelineBuilder, TypedPipeline } from "./pipeline-types.js";
import type { SimplifyWritable } from "./type-utils.js";

// Re-export for consumers
export type { MigrationPipelineBuilder } from "./pipeline-types.js";

/**
 * MigrationOperators — the migration context with pipe and stage references.
 */
export type MigrationOperators<TFrom, TTo> = {
  pipe: MigrationPipelineBuilder<TFrom, TTo>;
};

/**
 * Creates a type-safe migration pipeline builder.
 *
 * The pipeline starts from TFrom (source schema) and validates that the
 * final output is assignable to TTo (target schema).
 *
 * Only update-allowed stages ($set, $unset, $addFields, $project,
 * $replaceRoot, $replaceWith) are accepted.
 *
 * @example
 * ```ts
 * type OldUser = { _id: ObjectId; name: string; age: number };
 * type NewUser = { _id: ObjectId; name: string; age: number; email: string };
 *
 * const m = migrate<OldUser, NewUser>();
 * const pipeline = m.pipe(
 *   $set({ email: "unknown@example.com" }),
 * );
 * // ✅ compiles — output matches NewUser
 *
 * const bad = m.pipe(
 *   $unset("age"),
 * );
 * // ❌ error — output { _id, name } doesn't match NewUser
 * ```
 */
export const migrate = <TFrom, TTo>(): MigrationOperators<
  SimplifyWritable<TFrom>,
  SimplifyWritable<TTo>
> => {
  const pipe = (
    ...stages: ((agg: Agg<unknown, unknown>) => Agg<unknown, unknown>)[]
  ): TypedPipeline<TFrom, unknown> => {
    const fakeAgg: Agg<TFrom, unknown> = {
      _in: undefined as never,
      _current: undefined as never,
      stages: [] as object[],
      pipe: undefined as never,
      toList: undefined as never,
      toMQL: undefined as never,
    };
    // eslint-disable-next-line @typescript-eslint/prefer-reduce-type-parameter
    const result = stages.reduce((agg, stage) => stage(agg), fakeAgg as Agg<unknown, unknown>);
    return {
      __pipelineIn: undefined as unknown as TFrom,
      __pipelineOut: undefined as unknown,
      stages: result.stages,
    };
  };

  return {
    pipe: pipe as unknown as MigrationPipelineBuilder<
      SimplifyWritable<TFrom>,
      SimplifyWritable<TTo>
    >,
  };
};
