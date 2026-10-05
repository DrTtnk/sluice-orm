import type {
  BulkWriteResult,
  CollationOptions,
  DeleteResult,
  Document,
  InsertManyResult,
  InsertOneResult,
  UpdateResult,
} from "mongodb";

import type { FindResult, ValidateProjectionOptions } from "./projection-types.js";
import type { ExecutionMode, OperationResult } from "./execution-types.js";
import type { UpdatePipelineCallback } from "./crud/updates/stages/index.js";
import type {
  StrictUpdateSpec as UpdateSpec,
  UpdateOptions as UpdateOpts,
  ValidateUpdateSpec,
} from "./crud/updates/types.js";
import type { ExtractRequiredIdentifiers } from "./crud/updates/validation.js";
import type { ExprBuilder, SimplifyWritable, ValidMatchFilterWithBuilder } from "./sluice.js";

export type CrudFilter<C> = ValidMatchFilterWithBuilder<C>;

export type SortSpec<C> = Partial<Record<string | (keyof C & string), 1 | -1>>;
export type ProjectionSpec<C> = Partial<Record<keyof C & string, 0 | 1>>;

export type FindOptions<C> = {
  projection?: ProjectionSpec<C>;
  sort?: SortSpec<C>;
  limit?: number;
  skip?: number;
  hint?: string | Document;
  collation?: CollationOptions;
  maxTimeMS?: number;
  comment?: string;
};

export type FindBuilder<C, Mode extends ExecutionMode = "promise"> = {
  readonly _filter: CrudFilter<C> | undefined;
  readonly _options: FindOptions<C> | undefined;
  toList(): OperationResult<C[], Mode>;
  toOne(): OperationResult<C | null, Mode>;
};

export type InsertOneBuilder<C, Mode extends ExecutionMode = "promise"> = {
  readonly _doc: C;
  execute(): OperationResult<InsertOneResult, Mode>;
};

export type InsertManyBuilder<C, Mode extends ExecutionMode = "promise"> = {
  readonly _docs: readonly C[];
  execute(): OperationResult<InsertManyResult, Mode>;
};

export type UpdateOneBuilder<
  C extends Document,
  U extends object = UpdateSpec<C>,
  Mode extends ExecutionMode = "promise",
> = {
  readonly _filter: CrudFilter<C>;
  readonly _update: U | (($: ExprBuilder<C>) => U);
  readonly _options: UpdateOpts<C, U> | undefined;
  execute(): OperationResult<UpdateResult, Mode>;
};

export type UpdateManyBuilder<
  C extends Document,
  U extends object = UpdateSpec<C>,
  Mode extends ExecutionMode = "promise",
> = {
  readonly _filter: CrudFilter<C>;
  readonly _update: U | (($: ExprBuilder<C>) => U);
  readonly _options: UpdateOpts<C, U> | undefined;
  execute(): OperationResult<UpdateResult, Mode>;
};

export type ReplaceBuilder<C, Mode extends ExecutionMode = "promise"> = {
  readonly _filter: CrudFilter<C>;
  readonly _replacement: C;
  readonly _options: unknown;
  execute(): OperationResult<UpdateResult, Mode>;
};

export type DeleteBuilder<C, Mode extends ExecutionMode = "promise"> = {
  readonly _filter: CrudFilter<C>;
  execute(): OperationResult<DeleteResult, Mode>;
};

export type FindOneAndDeleteBuilder<C, Mode extends ExecutionMode = "promise", Output = C> = {
  readonly _filter: CrudFilter<C>;
  execute(): OperationResult<Output | null, Mode>;
};

export type FindOneAndReplaceBuilder<C, Mode extends ExecutionMode = "promise", Output = C> = {
  readonly _filter: CrudFilter<C>;
  readonly _replacement: C;
  execute(): OperationResult<Output | null, Mode>;
};

export type FindOneAndUpdateBuilder<
  C extends Document,
  U extends object = UpdateSpec<C>,
  Mode extends ExecutionMode = "promise",
  Output = C,
> = {
  readonly _filter: CrudFilter<C>;
  readonly _update: U;
  execute(): OperationResult<Output | null, Mode>;
};

export type FindOneAndOptions<C> = {
  sort?: SortSpec<C>;
  projection?: ProjectionSpec<C>;
  upsert?: boolean;
  returnDocument?: "before" | "after";
  hint?: string | Document;
  collation?: CollationOptions;
  maxTimeMS?: number;
  comment?: string;
};

export type CountOptions = {
  limit?: number;
  skip?: number;
  maxTimeMS?: number;
  hint?: string | Document;
  collation?: CollationOptions;
  comment?: string;
};

export type CountBuilder<Mode extends ExecutionMode = "promise"> = {
  execute(): OperationResult<number, Mode>;
};

export type BulkWriteOp<C extends Document> =
  | { insertOne: { document: C } }
  | {
      updateOne: {
        filter: CrudFilter<C>;
        update: UpdateSpec<C>;
        upsert?: boolean;
        arrayFilters?: Document[];
        hint?: string | Document;
        collation?: CollationOptions;
      };
    }
  | {
      updateMany: {
        filter: CrudFilter<C>;
        update: UpdateSpec<C>;
        upsert?: boolean;
        arrayFilters?: Document[];
        hint?: string | Document;
        collation?: CollationOptions;
      };
    }
  | { deleteOne: { filter: CrudFilter<C>; hint?: string | Document; collation?: CollationOptions } }
  | {
      deleteMany: { filter: CrudFilter<C>; hint?: string | Document; collation?: CollationOptions };
    }
  | {
      replaceOne: {
        filter: CrudFilter<C>;
        replacement: C;
        upsert?: boolean;
        hint?: string | Document;
        collation?: CollationOptions;
      };
    };

export type BulkWriteBuilder<C extends Document, Mode extends ExecutionMode = "promise"> = {
  readonly _operations: readonly BulkWriteOp<C>[];
  execute(options?: { ordered?: boolean }): OperationResult<BulkWriteResult, Mode>;
};

export type DistinctBuilder<T, Mode extends ExecutionMode = "promise"> = {
  execute(): OperationResult<T[], Mode>;
};

// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
export interface CrudCollection<C extends Document, Mode extends ExecutionMode = "promise"> {
  find: {
    (): FindBuilder<C, Mode>;
    <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>, const O extends FindOptions<C> = {}>(
      filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
      options?: O & ValidateProjectionOptions<O>,
    ): FindBuilder<FindResult<C, O>, Mode>;
  };

  findOne: <
    const R extends NoInfer<ValidMatchFilterWithBuilder<C>>,
    const O extends FindOptions<C> = {},
  >(
    filter?: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    options?: O & ValidateProjectionOptions<O>,
  ) => FindBuilder<FindResult<C, O>, Mode>;

  insertOne: (doc: C) => InsertOneBuilder<C, Mode>;
  insertMany: (docs: readonly C[]) => InsertManyBuilder<C, Mode>;

  // Update supports update spec objects or pipeline callbacks
  updateOne: <
    const R extends NoInfer<ValidMatchFilterWithBuilder<C>>,
    const Update extends UpdateSpec<C> | UpdatePipelineCallback<C>,
  >(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    update: Update extends UpdatePipelineCallback<C> ? Update
    : Update & ValidateUpdateSpec<C, Update>,
    ...options: NoInfer<
      Update extends UpdatePipelineCallback<C> ? []
      : ExtractRequiredIdentifiers<Update> extends never ? [UpdateOpts<C, Update>?]
      : [UpdateOpts<C, Update>]
    >
  ) => UpdateOneBuilder<C, Update extends UpdatePipelineCallback<C> ? UpdateSpec<C> : Update, Mode>;

  // Update many supports update spec objects or pipeline callbacks
  updateMany: <
    const R extends NoInfer<ValidMatchFilterWithBuilder<C>>,
    const Update extends UpdateSpec<C> | UpdatePipelineCallback<C>,
  >(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    update: Update extends UpdatePipelineCallback<C> ? Update
    : Update & ValidateUpdateSpec<C, Update>,
    ...options: NoInfer<
      Update extends UpdatePipelineCallback<C> ? []
      : ExtractRequiredIdentifiers<Update> extends never ? [UpdateOpts<C, Update>?]
      : [UpdateOpts<C, Update>]
    >
  ) => UpdateManyBuilder<
    C,
    Update extends UpdatePipelineCallback<C> ? UpdateSpec<C> : Update,
    Mode
  >;

  replaceOne: <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    replacement: C,
    options?: unknown,
  ) => ReplaceBuilder<C, Mode>;

  deleteOne: <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
  ) => DeleteBuilder<C, Mode>;

  deleteMany: <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
  ) => DeleteBuilder<C, Mode>;

  findOneAndDelete: <
    const R extends NoInfer<ValidMatchFilterWithBuilder<C>>,
    const O extends FindOneAndOptions<C> = {},
  >(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    options?: O & ValidateProjectionOptions<O>,
  ) => FindOneAndDeleteBuilder<C, Mode, FindResult<C, O>>;

  findOneAndReplace: <
    const R extends NoInfer<ValidMatchFilterWithBuilder<C>>,
    const O extends FindOneAndOptions<C> = {},
  >(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    replacement: C,
    options?: O & ValidateProjectionOptions<O>,
  ) => FindOneAndReplaceBuilder<C, Mode, FindResult<C, O>>;

  findOneAndUpdate: <
    const R extends NoInfer<ValidMatchFilterWithBuilder<C>>,
    const Update extends UpdateSpec<C>,
    const O extends FindOneAndOptions<C> = {},
  >(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    update: Update & ValidateUpdateSpec<C, Update>,
    ...options: ExtractRequiredIdentifiers<Update> extends never ?
      [(NoInfer<UpdateOpts<C, Update>> & O & ValidateProjectionOptions<O>)?]
    : [NoInfer<UpdateOpts<C, Update>> & O & ValidateProjectionOptions<O>]
  ) => FindOneAndUpdateBuilder<C, Update, Mode, FindResult<C, O>>;

  countDocuments: {
    (): CountBuilder<Mode>;
    <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
      filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
      options?: CountOptions,
    ): CountBuilder<Mode>;
  };

  estimatedDocumentCount: () => CountBuilder<Mode>;

  distinct: <K extends keyof C & string>(
    field: K,
    filter?: ($: ExprBuilder<SimplifyWritable<C>>) => ValidMatchFilterWithBuilder<C>,
  ) => DistinctBuilder<C[K] extends readonly (infer E)[] ? E : C[K], Mode>;

  bulkWrite: (
    operations: readonly BulkWriteOp<C>[],
    options?: { ordered?: boolean },
  ) => BulkWriteBuilder<C, Mode>;
}
