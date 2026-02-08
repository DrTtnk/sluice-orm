import type {
  BulkWriteResult,
  CollationOptions,
  DeleteResult,
  Document,
  InsertManyResult,
  InsertOneResult,
  UpdateResult,
} from "mongodb";

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

export type FindBuilder<C> = {
  readonly _filter: CrudFilter<C> | undefined;
  readonly _options: FindOptions<C> | undefined;
  toList(): Promise<C[]>;
  toOne(): Promise<C | null>;
};

export type InsertOneBuilder<C> = {
  readonly _doc: C;
  execute(): Promise<InsertOneResult>;
};

export type InsertManyBuilder<C> = {
  readonly _docs: readonly C[];
  execute(): Promise<InsertManyResult>;
};

export type UpdateOneBuilder<C extends Document, U extends UpdateSpec<C> = UpdateSpec<C>> = {
  readonly _filter: CrudFilter<C>;
  readonly _update: U | (($: ExprBuilder<C>) => U);
  readonly _options: UpdateOpts<C, U> | undefined;
  execute(): Promise<UpdateResult>;
};

export type UpdateManyBuilder<C extends Document, U extends UpdateSpec<C> = UpdateSpec<C>> = {
  readonly _filter: CrudFilter<C>;
  readonly _update: U | (($: ExprBuilder<C>) => U);
  readonly _options: UpdateOpts<C, U> | undefined;
  execute(): Promise<UpdateResult>;
};

export type ReplaceBuilder<C> = {
  readonly _filter: CrudFilter<C>;
  readonly _replacement: C;
  readonly _options: unknown;
  execute(): Promise<UpdateResult>;
};

export type DeleteBuilder<C> = {
  readonly _filter: CrudFilter<C>;
  execute(): Promise<DeleteResult>;
};

export type FindOneAndDeleteBuilder<C> = {
  readonly _filter: CrudFilter<C>;
  execute(): Promise<C | null>;
};

export type FindOneAndReplaceBuilder<C> = {
  readonly _filter: CrudFilter<C>;
  readonly _replacement: C;
  execute(): Promise<C | null>;
};

export type FindOneAndUpdateBuilder<C extends Document, U extends UpdateSpec<C> = UpdateSpec<C>> = {
  readonly _filter: CrudFilter<C>;
  readonly _update: U;
  execute(): Promise<C | null>;
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

export type CountBuilder = {
  execute(): Promise<number>;
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

export type BulkWriteBuilder<C extends Document> = {
  readonly _operations: readonly BulkWriteOp<C>[];
  execute(options?: { ordered?: boolean }): Promise<BulkWriteResult>;
};

export type DistinctBuilder<T> = {
  execute(): Promise<T[]>;
};

// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
export interface CrudCollection<C extends Document> {
  find: {
    (): FindBuilder<C>;
    <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
      filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
      options?: FindOptions<C>,
    ): FindBuilder<C>;
  };

  findOne: <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
    filter?: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    options?: FindOptions<C>,
  ) => FindBuilder<C>;

  insertOne: (doc: C) => InsertOneBuilder<C>;
  insertMany: (docs: readonly C[]) => InsertManyBuilder<C>;

  // Update supports update spec objects or pipeline callbacks
  updateOne: <
    const R extends NoInfer<ValidMatchFilterWithBuilder<C>>,
    const Update extends UpdateSpec<C> | UpdatePipelineCallback<C>,
  >(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    // @ts-expect-error - Conditional type inference for update spec vs pipeline callback
    update: Update extends UpdateSpec<C> ? Update & ValidateUpdateSpec<C, Update>
    : Update extends UpdatePipelineCallback<C> ? Update
    : never,
    ...options: NoInfer<
      Update extends UpdateSpec<C> ?
        ExtractRequiredIdentifiers<Update> extends never ?
          [UpdateOpts<C, Update>?]
        : [UpdateOpts<C, Update>]
      : []
    >
  ) => UpdateOneBuilder<C, Update extends UpdateSpec<C> ? Update : UpdateSpec<C>>;

  // Update many supports update spec objects or pipeline callbacks
  updateMany: <
    const R extends NoInfer<ValidMatchFilterWithBuilder<C>>,
    const Update extends UpdateSpec<C> | UpdatePipelineCallback<C>,
  >(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    update: Update extends UpdateSpec<C> ? Update & ValidateUpdateSpec<C, Update>
    : Update extends UpdatePipelineCallback<C> ? Update
    : never,
    ...options: NoInfer<
      Update extends UpdateSpec<C> ?
        ExtractRequiredIdentifiers<Update> extends never ?
          [UpdateOpts<C, Update>?]
        : [UpdateOpts<C, Update>]
      : []
    >
  ) => UpdateManyBuilder<C, Update extends UpdateSpec<C> ? Update : UpdateSpec<C>>;

  replaceOne: <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    replacement: C,
    options?: unknown,
  ) => ReplaceBuilder<C>;

  deleteOne: <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
  ) => DeleteBuilder<C>;

  deleteMany: <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
  ) => DeleteBuilder<C>;

  findOneAndDelete: <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    options?: FindOneAndOptions<C>,
  ) => FindOneAndDeleteBuilder<C>;

  findOneAndReplace: <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    replacement: C,
    options?: FindOneAndOptions<C>,
  ) => FindOneAndReplaceBuilder<C>;

  findOneAndUpdate: <
    const R extends NoInfer<ValidMatchFilterWithBuilder<C>>,
    const Update extends UpdateSpec<C>,
  >(
    filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
    update: Update & ValidateUpdateSpec<C, Update>,
    ...options: NoInfer<
      ExtractRequiredIdentifiers<Update> extends never ?
        [(UpdateOpts<C, Update> & FindOneAndOptions<C>)?]
      : [UpdateOpts<C, Update> & FindOneAndOptions<C>]
    >
  ) => FindOneAndUpdateBuilder<C, Update>;

  countDocuments: {
    (): CountBuilder;
    <const R extends NoInfer<ValidMatchFilterWithBuilder<C>>>(
      filter: ($: ExprBuilder<SimplifyWritable<C>>) => R,
      options?: CountOptions,
    ): CountBuilder;
  };

  estimatedDocumentCount: () => CountBuilder;

  distinct: <K extends keyof C & string>(
    field: K,
    filter?: ($: ExprBuilder<SimplifyWritable<C>>) => ValidMatchFilterWithBuilder<C>,
  ) => DistinctBuilder<C[K]>;

  bulkWrite: (
    operations: readonly BulkWriteOp<C>[],
    options?: { ordered?: boolean },
  ) => BulkWriteBuilder<C>;
}
