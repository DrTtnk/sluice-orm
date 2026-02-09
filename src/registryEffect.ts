import { Context, Data, Effect, Layer } from "effect";
import type {
  AnyBulkWriteOperation,
  BulkWriteOptions,
  BulkWriteResult,
  Collection as MongoCollection,
  CountDocumentsOptions,
  Db,
  DeleteResult,
  Document,
  Filter,
  FindOneAndDeleteOptions,
  FindOneAndReplaceOptions,
  FindOneAndUpdateOptions,
  FindOneOptions,
  InsertManyResult,
  InsertOneResult,
  ReplaceOptions,
  Sort,
  UpdateOptions,
  UpdateResult,
} from "mongodb";

import type { FindOptions } from "./crud.js";
import { update } from "./crud/updates/stages/index.js";
import type { Dict, SimplifyWritable } from "./type-utils.js";

// ==========================================
// Tagged Error Types
// ==========================================

export class MongoError extends Data.TaggedError("MongoError")<{
  readonly operation: string;
  readonly cause: unknown;
  readonly message: string;
}> {}

// ==========================================
// MongoDB Client Service
// ==========================================

export class MongoDbClient extends Context.Tag("MongoDbClient")<
  MongoDbClient,
  { readonly db: Db }
>() {}

// ==========================================
// DRY Utility for Promise Wrapping
// ==========================================

const wrapMongoOperation = <T>(
  operation: string,
  fn: () => Promise<T>,
): Effect.Effect<T, MongoError> =>
  Effect.tryPromise({
    try: fn,
    catch: cause =>
      new MongoError({
        operation,
        cause,
        message: cause instanceof Error ? cause.message : String(cause),
      }),
  });

// ==========================================
// Schema-Agnostic Interface
// ==========================================

export type SchemaLike =
  | { readonly Type: Dict<unknown> }
  | { readonly _output: Dict<unknown> }
  | { readonly [schemaTypeKey]: Dict<unknown> };

declare const schemaTypeKey: unique symbol;

export type InferSchema<S extends SchemaLike> =
  S extends { readonly Type: infer T extends Dict<unknown> } ? T
  : S extends { readonly _output: infer T extends Dict<unknown> } ? T
  : S extends { readonly [schemaTypeKey]: infer T extends Dict<unknown> } ? T
  : Dict<unknown>;

type MongoVersions = ["4.2", "4.4", "5.0", "5.1", "6.0", "6.1", "7.0", "7.1", "8.0"];

export type Collection<
  TName extends string = string,
  TSchema extends Dict<unknown> = Dict<unknown>,
> = {
  readonly __collectionName: TName;
  readonly __collectionType: SimplifyWritable<TSchema>;
};

export type CollectionType<T extends Collection> = T["__collectionType"];

// ==========================================
// Effect-based CRUD Interface
// ==========================================

export type CrudCollectionEffect<T extends Document = Document> = {
  find: {
    (filterFn?: ($: Record<string, never>) => Filter<T>): {
      toList: () => Effect.Effect<T[], MongoError>;
      toOne: () => Effect.Effect<T | null, MongoError>;
    };
    (
      filterFn: ($: Record<string, never>) => Filter<T>,
      options: FindOptions<T>,
    ): {
      toList: () => Effect.Effect<T[], MongoError>;
      toOne: () => Effect.Effect<T | null, MongoError>;
    };
  };

  findOne: {
    (filterFn?: ($: Record<string, never>) => Filter<T>): {
      toList: () => Effect.Effect<T[], MongoError>;
      toOne: () => Effect.Effect<T | null, MongoError>;
    };
    (
      filterFn: ($: Record<string, never>) => Filter<T>,
      options: FindOptions<T>,
    ): {
      toList: () => Effect.Effect<T[], MongoError>;
      toOne: () => Effect.Effect<T | null, MongoError>;
    };
  };

  insertOne: (doc: T) => {
    execute: () => Effect.Effect<InsertOneResult<T>, MongoError>;
  };

  insertMany: (docs: readonly T[]) => {
    execute: () => Effect.Effect<InsertManyResult<T>, MongoError>;
  };

  updateOne: (
    filterFn: ($: Record<string, never>) => Filter<T>,
    update: object | (($: unknown) => unknown),
    options?: { upsert?: boolean; hint?: unknown; arrayFilters?: unknown },
  ) => {
    execute: () => Effect.Effect<UpdateResult<T>, MongoError>;
  };

  updateMany: (
    filterFn: ($: Record<string, never>) => Filter<T>,
    update: object | (($: unknown) => unknown),
    options?: { upsert?: boolean; hint?: unknown; arrayFilters?: unknown },
  ) => {
    execute: () => Effect.Effect<UpdateResult<T>, MongoError>;
  };

  replaceOne: (
    filterFn: ($: Record<string, never>) => Filter<T>,
    replacement: T,
    options?: unknown,
  ) => {
    execute: () => Effect.Effect<UpdateResult<T>, MongoError>;
  };

  deleteOne: (filterFn: ($: Record<string, never>) => Filter<T>) => {
    execute: () => Effect.Effect<DeleteResult, MongoError>;
  };

  deleteMany: (filterFn: ($: Record<string, never>) => Filter<T>) => {
    execute: () => Effect.Effect<DeleteResult, MongoError>;
  };

  findOneAndDelete: (
    filterFn: ($: Record<string, never>) => Filter<T>,
    options?: {
      sort?: unknown;
      projection?: unknown;
      returnDocument?: string;
      hint?: unknown;
      collation?: unknown;
      maxTimeMS?: number;
      comment?: string;
    },
  ) => {
    execute: () => Effect.Effect<T | null, MongoError>;
  };

  findOneAndReplace: (
    filterFn: ($: Record<string, never>) => Filter<T>,
    replacement: T,
    options?: {
      sort?: unknown;
      projection?: unknown;
      upsert?: boolean;
      returnDocument?: string;
      hint?: unknown;
      collation?: unknown;
      maxTimeMS?: number;
      comment?: string;
    },
  ) => {
    execute: () => Effect.Effect<T | null, MongoError>;
  };

  findOneAndUpdate: (
    filterFn: ($: Record<string, never>) => Filter<T>,
    update: unknown,
    options?: {
      sort?: unknown;
      projection?: unknown;
      upsert?: boolean;
      returnDocument?: string;
      arrayFilters?: unknown;
      hint?: unknown;
      collation?: unknown;
      maxTimeMS?: number;
      comment?: string;
    },
  ) => {
    execute: () => Effect.Effect<T | null, MongoError>;
  };

  countDocuments: (
    filterFn?: ($: Record<string, never>) => Filter<T>,
    options?: {
      limit?: number;
      skip?: number;
      maxTimeMS?: number;
      hint?: unknown;
      collation?: unknown;
      comment?: string;
    },
  ) => {
    execute: () => Effect.Effect<number, MongoError>;
  };

  estimatedDocumentCount: () => {
    execute: () => Effect.Effect<number, MongoError>;
  };

  distinct: (
    field: string,
    filterFn?: ($: Record<string, never>) => Filter<T>,
  ) => {
    execute: () => Effect.Effect<unknown[], MongoError>;
  };

  bulkWrite: (
    operations: readonly AnyBulkWriteOperation<T>[],
    options?: { ordered?: boolean },
  ) => {
    execute: () => Effect.Effect<BulkWriteResult, MongoError>;
  };
};

export type BoundCollectionEffect<TName extends string, TSchema extends Dict<unknown>> = Collection<
  TName,
  TSchema
> & {
  aggregate: AggregateBuilderEffect<SimplifyWritable<TSchema>>;
} & CrudCollectionEffect<SimplifyWritable<TSchema>>;

type AggregateBuilderEffect<T extends Document> = (...stages: unknown[]) => {
  toList: () => Effect.Effect<T[], MongoError>;
  toMQL: () => string;
};

// ==========================================
// CRUD Runtime Implementation with Effect
// ==========================================

function createCrudMethodsEffect<T extends Document>(
  mongoCol: MongoCollection<T>,
): CrudCollectionEffect<T> {
  const extractFilter = (filterFn: unknown): Filter<T> => {
    if (!filterFn) return {} as Filter<T>;
    return (filterFn as ($: unknown) => unknown)({}) as Filter<T>;
  };

  return {
    find: ((filterFn?: unknown, options?: FindOptions<T>) => ({
      toList: () =>
        wrapMongoOperation("find.toList", () => {
          const filter = filterFn ? extractFilter(filterFn) : {};
          let cursor = mongoCol.find(filter as Filter<T>);
          if (options?.sort) cursor = cursor.sort(options.sort as Sort);
          if (options?.skip) cursor = cursor.skip(options.skip);
          if (options?.limit) cursor = cursor.limit(options.limit);
          if (options?.projection) cursor = cursor.project(options.projection);
          if (options?.hint) cursor = cursor.hint(options.hint as string);
          if (options?.maxTimeMS) cursor = cursor.maxTimeMS(options.maxTimeMS);
          if (options?.collation) cursor = cursor.collation(options.collation);
          if (options?.comment) cursor = cursor.comment(options.comment);
          return cursor.toArray() as unknown as Promise<T[]>;
        }),
      toOne: () =>
        wrapMongoOperation("find.toOne", () => {
          const filter = filterFn ? extractFilter(filterFn) : {};
          return mongoCol.findOne(
            filter as Filter<T>,
            options as FindOneOptions,
          ) as unknown as Promise<T | null>;
        }),
    })) as CrudCollectionEffect<T>["find"],

    findOne: ((filterFn?: unknown, options?: FindOptions<T>) => ({
      toList: () =>
        wrapMongoOperation("findOne.toList", async () => {
          const doc = await mongoCol.findOne(
            filterFn ? extractFilter(filterFn) : ({} as Filter<T>),
            options as FindOneOptions,
          );
          return doc ? [doc as T] : [];
        }),
      toOne: () =>
        wrapMongoOperation(
          "findOne.toOne",
          () =>
            mongoCol.findOne(
              filterFn ? extractFilter(filterFn) : ({} as Filter<T>),
              options as FindOneOptions,
            ) as unknown as Promise<T | null>,
        ),
    })) as CrudCollectionEffect<T>["findOne"],

    insertOne: doc => ({
      execute: () => wrapMongoOperation("insertOne", () => mongoCol.insertOne(doc as never)),
    }),

    insertMany: docs => ({
      execute: () => wrapMongoOperation("insertMany", () => mongoCol.insertMany(docs as never[])),
    }),

    updateOne: ((
      filterFn: unknown,
      updateArg: unknown,
      options?: { upsert?: boolean; hint?: unknown; arrayFilters?: unknown },
    ) => {
      const filter = extractFilter(filterFn);
      const updateDoc =
        typeof updateArg === "function" ?
          (updateArg(update()) as { stages: readonly object[] }).stages
        : updateArg;

      return {
        execute: () =>
          wrapMongoOperation("updateOne", () =>
            mongoCol.updateOne(filter, updateDoc as Document, options as UpdateOptions),
          ),
      };
    }) as CrudCollectionEffect<T>["updateOne"],

    updateMany: ((
      filterFn: unknown,
      updateArg: unknown,
      options?: { upsert?: boolean; hint?: unknown; arrayFilters?: unknown },
    ) => {
      const filter = extractFilter(filterFn);
      const updateDoc =
        typeof updateArg === "function" ?
          (updateArg(update()) as { stages: readonly object[] }).stages
        : updateArg;

      return {
        execute: () =>
          wrapMongoOperation("updateMany", () =>
            mongoCol.updateMany(filter, updateDoc as Document, options as UpdateOptions),
          ),
      };
    }) as CrudCollectionEffect<T>["updateMany"],

    replaceOne: ((filterFn: unknown, replacement: T, options?: unknown) => ({
      execute: () =>
        wrapMongoOperation("replaceOne", () =>
          mongoCol.replaceOne(
            extractFilter(filterFn),
            replacement as never,
            options as ReplaceOptions,
          ),
        ),
    })) as CrudCollectionEffect<T>["replaceOne"],

    deleteOne: ((filterFn: unknown) => ({
      execute: () =>
        wrapMongoOperation("deleteOne", () => mongoCol.deleteOne(extractFilter(filterFn))),
    })) as CrudCollectionEffect<T>["deleteOne"],

    deleteMany: ((filterFn: unknown) => ({
      execute: () =>
        wrapMongoOperation("deleteMany", () => mongoCol.deleteMany(extractFilter(filterFn))),
    })) as CrudCollectionEffect<T>["deleteMany"],

    findOneAndDelete: ((
      filterFn: unknown,
      options?: {
        sort?: unknown;
        projection?: unknown;
        returnDocument?: string;
        hint?: unknown;
        collation?: unknown;
        maxTimeMS?: number;
        comment?: string;
      },
    ) => ({
      execute: () =>
        wrapMongoOperation("findOneAndDelete", () =>
          mongoCol
            .findOneAndDelete(extractFilter(filterFn), options as FindOneAndDeleteOptions)
            .then(r => r as T | null),
        ),
    })) as CrudCollectionEffect<T>["findOneAndDelete"],

    findOneAndReplace: ((
      filterFn: unknown,
      replacement: T,
      options?: {
        sort?: unknown;
        projection?: unknown;
        upsert?: boolean;
        returnDocument?: string;
        hint?: unknown;
        collation?: unknown;
        maxTimeMS?: number;
        comment?: string;
      },
    ) => ({
      execute: () =>
        wrapMongoOperation("findOneAndReplace", () =>
          mongoCol
            .findOneAndReplace(
              extractFilter(filterFn),
              replacement as never,
              options as FindOneAndReplaceOptions,
            )
            .then(r => r as T | null),
        ),
    })) as CrudCollectionEffect<T>["findOneAndReplace"],

    findOneAndUpdate: ((
      filterFn: unknown,
      updateArg: unknown,
      options?: {
        sort?: unknown;
        projection?: unknown;
        upsert?: boolean;
        returnDocument?: string;
        arrayFilters?: unknown;
        hint?: unknown;
        collation?: unknown;
        maxTimeMS?: number;
        comment?: string;
      },
    ) => ({
      execute: () =>
        wrapMongoOperation("findOneAndUpdate", () =>
          mongoCol
            .findOneAndUpdate(
              extractFilter(filterFn),
              updateArg as Document,
              options as FindOneAndUpdateOptions,
            )
            .then(r => r as T | null),
        ),
    })) as CrudCollectionEffect<T>["findOneAndUpdate"],

    countDocuments: ((
      filterFn?: unknown,
      options?: {
        limit?: number;
        skip?: number;
        maxTimeMS?: number;
        hint?: unknown;
        collation?: unknown;
        comment?: string;
      },
    ) => ({
      execute: () =>
        wrapMongoOperation("countDocuments", () =>
          mongoCol.countDocuments(
            filterFn ? extractFilter(filterFn) : {},
            options as CountDocumentsOptions,
          ),
        ),
    })) as CrudCollectionEffect<T>["countDocuments"],

    estimatedDocumentCount: () => ({
      execute: () =>
        wrapMongoOperation("estimatedDocumentCount", () => mongoCol.estimatedDocumentCount()),
    }),

    distinct: ((field: string, filterFn?: unknown) => ({
      execute: () =>
        wrapMongoOperation("distinct", () =>
          mongoCol.distinct(field, filterFn ? extractFilter(filterFn) : ({} as Filter<T>)),
        ),
    })) as CrudCollectionEffect<T>["distinct"],

    bulkWrite: ((operations: readonly unknown[], options?: { ordered?: boolean }) => ({
      execute: () =>
        wrapMongoOperation("bulkWrite", () =>
          mongoCol.bulkWrite(operations as AnyBulkWriteOperation<T>[], options as BulkWriteOptions),
        ),
    })) as unknown as CrudCollectionEffect<T>["bulkWrite"],
  };
}

// ==========================================
// Collection Helper Function with Effect
// ==========================================

export const collectionEffect = <TName extends string, TSchema extends SchemaLike>(
  name: TName,
  _schema: TSchema,
  mongoCol: MongoCollection<SimplifyWritable<InferSchema<TSchema>>>,
): BoundCollectionEffect<TName, InferSchema<TSchema>> => {
  const col: Collection<TName, InferSchema<TSchema>> = {
    __collectionName: name,
    __collectionType: {} as SimplifyWritable<InferSchema<TSchema>>,
  };

  const crud = createCrudMethodsEffect(mongoCol);

  return Object.assign(col, {
    aggregate: ((...stages: unknown[]) => {
      let agg = { collectionName: name, stages: [] };
      for (const s of stages) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-function-type
        agg = (s as Function)(agg);
      }

      return {
        ...agg,
        toList: () =>
          wrapMongoOperation("aggregate", () =>
            mongoCol.aggregate(agg.stages as Document[]).toArray(),
          ),
        toMQL: () => JSON.stringify(agg.stages, null, 2),
      };
    }) as unknown as AggregateBuilderEffect<SimplifyWritable<InferSchema<TSchema>>>,
    ...crud,
  });
};

// ==========================================
// Registry Function with Effect + Layer
// ==========================================

/**
 * Creates a type-safe registry of MongoDB collections with Effect-based operations using dependency injection.
 *
 * Returns a factory that creates an Effect which requires MongoDbClient service,
 * producing collections with fully-typed aggregation pipelines and CRUD operations.
 *
 * @example
 * ```ts
 * import { Effect, Layer } from "effect";
 * import { MongoClient } from "mongodb";
 *
 * // Define your registry
 * const makeRegistry = registryEffect("8.0", {
 *   users: userSchema,
 *   orders: orderSchema,
 * });
 *
 * // Create the client layer
 * const client = await MongoClient.connect("mongodb://localhost:27017");
 * const MongoDbClientLive = Layer.succeed(MongoDbClient, { db: client.db("mydb") });
 *
 * // Use with Effect.gen
 * const program = Effect.gen(function* () {
 *   const registry = yield* makeRegistry;
 *   const users = yield* registry.users.find(() => ({ age: { $gte: 18 } })).toList();
 *   return users;
 * });
 *
 * // Run with the layer provided
 * const result = await Effect.runPromise(program.pipe(Effect.provide(MongoDbClientLive)));
 * ```
 */
export const registryEffect = <
  const TMap extends Dict<SchemaLike>,
  const TVersion extends MongoVersions[number],
>(
  _version: TVersion,
  schemas: { [K in keyof TMap]: TMap[K] },
) =>
  Effect.gen(function* () {
    const { db: client } = yield* MongoDbClient;

    const bind = <TName extends string, TSchema extends SchemaLike>(
      name: TName,
      schema: TSchema,
    ): BoundCollectionEffect<TName, InferSchema<TSchema>> =>
      collectionEffect(
        name,
        schema,
        client.collection<SimplifyWritable<InferSchema<TSchema>>>(name),
      );

    return Object.fromEntries(
      Object.entries(schemas).map(([name, schema]) => [name, bind(name, schema)]),
    ) as unknown as { [K in keyof TMap]: BoundCollectionEffect<K & string, InferSchema<TMap[K]>> };
  });

/**
 * Creates a Layer that provides MongoDbClient service.
 *
 * @example
 * ```ts
 * import { MongoClient } from "mongodb";
 *
 * const client = await MongoClient.connect("mongodb://localhost:27017");
 * const mongoLayer = makeMongoDbClientLayer(client.db("mydb"));
 * ```
 */
export const makeMongoDbClientLayer = (db: Db): Layer.Layer<MongoDbClient> =>
  Layer.succeed(MongoDbClient, { db });
