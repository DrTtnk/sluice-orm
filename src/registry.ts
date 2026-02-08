import type {
  AnyBulkWriteOperation,
  BulkWriteOptions,
  Collection as MongoCollection,
  CountDocumentsOptions,
  Db,
  Document,
  Filter,
  FindOneAndDeleteOptions,
  FindOneAndReplaceOptions,
  FindOneAndUpdateOptions,
  FindOneOptions,
  ReplaceOptions,
  Sort,
  UpdateOptions,
} from "mongodb";

import type { CrudCollection, FindOptions } from "./crud.js";
import { update } from "./crud/updates/stages/index.js";
import type { AggregateBuilder } from "./pipeline-types.js";
import type { Dict, SimplifyWritable } from "./type-utils.js";

// ==========================================
// Schema-Agnostic Interface
// ==========================================

/**
 * A schema-like object from which a document TypeScript type can be extracted.
 *
 * Compatible with:
 * - **Zod**: `z.object({ name: z.string() })` → inferred via `_output`
 * - **Effect Schema**: `Schema.Struct({ name: Schema.String })` → inferred via `Type`
 * - **Plain type marker**: `{ Type: MyType }` for manual schemas
 *
 * @example
 * ```ts
 * import { z } from "zod";
 * const UserSchema = z.object({ name: z.string(), age: z.number() });
 * const reg = registry("8.0", { users: UserSchema });
 * ```
 */
export type SchemaLike =
  | { readonly Type: Dict<unknown> }              // Effect Schema
  | { readonly _output: Dict<unknown> }           // Zod
  | { readonly [schemaTypeKey]: Dict<unknown> };   // Custom adapter

declare const schemaTypeKey: unique symbol;

/** Extract the inferred document type from a SchemaLike */
export type InferSchema<S extends SchemaLike> =
  S extends { readonly Type: infer T extends Dict<unknown> } ? T
  : S extends { readonly _output: infer T extends Dict<unknown> } ? T
  : S extends { readonly [schemaTypeKey]: infer T extends Dict<unknown> } ? T
  : Dict<unknown>;

type MongoVersions = ["4.2", "4.4", "5.0", "5.1", "6.0", "6.1", "7.0", "7.1", "8.0"];

/**
 * Collection type that carries both name and schema information
 * Used for type-safe collection references in $lookup, $unionWith, etc.
 */
export type Collection<
  TName extends string = string,
  TSchema extends Dict<unknown> = Dict<unknown>,
> = {
  readonly __collectionName: TName;
  readonly __collectionType: SimplifyWritable<TSchema>;
};

export type CollectionType<T extends Collection> = T["__collectionType"];

export type BoundCollection<TName extends string, TSchema extends Dict<unknown>> = Collection<
  TName,
  TSchema
> & {
  aggregate: AggregateBuilder<SimplifyWritable<TSchema>>;
} & CrudCollection<SimplifyWritable<TSchema>>;

// ==========================================
// CRUD Runtime Implementation
// ==========================================

/**
 * Creates CRUD methods for a collection - no proxies, direct implementation
 */
function createCrudMethods<T extends Document>(mongoCol: MongoCollection<T>): CrudCollection<T> {
  // Helper to extract filter from callback - runtime only, types handled by interface
  const extractFilter = (filterFn: unknown): Filter<T> => {
    if (!filterFn) return {} as Filter<T>;
    // The callback returns a MongoDB filter document directly
    return (filterFn as ($: unknown) => unknown)({}) as Filter<T>;
  };

  return {
    find: ((filterFn?: unknown, options?: FindOptions<T>) => ({
      _filter: filterFn ? extractFilter(filterFn) : undefined,
      _options: options,
      toList: () => {
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
      },
      toOne: async () => {
        const filter = filterFn ? extractFilter(filterFn) : {};
        return mongoCol.findOne(
          filter as Filter<T>,
          options as FindOneOptions,
        ) as unknown as Promise<T | null>;
      },
    })) as CrudCollection<T>["find"],

    findOne: ((filterFn?: unknown, options?: FindOptions<T>) => ({
      _filter: filterFn ? extractFilter(filterFn) : undefined,
      _options: options,
      toList: async () => {
        const doc = await mongoCol.findOne(
          filterFn ? extractFilter(filterFn) : ({} as Filter<T>),
          options as FindOneOptions,
        );
        return doc ? [doc as T] : [];
      },
      toOne: () =>
        mongoCol.findOne(
          filterFn ? extractFilter(filterFn) : ({} as Filter<T>),
          options as FindOneOptions,
        ) as unknown as Promise<T | null>,
    })) as CrudCollection<T>["findOne"],

    insertOne: doc => ({
      _doc: doc,
      execute: () => mongoCol.insertOne(doc as never),
    }),

    insertMany: docs => ({
      _docs: docs,
      execute: () => mongoCol.insertMany(docs as never[]),
    }),

    updateOne: ((
      filterFn: unknown,
      updateArg: unknown,
      options?: { upsert?: boolean; hint?: unknown; arrayFilters?: unknown },
    ) => {
      const filter = extractFilter(filterFn);
      // Resolve the update: callback returns TypedPipeline, or update spec object
      const updateDoc =
        typeof updateArg === "function" ?
          // Callback form: invoke with operators, get stages from TypedPipeline
          (updateArg(update()) as { stages: readonly object[] }).stages
        : updateArg;

      return {
        _filter: filter,
        _update: updateDoc,
        _options: options,
        execute: () => mongoCol.updateOne(filter, updateDoc as Document, options as UpdateOptions),
      };
    }) as CrudCollection<T>["updateOne"],

    updateMany: ((
      filterFn: unknown,
      updateArg: unknown,
      options?: { upsert?: boolean; hint?: unknown; arrayFilters?: unknown },
    ) => {
      const filter = extractFilter(filterFn);
      // Resolve the update: callback returns TypedPipeline, or update spec object
      const updateDoc =
        typeof updateArg === "function" ?
          // Callback form: invoke with operators, get stages from TypedPipeline
          (updateArg(update()) as { stages: readonly object[] }).stages
        : updateArg;

      return {
        _filter: filter,
        _update: updateDoc,
        _options: options,
        execute: () => mongoCol.updateMany(filter, updateDoc as Document, options as UpdateOptions),
      };
    }) as CrudCollection<T>["updateMany"],

    replaceOne: ((filterFn: unknown, replacement: T, options?: unknown) => ({
      _filter: extractFilter(filterFn),
      _replacement: replacement,
      _options: options,
      execute: () =>
        mongoCol.replaceOne(
          extractFilter(filterFn),
          replacement as never,
          options as ReplaceOptions,
        ),
    })) as CrudCollection<T>["replaceOne"],

    deleteOne: ((filterFn: unknown) => ({
      _filter: extractFilter(filterFn),
      execute: () => mongoCol.deleteOne(extractFilter(filterFn)),
    })) as CrudCollection<T>["deleteOne"],

    deleteMany: ((filterFn: unknown) => ({
      _filter: extractFilter(filterFn),
      execute: () => mongoCol.deleteMany(extractFilter(filterFn)),
    })) as CrudCollection<T>["deleteMany"],

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
      _filter: extractFilter(filterFn),
      execute: () =>
        mongoCol
          .findOneAndDelete(extractFilter(filterFn), options as FindOneAndDeleteOptions)
          .then(r => r as T | null),
    })) as CrudCollection<T>["findOneAndDelete"],

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
      _filter: extractFilter(filterFn),
      _replacement: replacement,
      execute: () =>
        mongoCol
          .findOneAndReplace(
            extractFilter(filterFn),
            replacement as never,
            options as FindOneAndReplaceOptions,
          )
          .then(r => r as T | null),
    })) as CrudCollection<T>["findOneAndReplace"],

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
      _filter: extractFilter(filterFn),
      _update: updateArg,
      execute: () =>
        mongoCol
          .findOneAndUpdate(
            extractFilter(filterFn),
            updateArg as Document,
            options as FindOneAndUpdateOptions,
          )
          .then(r => r as T | null),
    })) as CrudCollection<T>["findOneAndUpdate"],

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
        mongoCol.countDocuments(
          filterFn ? extractFilter(filterFn) : {},
          options as CountDocumentsOptions,
        ),
    })) as CrudCollection<T>["countDocuments"],

    estimatedDocumentCount: () => ({
      execute: () => mongoCol.estimatedDocumentCount(),
    }),

    distinct: ((field: string, filterFn?: unknown) => ({
      execute: () =>
        mongoCol.distinct(field, filterFn ? extractFilter(filterFn) : ({} as Filter<T>)),
    })) as CrudCollection<T>["distinct"],

    bulkWrite: ((operations: readonly unknown[], options?: { ordered?: boolean }) => ({
      _operations: operations,
      execute: () =>
        mongoCol.bulkWrite(operations as AnyBulkWriteOperation<T>[], options as BulkWriteOptions),
    })) as unknown as CrudCollection<T>["bulkWrite"],
  };
}

// ==========================================
// Collection Helper Function
// ==========================================

/**
 * Creates a bound collection with type-safe aggregate and CRUD methods.
 *
 * Wraps a MongoDB native collection with schema-aware operations.
 * Typically called indirectly via {@link registry} rather than directly.
 *
 * @example
 * ```ts
 * const users = collection("users", userSchema, db.collection("users"));
 * const result = await users.aggregate($match($ => ({ age: $.gte(18) }))).toList();
 * ```
 */
export const collection = <TName extends string, TSchema extends SchemaLike>(
  name: TName,
  _schema: TSchema,
  mongoCol: MongoCollection<SimplifyWritable<InferSchema<TSchema>>>,
): BoundCollection<TName, InferSchema<TSchema>> => {
  const col: Collection<TName, InferSchema<TSchema>> = {
    __collectionName: name,
    __collectionType: {} as SimplifyWritable<InferSchema<TSchema>>,
  };

  const crud = createCrudMethods(mongoCol);

  return Object.assign(col, {
    aggregate: ((...stages: unknown[]) => {
      let agg = { collectionName: name, stages: [] };
      for (const s of stages) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-function-type
        agg = (s as Function)(agg);
      }
       
      return {
        ...agg,
        toList: () => mongoCol.aggregate(agg.stages as Document[]).toArray(),
        toMQL: () => JSON.stringify(agg.stages, null, 2),
      };
    }) as unknown as AggregateBuilder<SimplifyWritable<InferSchema<TSchema>>>,
    ...crud,
  });
};

// ==========================================
// Registry Function
// ==========================================

/**
 * Creates a type-safe registry of MongoDB collections from schema definitions.
 *
 * Returns a factory that binds schemas to a `Db` instance, producing
 * collections with fully-typed aggregation pipelines and CRUD operations.
 *
 * @example
 * ```ts
 * const db = registry("8.0", {
 *   users: userSchema,
 *   orders: orderSchema,
 * })(client.db("mydb"));
 *
 * const result = await db.users.aggregate(
 *   $match($ => ({ active: true })),
 *   $project($ => ({ name: 1 })),
 * ).toList();
 * ```
 */
export const registry =
  <const TMap extends Dict<SchemaLike>, const TVersion extends MongoVersions[number]>(
    version: TVersion, // For later on when we have to deal with version-specific features
    schemas: { [K in keyof TMap]: TMap[K] },
  ) =>
  (client: Db): { [K in keyof TMap]: BoundCollection<K & string, InferSchema<TMap[K]>> } => {
    const bind = <TName extends string, TSchema extends SchemaLike>(
      name: TName,
      schema: TSchema,
    ): BoundCollection<TName, InferSchema<TSchema>> =>
      collection(name, schema, client.collection<SimplifyWritable<InferSchema<TSchema>>>(name));

    return Object.fromEntries(
      Object.entries(schemas).map(([name, schema]) => [name, bind(name, schema)]),
    ) as unknown as { [K in keyof TMap]: BoundCollection<K & string, InferSchema<TMap[K]>> };
  };
