import { Context, Data, Effect, Layer } from "effect";
import type { Collection as MongoCollection, Db, Document } from "mongodb";

import type { CrudCollection } from "./crud.js";
import type { AggregateBuilder } from "./pipeline-types.js";
import { type Collection, collection, type InferSchema, type SchemaLike } from "./registry.js";
import type { Dict, SimplifyWritable } from "./type-utils.js";

export class MongoError extends Data.TaggedError("MongoError")<{
  readonly operation: string;
  readonly cause: unknown;
  readonly message: string;
}> {}

export class MongoDbClient extends Context.Tag("MongoDbClient")<
  MongoDbClient,
  { readonly db: Db }
>() {}

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

export type CrudCollectionEffect<T extends Document = Document> = CrudCollection<T, "effect">;
export type BoundCollectionEffect<TName extends string, TSchema extends Dict<unknown>> = Collection<
  TName,
  TSchema
> & {
  aggregate: AggregateBuilder<SimplifyWritable<TSchema>, "effect">;
} & CrudCollectionEffect<SimplifyWritable<TSchema>>;

/** Wrap only terminal operations; query construction and typing use the shared core. */
const adaptBuilder = (builder: object, operation: string): object =>
  Object.fromEntries(
    Object.entries(builder).map(([key, value]) => {
      if (typeof value !== "function") return [key, value];
      const invoke = value as (...args: unknown[]) => unknown;
      if (key === "pipe")
        return [key, (...args: unknown[]) => adaptBuilder(invoke(...args) as object, operation)];
      if (key === "execute" || key === "toList" || key === "toOne") {
        const label = operation === "aggregate" ? operation : `${operation}.${key}`;
        return [
          key,
          (...args: unknown[]) =>
            wrapMongoOperation(
              key === "execute" ? operation : label,
              () => invoke(...args) as Promise<unknown>,
            ),
        ];
      }
      return [key, value];
    }),
  );

export const collectionEffect = <TName extends string, TSchema extends SchemaLike>(
  name: TName,
  schema: TSchema,
  mongoCol: MongoCollection<SimplifyWritable<InferSchema<TSchema>>>,
): BoundCollectionEffect<TName, InferSchema<TSchema>> => {
  const shared = collection(name, schema, mongoCol);
  return Object.fromEntries(
    Object.entries(shared).map(([key, value]) => {
      if (typeof value !== "function") return [key, value];
      const invoke = value as (...args: unknown[]) => object;
      return [key, (...args: unknown[]) => adaptBuilder(invoke(...args), key)];
    }),
  ) as BoundCollectionEffect<TName, InferSchema<TSchema>>;
};

/** Creates a schema-aware registry requiring the MongoDbClient service. */
export const registryEffect = <const TMap extends Dict<SchemaLike>>(
  version: Parameters<typeof import("./registry.js").registry>[0],
  schemas: { [K in keyof TMap]: TMap[K] },
) =>
  Effect.gen(function* () {
    const { db } = yield* MongoDbClient;
    return Object.fromEntries(
      Object.entries(schemas).map(([name, schema]) => [
        name,
        collectionEffect(
          name,
          schema,
          db.collection<SimplifyWritable<InferSchema<typeof schema>>>(name),
        ),
      ]),
    ) as unknown as { [K in keyof TMap]: BoundCollectionEffect<K & string, InferSchema<TMap[K]>> };
  });

export const makeMongoDbClientLayer = (db: Db): Layer.Layer<MongoDbClient> =>
  Layer.succeed(MongoDbClient, { db });
