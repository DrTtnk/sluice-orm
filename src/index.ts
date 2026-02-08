// ==========================================================================
// Curated Public API — only intentional exports, no internal leakage
// ==========================================================================

// --- Custom $accumulator support ---
export type { TypedAccumulator } from "./accumulator-utils.js";
export { functionToString, resolveAccumulator } from "./accumulator-utils.js";

// --- Expression / Accumulator / Window builders ---
export { AccumulatorBuilder, ExprBuilder, Ret, WindowBuilder } from "./builder.js";

// --- Error types visible to consumers ---
export type { CallbackOnlyError } from "./common-errors.js";

// --- CRUD types ---
export type {
  BulkWriteBuilder,
  BulkWriteOp,
  CountBuilder,
  CountOptions,
  CrudCollection,
  CrudFilter,
  DeleteBuilder,
  DistinctBuilder,
  FindBuilder,
  FindOneAndDeleteBuilder,
  FindOneAndOptions,
  FindOneAndReplaceBuilder,
  FindOneAndUpdateBuilder,
  FindOptions,
  InsertManyBuilder,
  InsertOneBuilder,
  ProjectionSpec,
  ReplaceBuilder,
  SortSpec,
  UpdateManyBuilder,
  UpdateOneBuilder,
} from "./crud.js";

// --- Update pipeline support ---
export type { UpdateOperators, UpdatePipelineCallback } from "./crud/updates/stages/index.js";

// --- Migration ---
export type { MigrationOperators } from "./migrate.js";
export { migrate } from "./migrate.js";

// --- Pipeline types ---
export type {
  Agg,
  AggregateBuilder,
  MigrationPipelineBuilder,
  PipelineBuilder,
  StageFunction,
  TypedPipeline,
  UpdatePipelineBuilder,
} from "./pipeline-types.js";

// --- Registry & Collection ---
export type {
  BoundCollection,
  Collection,
  CollectionType,
  InferSchema,
  SchemaLike,
} from "./registry.js";
export { collection, registry } from "./registry.js";

// --- Domain types defined in sluice.ts ---
export type { ForeignType, Geometry, TimeUnit, WindowSpec } from "./sluice.js";

// --- Aggregation pipeline stage functions ---
export {
  $addFields,
  $bucket,
  $bucketAuto,
  $changeStream,
  $collStats,
  $count,
  $currentOp,
  $densify,
  $documents,
  $facet,
  $fill,
  $geoNear,
  $graphLookup,
  $group,
  $indexStats,
  $limit,
  $listLocalSessions,
  $listSessions,
  $lookup,
  $match,
  $merge,
  $out,
  $planCacheStats,
  $project,
  $redact,
  $replaceRoot,
  $replaceWith,
  $sample,
  $set,
  $setWindowFields,
  $shardedDataDistribution,
  $skip,
  $sort,
  $sortByCount,
  $unionWith,
  $unset,
  $unwind,
} from "./sluice-stages.js";
