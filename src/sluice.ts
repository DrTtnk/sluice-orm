import type { JSONSchema } from "json-schema-to-typescript"; // Used for future JSON Schema inference
import type { BSONTypeAlias } from "mongodb";

// Import Ret class (both type and value) from builder
import { Ret } from "./builder.js";
import type { CallbackOnlyError } from "./common-errors.js";
import type {
  ArrayFieldRef,
  ArrayProjectionPath,
  BooleanFieldRef,
  DateFieldRef,
  FilteredPath,
  NumericFieldRef,
  PathType,
  PathValueArrayProjection,
  ResolveValue,
  StringFieldRef,
} from "./paths.js";
import type { Collection as CollectionT } from "./registry.js";
import type { OpaqueError, TypeError } from "./type-errors.js";
import type { Dict } from "./type-utils.js";

export { Ret };

// Re-export pipeline types
export type {
  Agg,
  AggregateBuilder,
  MigrationPipelineBuilder,
  PipelineBuilder,
  StageFunction,
  TypedPipeline,
  UpdatePipelineBuilder,
  UpdateStageBrand,
  UpdateStageFunction,
} from "./pipeline-types.js";

// Re-export type utilities
export type { SimplifyWritable } from "./type-utils.js";
export type { CallbackOnlyError };

type ResolvePathValue<C, P extends string> =
  P extends PathType<C> ?
    PathValueArrayProjection<C, P> extends never ?
      ResolveValue<C, P>
    : PathValueArrayProjection<C, P>
  : OpaqueError<`Invalid path: ${P}`>;

// GeoJSON Geometry
export type Geometry =
  | { type: "Point"; coordinates: [number, number] }
  | { type: "LineString"; coordinates: [number, number][] }
  | { type: "Polygon"; coordinates: [number, number][][] }
  | { type: "MultiPoint"; coordinates: [number, number][] }
  | { type: "MultiLineString"; coordinates: [number, number][][] }
  | { type: "MultiPolygon"; coordinates: [number, number][][][] }
  | { type: "GeometryCollection"; geometries: Geometry[] };

// ==========================================
// Core Pipeline Types
// ==========================================

// Re-export Collection types from registry
export type {
  BoundCollection,
  Collection,
  CollectionType,
  InferSchema,
  SchemaLike,
} from "./registry.js";
export { collection, registry } from "./registry.js";

// Type alias for extracting document type from Collection
export type ForeignType<T extends CollectionT> = T["__collectionType"];

// ==========================================
// Base Types & Inputs - STRICT (no lenient fallbacks!)
// ==========================================

// Field reference types - reexport from paths.ts (battle-tested utilities)
// These are STRICT: only valid paths when context is known
// When path computation fails (C is any/unknown), these resolve to never
export type {
  ArrayFieldRef,
  BooleanFieldRef,
  DateFieldRef,
  NumericFieldRef,
  StringFieldRef,
} from "./paths.js";

// Paths that resolve to arrays of a specific type (for MongoDB array projection semantics)
// E.g., items.quantity on { items: { quantity: number }[] } resolves to number[]
type NumericArrayPath<T> = FilteredPath<T, readonly number[] | number[] | null | undefined>;

export type NumericArrayFieldRef<T> = `$${NumericArrayPath<T>}`;

// Generic path field ref - any valid path in the document
type AnyPathFieldRef<T> = `$${PathType<T>}`;

// Generic field ref for any field in context
// Smart field ref - allows any $field when context is any, otherwise strict
// This handles TypeScript inference failures gracefully without being lenient by default
type SmartFieldRef<C, StrictRef> =
  [StrictRef] extends [never] ? OpaqueError<"Invalid field reference"> : StrictRef;

type SmartNumericFieldRef<C> = SmartFieldRef<C, NumericFieldRef<C> | NumericArrayFieldRef<C>>;
export type SmartAnyPathFieldRef<C> = SmartFieldRef<C, AnyPathFieldRef<C>>;

// Input types - STRICT when context is known, lenient when context is any (inference failure)
export type ArrayIn<C> = SmartFieldRef<C, ArrayFieldRef<C>> | readonly unknown[];
export type NumericIn<C> = SmartFieldRef<C, NumericFieldRef<C>> | (number & {});
export type BooleanIn<C> = SmartFieldRef<C, BooleanFieldRef<C>> | (boolean & {});
// StringIn: accepts valid string field refs OR literal strings
// For field refs ($...), only valid StringFieldRef<C> are accepted
// For literals, any string that isn't a field ref is accepted
export type StringIn<C> = SmartFieldRef<C, StringFieldRef<C>> | (string & {});
export type DateIn<C> = SmartFieldRef<C, DateFieldRef<C>> | (Date & {});
export type ExpressionIn<C> = ArrayIn<C> | NumericIn<C> | BooleanIn<C> | StringIn<C> | DateIn<C>;

// ValidExpressionValue - validates that field refs ($...) are valid paths in the context
// Non-field-ref values (numbers, booleans, dates, literals) are accepted as-is
export type ValidExpressionValue<C, T> =
  T extends `$${infer _Path}` ?
    T extends SmartAnyPathFieldRef<C> ?
      T
    : OpaqueError<`Invalid field path: ${T & string}`> // Field ref must be valid path
  : T; // Non-field-refs are OK

// ==========================================
// Type Resolution
// ==========================================

export type ElemType<T> = T extends readonly (infer E)[] ? E : unknown;

// Resolve expression type:
// - string path -> property type
// - Ret<C, T> -> T
// - builder function -> return type
// - value -> value type
// Extract $this context from C
type ExtractThis<C> = C extends { $this: infer ThisType } ? ThisType : never;

// Extract $value context from C
type ExtractValue<C> = C extends { $value: infer ValueType } ? ValueType : never;

// Extract $vars map from C
type ExtractVars<C> = C extends { $vars: infer Vars } ? Vars : never;

export type ResolveType<C, T> =
  // Handle $$this.path - resolve path relative to $this in context
  // PathType doesn't handle keys starting with $, so we need explicit handling
  T extends "$$this" ? ExtractThis<C>
  : T extends `$$this.${infer P}` ? ResolvePathValue<ExtractThis<C>, P>
  : T extends "$$value" ? ExtractValue<C>
  : T extends `$$value.${infer P}` ? ResolvePathValue<ExtractValue<C>, P>
  : // System variables - keep them as-is
  T extends "$$ROOT" | "$$CURRENT" ? C
  : T extends `$$${"DESCEND" | "PRUNE" | "KEEP" | "NOW" | "CLUSTER_TIME"}` ? T
  : // Handle custom variable names with path ($$item.name)
  T extends `$$${infer Var}.${infer P}` ?
    Var extends "ROOT" | "CURRENT" ? ResolvePathValue<C, P>
    : `$${Var}` extends keyof C ? ResolvePathValue<C[`$${Var}`], P>
    : `$$${Var}` extends keyof C ? ResolvePathValue<C[`$$${Var}`], P>
    : ExtractVars<C> extends infer Vars ?
      Var extends keyof Vars ?
        ResolvePathValue<Vars[Var & keyof Vars], P>
      : unknown
    : unknown
  : // Handle custom variable names without path ($$order, $$item, etc.) - AFTER system vars
  T extends `$$${infer Var}` ?
    `$${Var}` extends keyof C ? C[`$${Var}`]
    : T extends keyof C ? C[T]
    : ExtractVars<C> extends infer Vars ?
      Var extends keyof Vars ?
        Vars[Var & keyof Vars]
      : ExtractThis<C>
    : ExtractThis<C>
  : T extends `$${infer P}` ? ResolvePathValue<C, P>
  : T extends Ret<any, infer R> ? R
  : T extends object ? ResolveObject<C, T>
  : T;

// Resolve all Ret types in an object - used for $group output
// Recursively resolve an object's values
// Helper to resolve a single property value (reduces branching in mapped type)
type ResolveObjectValue<C, V> =
  V extends Ret<any, infer R> ? R
  : V extends `$${string}` ? ResolveType<C, V>
  : V extends object ? ResolveObject<C, V>
  : V;

type ResolveObject<C, Obj> = {
  [K in keyof Obj]: ResolveObjectValue<C, Obj[K]>;
};

// Helper to resolve a spec property (consolidates branching logic)
type ResolveSpecValue<C, V> =
  V extends Ret<any, infer R> ? R
  : V extends (c: any) => infer R ? R
  : V extends `$${string}` ? ResolveType<C, V>
  : V extends { [key: `$${string}`]: any } ?
    OpaqueError<"Invalid MongoDB operator object - use builder API instead (e.g., $.sum(...))"> &
      never
  : V extends object ? ResolveObject<C, V>
  : V;

export type ResolveSpec<C, Spec> = {
  [K in keyof Spec]: ResolveSpecValue<C, Spec[K]>;
};

export type DeepSpecInput<C, Spec> =
  Spec extends Ret<any, any> ? Spec
  : Spec extends ExprArg<C> ? ExprArgInput<C, Spec>
  : Spec extends readonly (infer E)[] ? readonly DeepSpecInput<C, E>[]
  : Spec extends object ? { [K in keyof Spec]: DeepSpecInput<C, Spec[K]> }
  : Spec;

export type DeepSpecResolve<C, Spec> =
  Spec extends Ret<any, infer R> ? R
  : Spec extends ExprArg<C> ? ResolveType<C, Spec>
  : Spec extends readonly (infer E)[] ? DeepSpecResolve<C, E>[]
  : Spec extends object ? { [K in keyof Spec]: DeepSpecResolve<C, Spec[K]> }
  : Spec;

// ==========================================
// Match Filter Types
// ==========================================

// Variable references like $$this, $$value, $$ROOT, and $$var paths
type SystemVarName = "ROOT" | "CURRENT" | "DESCEND" | "PRUNE" | "KEEP" | "NOW" | "CLUSTER_TIME";
type ThisValueVarName = "this" | "value";
type VarsRecord<C> = C extends { $vars: infer Vars extends Dict<unknown> } ? Vars : Dict<never>;
type VarsFromContext<C> = Extract<keyof VarsRecord<C>, string>;
type VarName<C> = SystemVarName | ThisValueVarName | VarsFromContext<C>;
type VarRef<C> = VarName<C> extends infer V extends string ? `$$${V}` : never;

type VarPathRef<C> =
  | (C extends { $this: infer ThisType } ? `$$this.${PathType<ThisType>}` : never)
  | (C extends { $value: infer ValueType } ? `$$value.${PathType<ValueType>}` : never)
  | {
      [K in VarsFromContext<C>]: `$$${K}.${PathType<VarsRecord<C>[K]>}`;
    }[VarsFromContext<C>]
  | (C extends unknown ? `$$ROOT.${PathType<C>}` | `$$CURRENT.${PathType<C>}` : never);

type KnownVar<C, V extends string> =
  V extends SystemVarName ? true
  : V extends ThisValueVarName ? true
  : `$${V}` extends keyof C ? true
  : `$$${V}` extends keyof C ? true
  : C extends { $vars: infer Vars } ?
    V extends keyof Vars ?
      true
    : false
  : false;

// Loose args that accept field refs, literals, variable refs, and Ret values
// NumericArg accepts: numeric fields, numeric array fields (for $sum/$avg), Ret types, and var refs
export type NumericArg<C> =
  | NumericIn<C>
  | SmartNumericFieldRef<C>
  | Ret<C, number>
  | Ret<C, number | null>
  | Ret<C, number[]>
  | VarRef<C>
  | VarPathRef<C>;
export type BooleanArg<C> = BooleanIn<C> | Ret<C, boolean> | VarRef<C> | VarPathRef<C>;
// StringArg: accepts string field refs, literal strings, Ret<C, string>, and VarRef
// Field refs starting with $ are validated against StringFieldRef<C>
export type StringArg<C> =
  | SmartFieldRef<C, StringFieldRef<C>>
  | Ret<C, string>
  | VarRef<C>
  | VarPathRef<C>
  | (string & {});
export type DateArg<C> = DateIn<C> | Ret<C, Date> | VarRef<C> | VarPathRef<C>;
type ArrayLiteral<C> = readonly ExprArg<C>[] | ExprArg<C>[];
type ArrayProjectionFieldRef<T> = `$${ArrayProjectionPath<T>}`;
type ArrayFieldRefLike<T> = ArrayFieldRef<T> | ArrayProjectionFieldRef<T>;
export type ArrayArg<C> =
  | SmartFieldRef<C, ArrayFieldRefLike<C>>
  | Ret<C, unknown[]>
  | VarRef<C>
  | VarPathRef<C>
  | ArrayLiteral<C>;
// ExprArg accepts ANY valid path (not just typed ones) so ResolveType can work
// Also accepts literal values (strings, numbers, etc.) for conditional expressions
export type ExprArg<C> =
  | SmartAnyPathFieldRef<C>
  | Ret<C>
  | VarRef<C>
  | VarPathRef<C>
  | number
  | boolean
  | Date
  | (string & {})
  | null;

// AccumulatorExprArg: like ExprArg but WITHOUT bare strings.
// In accumulator position ($avg, $sum, $min, $max, etc.), a bare string like "score"
// is almost always a bug — it's treated as a literal string, not a field reference.
// Use "$score" for field refs, or $.literal("score") for intentional literals.
export type AccumulatorExprArg<C> =
  | SmartAnyPathFieldRef<C>
  | Ret<C>
  | VarRef<C>
  | VarPathRef<C>
  | number
  | boolean
  | Date
  | null;

// ==========================================
// Input Validation Types with Detailed Errors
// ==========================================

type ValidInput<C, T, FieldRefType, TypeName extends string> =
  T extends VarRef<C> | VarPathRef<C> ? T
  : T extends Ret<any, any> ? T
  : T extends `$${string}` ?
    T extends SmartFieldRef<C, FieldRefType> ?
      T
    : TypeError<`"${T}" is not a valid ${TypeName} field`, `${TypeName} | ${TypeName} field ref`, T>
  : T;

type ValidStringInput<C, T> = ValidInput<C, T, StringFieldRef<C>, "string">;
type ValidNumericInput<C, T> =
  T extends number ? T : ValidInput<C, T, SmartNumericFieldRef<C>, "numeric">;
type ValidBooleanInput<C, T> =
  T extends boolean ? T : ValidInput<C, T, BooleanFieldRef<C>, "boolean">;
type ValidDateInput<C, T> = T extends Date ? T : ValidInput<C, T, DateFieldRef<C>, "date">;
type ValidArrayInput<C, T> =
  T extends readonly unknown[] ? T : ValidInput<C, T, ArrayFieldRefLike<C>, "array">;

// ValidExprArg validates general expression arguments (field refs must exist)
type ValidExprArg<C, T> =
  T extends `$$${infer Var}.${string}` ?
    KnownVar<C, Var> extends true ?
      T
    : OpaqueError<`Invalid variable: $$${Var}`>
  : T extends VarRef<C> | VarPathRef<C> ? T
  : T extends `$${string}` ? ValidExpressionValue<C, T>
  : T;

// ValidAccumulatorArg: like ValidExprArg but rejects bare strings with helpful error
type ValidAccumulatorArg<C, T> =
  T extends `$$${infer Var}.${string}` ?
    KnownVar<C, Var> extends true ?
      T
    : OpaqueError<`Invalid variable: $$${Var}`>
  : T extends VarRef<C> | VarPathRef<C> ? T
  : T extends `$${string}` ? ValidExpressionValue<C, T>
  : T extends string ?
    OpaqueError<`Bare string "${T}" is not valid in accumulator position — did you mean "$${T}"?`>
  : T;

// Input type aliases for validation
export type StringArgInput<C, T extends StringArg<C>> = ValidStringInput<C, T>;
export type NumericArgInput<C, T extends NumericArg<C>> = ValidNumericInput<C, T>;
export type BooleanArgInput<C, T extends BooleanArg<C>> = ValidBooleanInput<C, T>;
export type DateArgInput<C, T extends DateArg<C>> = ValidDateInput<C, T>;
export type ArrayArgInput<C, T extends ArrayArg<C>> = ValidArrayInput<C, T>;
export type ExprArgInput<C, T extends ExprArg<C>> = ValidExprArg<C, T>;
export type ExprArgInputs<C, T extends readonly ExprArg<C>[]> = {
  [K in keyof T]: ExprArgInput<C, T[K]>;
};
export type AccumulatorArgInput<C, T extends AccumulatorExprArg<C>> = ValidAccumulatorArg<C, T>;
export type AccumulatorArgInputs<C, T extends readonly AccumulatorExprArg<C>[]> = {
  [K in keyof T]: AccumulatorArgInput<C, T[K]>;
};

// CompatibleArg: Given a resolved type T, what arguments are compatible?
type CompatibleArg<C, T> =
  [T] extends [unknown] ?
    [unknown] extends [T] ?
      ExprArg<C>
    : CompatibleArgKnown<C, T>
  : CompatibleArgKnown<C, T>;

type CompatibleArgKnown<C, T> =
  | Ret<C, T>
  | VarRef<C>
  | VarPathRef<C>
  | (T extends boolean ? BooleanArg<C>
    : T extends number ? NumericArg<C>
    : T extends string ? SmartFieldRef<C, StringFieldRef<C>> | Ret<C, string> | VarRef<C>
    : T extends Date ? DateArg<C>
    : never)
  | (T extends boolean ? boolean
    : T extends number ? number
    : T extends string ? string & {}
    : T extends Date ? Date
    : T extends null | undefined ? null | undefined
    : never)
  | (T extends (infer E)[] ? CompatibleArg<C, E> | ArrayArg<C> : never);

// Match query operators based on field type
type ComparisonOps<T> = {
  $eq?: T;
  $ne?: T;
  $exists?: boolean;
  $type?: BSONTypeAlias;
};

/** Adds $not wrapper to any match operator base type */
type WithNot<T> = T & { $not?: T };

// Match-specific input types: no field refs ($-prefixed strings)
// In $match context (outside $expr), MongoDB does NOT interpret field refs
type MatchNumericIn = number & {};
type MatchStringIn = string & {};
type MatchDateIn = Date & {};
type MatchBooleanIn = boolean & {};

// Base types (without $not) to avoid circular references
type NumericMatchOpsBase = ComparisonOps<MatchNumericIn> & {
  $gt?: MatchNumericIn;
  $gte?: MatchNumericIn;
  $lt?: MatchNumericIn;
  $lte?: MatchNumericIn;
  $in?: MatchNumericIn[];
  $nin?: MatchNumericIn[];
  $mod?: [number, number];
  $bitsAllClear?: number | number[];
  $bitsAllSet?: number | number[];
  $bitsAnyClear?: number | number[];
  $bitsAnySet?: number | number[];
};

type NumericMatchOperators = WithNot<NumericMatchOpsBase>;

// $regex/$options: $options requires $regex to be present
type StringRegexOps =
  | { $regex: string | RegExp; $options?: string }
  | { $regex?: undefined; $options?: undefined };

type StringMatchOpsBase = ComparisonOps<MatchStringIn> & {
  $gt?: MatchStringIn;
  $gte?: MatchStringIn;
  $lt?: MatchStringIn;
  $lte?: MatchStringIn;
  $in?: MatchStringIn[];
  $nin?: MatchStringIn[];
} & StringRegexOps;

type StringMatchOperators = WithNot<StringMatchOpsBase>;

type BooleanMatchOpsBase = ComparisonOps<MatchBooleanIn>;

type BooleanMatchOperators = WithNot<BooleanMatchOpsBase>;

type ElemMatchValue<E> = { $elemMatch: ValidMatchFilterWithBuilder<E> };

type ArrayMatchOpsBase<E> = {
  $size?: number;
  $all?: readonly E[];
  $elemMatch?: ElemMatchValue<E>;
  $in?: readonly E[];
  $nin?: readonly E[];
  $exists?: boolean;
  $type?: BSONTypeAlias;
};

type ArrayMatchOperators<E> = WithNot<ArrayMatchOpsBase<E>>;

type GeoMatchOpsBase<C> = {
  $geoIntersects?: { $geometry: Geometry };
  $geoWithin?: {
    $geometry?: Geometry;
    $box?: [[number, number], [number, number]];
    $polygon?: [number, number][];
    $center?: [[number, number], number];
    $centerSphere?: [[number, number], number];
  };
  $near?: {
    $geometry: Geometry;
    $maxDistance?: number;
    $minDistance?: number;
  };
  $nearSphere?: {
    $geometry: Geometry;
    $maxDistance?: number;
    $minDistance?: number;
  };
};

type GeoMatchOperators<C> = WithNot<GeoMatchOpsBase<C>>;

type DateMatchOpsBase = ComparisonOps<MatchDateIn> & {
  $gt?: MatchDateIn;
  $gte?: MatchDateIn;
  $lt?: MatchDateIn;
  $lte?: MatchDateIn;
};

type DateMatchOperators = WithNot<DateMatchOpsBase>;

// ValidMatchFilter with builder support
export type ValidMatchFilterWithBuilder<C> = {
  [K in keyof C]?:
    | C[K]
    | Ret<C, C[K]>
    | (C[K] extends number ? NumericMatchOperators
      : C[K] extends string ? StringMatchOperators
      : C[K] extends boolean ? BooleanMatchOperators
      : C[K] extends Date ? DateMatchOperators
      : C[K] extends readonly (infer E)[] ? E | Ret<C, E> | ArrayMatchOperators<E>
      : C[K] extends Geometry ? GeoMatchOperators<C>
      : ComparisonOps<never>);
} & {
  $expr?: BooleanArg<C>;
  $or?: ValidMatchFilterWithBuilder<C>[];
  $and?: ValidMatchFilterWithBuilder<C>[];
  $nor?: ValidMatchFilterWithBuilder<C>[];
  $text?: {
    $search: string;
    $language?: string;
    $caseSensitive?: boolean;
    $diacriticSensitive?: boolean;
  };
  /** @deprecated $where is a MongoDB escape hatch and bypasses type safety. */
  $where?: string | ((...args: any[]) => boolean);
  $jsonSchema?: JSONSchema;
  $comment?: string;
};

// ==========================================
// Common Expression Function Signatures
// ==========================================

export type TimeUnit =
  | "year"
  | "quarter"
  | "month"
  | "week"
  | "day"
  | "hour"
  | "minute"
  | "second"
  | "millisecond";

type WindowBoundary = number | "current" | "unbounded";

export type WindowSpec =
  | { documents: [WindowBoundary, WindowBoundary] }
  | { range: [WindowBoundary, WindowBoundary]; unit: TimeUnit };

export type { AccumulatorBuilder, ExprBuilder, WindowBuilder } from "./builder.js";
export * from "./crud.js";
export * from "./sluice-stages.js";
// Note: sluice-operators.ts contains standalone $operators, not part of ExprBuilder
