/* eslint-disable @typescript-eslint/unified-signatures */
import { type Decimal128, ObjectId } from "mongodb";

import { resolveAccumulator, type TypedAccumulator } from "./accumulator-utils.js";
import type { PathType } from "./paths.js";
import type { PipelineBuilder } from "./pipeline-types.js";
// Type imports for proper operator typing
import type {
  AccumulatorArgInput,
  AccumulatorArgInputs,
  AccumulatorExprArg,
  ArrayArg,
  ArrayArgInput,
  ArrayIn,
  BooleanArg,
  BooleanArgInput,
  CallbackOnlyError,
  DateArg,
  DateArgInput,
  DeepSpecInput,
  DeepSpecResolve,
  ElemType,
  ExprArg,
  ExprArgInput,
  ExprArgInputs,
  NumericArg,
  NumericArgInput,
  ResolveType,
  StringArg,
  StringArgInput,
  TimeUnit,
  ValidMatchFilterWithBuilder,
  WindowSpec,
} from "./sluice.js";
import type {
  Dict,
  MergeWithIndexOverride,
  ShallowMergeObjectsOverride,
  SimplifyWritable,
} from "./type-utils.js";

type Stage = Dict<unknown>;
type RuntimeAgg = { stages?: Stage[] } & Dict<unknown>;

type BuilderMode = "expression" | "accumulator";

// ==========================================
// Helper Types for Proper Return Typing
// ==========================================

// Map ResolveType over a tuple
type ResolveTypes<C, T extends unknown[]> = { [K in keyof T]: ResolveType<C, T[K]> };

type DeepObjectExpr<C> =
  | ExprArg<C>
  | { [K in string]: DeepObjectExpr<C> }
  | readonly DeepObjectExpr<C>[];

type DeepObjectInput<C, T> =
  T extends ExprArg<C> ? ExprArgInput<C, T>
  : T extends readonly (infer E)[] ? readonly DeepObjectInput<C, E>[]
  : T extends object ? { [K in keyof T]: DeepObjectInput<C, T[K]> }
  : T;

type DeepObjectResolve<C, T> =
  T extends Ret<any, infer R> ? R
  : T extends ExprArg<C> ? ResolveType<C, T>
  : T extends readonly (infer E)[] ? DeepObjectResolve<C, E>[]
  : T extends object ? { [K in keyof T]: DeepObjectResolve<C, T[K]> }
  : T;

// Extract object type from arrayToObject input: {k: K, v: V}[] => Record<K, V>
// Use [K] extends [string] to prevent distributive conditional type over union K
type ArrayToObjectResult<Elem> =
  Elem extends { k: infer K; v: infer V } ?
    [K] extends [string] ?
      Record<K, V>
    : Dict<unknown>
  : Dict<unknown>;

// ==========================================
// DRY Operator Type Patterns
// ==========================================

// Unary operators: single argument in, specific type out
type UnaryNumericOp<C> = <const T extends NumericArg<C>>(
  arg: T & NumericArgInput<C, T>,
) => Ret<C, number>;

type BinaryNumericOp<C> = <const L extends NumericArg<C>, const R extends NumericArg<C>>(
  left: L & NumericArgInput<C, L>,
  right: R & NumericArgInput<C, R>,
) => Ret<C, number>;

type UnaryStringToNumber<C> = <const T extends StringArg<C>>(
  exp: StringArgInput<C, T>,
) => Ret<C, number>;

type UnaryStringToString<C> = <const T extends StringArg<C>>(
  exp: StringArgInput<C, T>,
) => Ret<C, string>;

type BinaryStringToNumber<C> = <const L extends StringArg<C>, const R extends StringArg<C>>(
  left: StringArgInput<C, L>,
  right: StringArgInput<C, R>,
) => Ret<C, -1 | 0 | 1>;

type UnaryDateToNumber<C> = <const T extends DateArg<C>>(
  exp: T & DateArgInput<C, T>,
) => Ret<C, number>;

type UnaryArrayToBool<C> = <const T extends ArrayArg<C>>(
  exp: T & ArrayArgInput<C, T>,
) => Ret<C, boolean>;

type UnaryTypeConversion<C, Output> = <const T extends ExprArg<C>>(
  exp: ExprArgInput<C, T>,
) => Ret<C, NullishResult<C, T, Output>>;

type HasNullish<T> = [Extract<T, null | undefined>] extends [never] ? false : true;
type NullishResult<C, T, Output> =
  HasNullish<ResolveType<C, T>> extends true ? Output | null : Output;
type NullishResultFromArgs<C, T extends readonly ExprArg<C>[], Output> =
  HasNullish<ResolveType<C, T[number]>> extends true ? Output | null : Output;

type ConvertTo = "double" | "string" | "objectId" | "bool" | "date" | "int" | "long" | "decimal";

type ConvertOutput<To extends ConvertTo> =
  To extends "double" | "int" ? number
  : To extends "long" ? number
  : To extends "decimal" ? Decimal128
  : To extends "string" ? string
  : To extends "bool" ? boolean
  : To extends "objectId" ? ObjectId
  : Date;

type BinaryArrayOp<C> = <const A extends ArrayArg<C>, const B extends ArrayArg<C>>(
  left: A & ArrayArgInput<C, A>,
  right: B & ArrayArgInput<C, B>,
) => Ret<C, boolean>;

type UnaryArrayToNumber<C> = <const T extends ArrayArg<C>>(
  exp: T & ArrayArgInput<C, T>,
) => Ret<C, number>;

// Comparison operator type (complex pattern shared by eq, gt, gte, lt, lte, ne)
type ComparisonOp<C> = <
  const L extends ExprArg<C>,
  const R extends CompatibleArg<C, ResolveType<C, L>>,
>(
  left: L extends `$${infer Rest}` ?
    Rest extends `$${string}` ? L
    : L extends AnyPathFieldRef<C> ? L
    : never
  : L,
  right: R extends `$${infer Rest}` ?
    Rest extends `$${string}` ? R
    : ResolveType<C, R> extends ResolveType<C, L> ? R
    : never
  : R,
) => Ret<C, boolean>;

// Varargs operators
type VarargsBooleanOp<C> = <const T extends BooleanArg<C>[]>(
  ...args: { [K in keyof T]: T[K] & BooleanArgInput<C, T[K]> }
) => Ret<C, boolean>;

type VarargsNumericOp<C> = <const T extends NumericArg<C>[]>(
  ...args: T extends readonly NumericArg<C>[] ? T : never
) => Ret<C, number>;

type NumericArgs<C> = readonly [NumericArg<C>, NumericArg<C>, ...NumericArg<C>[]];
type NumericArgsInput<C, T extends readonly NumericArg<C>[]> = {
  [K in keyof T]: NumericArgInput<C, T[K]>;
};
type AddOp<C> = {
  <const T extends NumericArgs<C>>(...args: NumericArgsInput<C, T>): Ret<C, number>;
  <const D extends DateArg<C>, const T extends readonly [NumericArg<C>, ...NumericArg<C>[]]>(
    date: DateArgInput<C, D>,
    ...args: NumericArgsInput<C, T>
  ): Ret<C, Date>;
  <const T extends readonly [NumericArg<C>, ...NumericArg<C>[]], const D extends DateArg<C>>(
    ...args: [...NumericArgsInput<C, T>, DateArgInput<C, D>]
  ): Ret<C, Date>;
};

type SubtractOp<C> = {
  <const L extends DateArg<C>, const R extends DateArg<C>>(
    left: DateArgInput<C, L>,
    right: DateArgInput<C, R>,
  ): Ret<C, number>;
  <const L extends DateArg<C>, const R extends NumericArg<C>>(
    left: DateArgInput<C, L>,
    right: NumericArgInput<C, R>,
  ): Ret<C, Date>;
  <const L extends NumericArg<C>, const R extends NumericArg<C>>(
    left: NumericArgInput<C, L>,
    right: NumericArgInput<C, R>,
  ): Ret<C, number>;
};

type SortPath<C> = PathType<C> | `_id.${string}`;
type SortBySpec<C> =
  | Partial<Record<SortPath<C>, 1 | -1>>
  | { $meta: "textScore" | "searchScore" }
  | (Partial<Record<SortPath<C>, 1 | -1>> & { $meta: "textScore" | "searchScore" });

type ArrayLiteral<C> = readonly ExprArg<C>[] | ExprArg<C>[];

// Get first non-nil type from tuple (for ifNull)
type FirstNotNil<T extends unknown[]> =
  T extends [infer First, ...infer Rest] ?
    First extends null | undefined ?
      FirstNotNil<Rest>
    : First
  : never;

// Detect if a type has a string index signature
type MergeTwo<A, B> =
  [A] extends [null | undefined] ? B
  : [B] extends [null | undefined] ? A
  : MergeWithIndexOverride<A, B>;

// Merge multiple objects (for mergeObjects varargs)
type MergeObjects<C, Objs extends unknown[], Acc = null> =
  Objs extends [infer First, ...infer Rest] ?
    MergeObjects<C, Rest, MergeTwo<ResolveType<C, First>, Acc>>
  : Acc;

type MergeVars<C, Vars extends Dict<unknown>> =
  C extends { $vars: infer Existing extends Dict<unknown> } ?
    SimplifyWritable<Omit<C, "$vars"> & { $vars: SimplifyWritable<Existing & Vars> }>
  : SimplifyWritable<C & { $vars: Vars }>;

type ResolveVarSpec<C, V extends Dict<ExprArg<C>>> = {
  [K in keyof V]: ResolveType<C, V[K]>;
};

// Augmented contexts for array iterators (map, filter, reduce)
type ContextWithThis<C, ThisType, As extends string | undefined = undefined> =
  As extends string ? MergeVars<SimplifyWritable<C & { $this: ThisType }>, Record<As, ThisType>>
  : SimplifyWritable<C & { $this: ThisType }>;
type ContextWithThisAndValue<C, ThisType, ValueType> = SimplifyWritable<
  C & { $this: ThisType; $value: ValueType }
>;

// Concatenate string literal types
type ConcatStrings<T extends readonly unknown[]> =
  T extends readonly [infer First, ...infer Rest] ?
    First extends string ?
      Rest extends readonly string[] ?
        `${First}${ConcatStrings<Rest>}`
      : string
    : string
  : "";

// Compatible argument type for comparison operators
type CompatibleArg<C, T> =
  [T] extends [unknown] ?
    [unknown] extends [T] ?
      ExprArg<C>
    : CompatibleArgKnown<C, T>
  : CompatibleArgKnown<C, T>;

type CompatibleArgKnown<C, T> =
  | Ret<C, T>
  | `$$${string}`
  | (T extends boolean ? BooleanArg<C>
    : T extends number ? NumericArg<C>
    : T extends string ? `$${string}` | Ret<C, string> | `$$${string}`
    : T extends Date ? DateArg<C>
    : never)
  | (T extends boolean ? boolean
    : T extends number ? number
    : T extends string ? string & {}
    : T extends Date ? Date
    : T extends null | undefined ? null | undefined
    : never)
  | (T extends (infer E)[] ? CompatibleArg<C, E> | ArrayArg<C> : never);

// Generic path field ref
type AnyPathFieldRef<C> = `$${string}`;

export type RuntimeValue =
  | number
  | string
  | boolean
  | null
  | undefined
  | Date
  | ObjectId
  | RegExp
  | Ret<never, RuntimeValue>
  | readonly RuntimeValue[]
  | Readonly<{ [key: string]: RuntimeValue }>;

// Ret as a class for instanceof support
export class Ret<out Context = unknown, T = unknown> {
  readonly __context!: Context;
  readonly __type!: SimplifyWritable<T>;
  readonly __tag = "Ret" as const;

  constructor(
    readonly __fn: unknown,
    readonly __minVersion = "",
  ) {}
}

type AnyRet = Ret<never, never>;

export class BaseBuilder<C, Mode extends BuilderMode = "expression"> {
  // Helper to resolve nested callbacks and values
  _resolve(value: unknown): unknown {
    const _foo = value as RuntimeValue | ((builder: BaseBuilder<C>) => unknown);
    if (_foo instanceof Ret) return this._resolve(_foo.__fn);
    if (typeof _foo === "function") return this._resolve(_foo(this));
    if (Array.isArray(_foo)) return _foo.map(item => this._resolve(item));
    if (_foo instanceof Date) return _foo;
    if (_foo instanceof ObjectId) return _foo;
    if (_foo instanceof RegExp) return _foo;
    if (typeof _foo !== "object" || _foo === null) return _foo;

    return Object.entries(_foo).reduce((acc, [k, v]) => ({ ...acc, [k]: this._resolve(v) }), {});
  }

  // Identity: single argument passed through
  protected id =
    (op: string) =>
    (arg: unknown): AnyRet =>
      new Ret({ [`$${op}`]: this._resolve(arg) });

  // Varargs: multiple arguments as array
  protected varargs =
    (op: string) =>
    (...args: unknown[]): AnyRet =>
      new Ret({ [`$${op}`]: args.map(arg => this._resolve(arg)) });

  // Flexible: 1 arg -> raw value, >1 args -> array (for operators that are both unary accumulators and variadic expressions)
  protected flexible =
    (op: string) =>
    (...args: unknown[]): AnyRet =>
      new Ret({
        [`$${op}`]: args.length === 1 ? this._resolve(args[0]) : args.map(this._resolve.bind(this)),
      });

  // Options: single object argument
  protected options =
    (op: string) =>
    (options: unknown): AnyRet =>
      new Ret({ [`$${op}`]: this._resolve(options) });

  // Nullary: no arguments
  protected nullary = (op: string) => (): AnyRet => new Ret({ [`$${op}`]: {} });

  // Pipeline builder for $facet and sub-pipelines
  pipe: PipelineBuilder<C> = ((...stages: ((agg: RuntimeAgg) => RuntimeAgg)[]) =>
    stages.reduce((agg: RuntimeAgg, stageFn) => stageFn(agg), {
      stages: [],
    })) as unknown as PipelineBuilder<C>;

  // Query helper for $match array element filters
  elemMatch = <const E>(filter: ($: ExprBuilder<E>) => ValidMatchFilterWithBuilder<E>) => ({
    $elemMatch: filter(new ExprBuilder<E>()),
  });

  // ==========================================
  // Arithmetic Operators
  // ==========================================

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/add/
  add: AddOp<C> = this.varargs("add");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/subtract/
  subtract: SubtractOp<C> = this.varargs("subtract");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/multiply/
  multiply: VarargsNumericOp<C> = this.varargs("multiply");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/divide/
  divide: BinaryNumericOp<C> = this.varargs("divide");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/mod/
  mod: BinaryNumericOp<C> = this.varargs("mod");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/abs/
  abs: UnaryNumericOp<C> = this.id("abs");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/ceil/
  ceil: UnaryNumericOp<C> = this.id("ceil");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/floor/
  floor: UnaryNumericOp<C> = this.id("floor");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/ln/
  ln: UnaryNumericOp<C> = this.id("ln");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/log/
  log: BinaryNumericOp<C> = this.varargs("log");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/log10/
  log10: UnaryNumericOp<C> = this.id("log10");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/pow/
  pow: BinaryNumericOp<C> = this.varargs("pow");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/sqrt/
  sqrt: UnaryNumericOp<C> = this.id("sqrt");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/trunc/
  trunc: <const T extends NumericArg<C>, const P extends NumericArg<C>>(
    num: T & NumericArgInput<C, T>,
    place?: P & NumericArgInput<C, P>,
  ) => Ret<C, number> = (num, place) =>
    place !== undefined ? this.varargs("trunc")(num, place) : this.options("trunc")(num);

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/round/
  round: <const T extends NumericArg<C>, const P extends NumericArg<C>>(
    num: T & NumericArgInput<C, T>,
    place?: P & NumericArgInput<C, P>,
  ) => Ret<C, number> = (num, place) =>
    place !== undefined ? this.varargs("round")(num, place) : this.options("round")(num);

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/exp/
  exp: UnaryNumericOp<C> = this.id("exp");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/rand/
  rand: () => Ret<C, number> = this.nullary("rand");

  // ==========================================
  // Array Operators
  // ==========================================

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayElemAt/
  arrayElemAt: <const A extends ArrayArg<C>, const I extends NumericArg<C>>(
    array: A & ArrayArgInput<C, A>,
    index: I & NumericArgInput<C, I>,
  ) => Ret<C, ElemType<ResolveType<C, A>>> = this.varargs("arrayElemAt");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayToObject/
  arrayToObject: <const T extends ArrayArg<C>>(
    exp: T & ArrayArgInput<C, T>,
  ) => Ret<C, ArrayToObjectResult<ElemType<ResolveType<C, T>>>> = this.id("arrayToObject");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/concatArrays/
  concatArrays: <const T extends ArrayArg<C>[]>(
    ...args: { [K in keyof T]: T[K] & ArrayArgInput<C, T[K]> }
  ) => Ret<C, ElemType<ResolveType<C, T[number]>>[]> = this.varargs("concatArrays");

  /** Selects a subset of array elements that match a condition.
   * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/filter/ */
  filter: {
    <
      const I extends ArrayArg<C>,
      const As extends string | undefined = undefined,
      const L extends NumericArg<C> = NumericArg<C>,
    >(options: {
      input: I & ArrayArgInput<C, I>;
      cond: (
        $: BaseBuilder<ContextWithThis<C, ElemType<ResolveType<C, I>>, As>>,
      ) => BooleanArg<ContextWithThis<C, ElemType<ResolveType<C, I>>, As>>;
      as?: As;
      limit?: L & NumericArgInput<C, L>;
    }): Ret<C, ResolveType<C, I>>;
    <
      const I extends ArrayArg<C>,
      const As extends string | undefined = undefined,
      const B extends BooleanArg<ContextWithThis<C, ElemType<ResolveType<C, I>>, As>> = BooleanArg<
        ContextWithThis<C, ElemType<ResolveType<C, I>>, As>
      >,
      const L extends NumericArg<C> = NumericArg<C>,
    >(options: {
      input: I & ArrayArgInput<C, I>;
      cond: B;
      as?: As;
      limit?: L & NumericArgInput<C, L>;
    }): Ret<C, CallbackOnlyError<"filter">>;
  } = this.options("filter");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/first/
  first: <const T extends AccumulatorExprArg<C>>(
    exp: AccumulatorArgInput<C, T>,
  ) => Ret<
    C,
    Mode extends "accumulator" ? ResolveType<C, T>
    : ResolveType<C, T> extends readonly (infer E)[] ? E | null
    : ResolveType<C, T> | null
  > = this.id("first");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/in/
  in: <const V extends ExprArg<C>, const A extends ArrayArg<C>>(
    value: ExprArgInput<C, V>,
    array: A & ArrayArgInput<C, A>,
  ) => Ret<C, boolean> = this.varargs("in");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexOfArray/
  indexOfArray: <
    const A extends ArrayArg<C>,
    const S extends ExprArg<C>,
    const N extends NumericArg<C>,
  >(options: {
    array: A & ArrayArgInput<C, A>;
    search: ExprArgInput<C, S>;
    start?: N & NumericArgInput<C, N>;
    end?: N & NumericArgInput<C, N>;
  }) => Ret<C, number> = this.options("indexOfArray");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/isArray/
  isArray: <const T extends ExprArg<C>>(exp: ExprArgInput<C, T>) => Ret<C, boolean> =
    this.id("isArray");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/last/
  last: <const T extends AccumulatorExprArg<C>>(
    exp: AccumulatorArgInput<C, T>,
  ) => Ret<
    C,
    Mode extends "accumulator" ? ResolveType<C, T>
    : ResolveType<C, T> extends readonly (infer E)[] ? E | null
    : ResolveType<C, T> | null
  > = this.id("last");

  /** Applies an expression to each element of an array and returns the results.
   * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/map/ */
  map: {
    <
      const A extends ArrayArg<C>,
      const As extends string | undefined = undefined,
      const R extends DeepObjectExpr<ContextWithThis<C, ElemType<ResolveType<C, A>>, As>> =
        DeepObjectExpr<ContextWithThis<C, ElemType<ResolveType<C, A>>, As>>,
    >(options: {
      input: A & ArrayArgInput<C, A>;
      as?: As;
      in: ($: BaseBuilder<ContextWithThis<C, ElemType<ResolveType<C, A>>, As>>) => R;
    }): Ret<C, DeepObjectResolve<ContextWithThis<C, ElemType<ResolveType<C, A>>, As>, R>[]>;
    <
      const A extends ArrayArg<C>,
      const As extends string | undefined = undefined,
      const R extends DeepObjectExpr<ContextWithThis<C, ElemType<ResolveType<C, A>>, As>> =
        DeepObjectExpr<ContextWithThis<C, ElemType<ResolveType<C, A>>, As>>,
    >(options: {
      input: A & ArrayArgInput<C, A>;
      as?: As;
      in: DeepObjectInput<ContextWithThis<C, ElemType<ResolveType<C, A>>, As>, R>;
    }): Ret<C, DeepObjectResolve<ContextWithThis<C, ElemType<ResolveType<C, A>>, As>, R>[]>;
  } = this.options("map");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/maxN/
  maxN: <const T extends ArrayArg<C>, const N extends NumericArg<C>>(options: {
    input: T & ArrayArgInput<C, T>;
    n: N & NumericArgInput<C, N>;
  }) => Ret<C, Mode extends "accumulator" ? ResolveType<C, T>[] : ResolveType<C, T>> =
    this.options("maxN");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/minN/
  minN: <const T extends ArrayArg<C>, const N extends NumericArg<C>>(options: {
    input: T & ArrayArgInput<C, T>;
    n: N & NumericArgInput<C, N>;
  }) => Ret<C, Mode extends "accumulator" ? ResolveType<C, T>[] : ResolveType<C, T>> =
    this.options("minN");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/firstN/
  firstN: <const T extends ArrayArg<C>, const N extends NumericArg<C>>(options: {
    input: T & ArrayArgInput<C, T>;
    n: N & NumericArgInput<C, N>;
  }) => Ret<C, Mode extends "accumulator" ? ResolveType<C, T>[] : ResolveType<C, T>> =
    this.options("firstN");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/lastN/
  lastN: <const T extends ArrayArg<C>, const N extends NumericArg<C>>(options: {
    input: T & ArrayArgInput<C, T>;
    n: N & NumericArgInput<C, N>;
  }) => Ret<C, Mode extends "accumulator" ? ResolveType<C, T>[] : ResolveType<C, T>> =
    this.options("lastN");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/objectToArray/
  objectToArray: <const T extends ExprArg<C>>(
    expression: ExprArgInput<C, T>,
  ) => Ret<C, { k: string; v: ResolveType<C, T> extends Dict<infer V> ? V : unknown }[]> =
    this.id("objectToArray");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/range/
  range: <
    const S extends NumericArg<C>,
    const E extends NumericArg<C>,
    const St extends NumericArg<C> = never,
  >(
    start: S & NumericArgInput<C, S>,
    end: E & NumericArgInput<C, E>,
    step?: St & NumericArgInput<C, St>,
  ) => Ret<C, number[]> = (start, end, step) =>
    step !== undefined ?
      this.varargs("range")(start, end, step)
    : this.varargs("range")(start, end);

  /** Applies an expression to each element of an array, accumulating into a single value.
   * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/reduce/ */
  reduce: {
    <
      const A extends ArrayArg<C>,
      const Init extends ExprArg<C> | ArrayIn<C>,
      const R extends Exclude<
        ExprArg<ContextWithThisAndValue<C, ElemType<ResolveType<C, A>>, ResolveType<C, Init>>>,
        string
      >,
    >(options: {
      input: A & ArrayArgInput<C, A>;
      initialValue: Init;
      in: (
        $: BaseBuilder<
          ContextWithThisAndValue<C, ElemType<ResolveType<C, A>>, ResolveType<C, Init>>
        >,
      ) => R;
    }): Ret<
      C,
      ResolveType<ContextWithThisAndValue<C, ElemType<ResolveType<C, A>>, ResolveType<C, Init>>, R>
    >;
    <const A extends ArrayArg<C>, const Init extends ExprArg<C> | ArrayIn<C>, const R>(options: {
      input: A & ArrayArgInput<C, A>;
      initialValue: Init;
      in: R;
    }): Ret<C, CallbackOnlyError<"reduce">>;
  } = this.options("reduce");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/reverseArray/
  reverseArray: <const T extends ArrayArg<C>>(
    exp: T & ArrayArgInput<C, T>,
  ) => Ret<C, ResolveType<C, T>> = this.id("reverseArray");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/size/
  size: UnaryArrayToNumber<C> = this.id("size");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/slice/
  slice: <const A extends ArrayArg<C>, const N extends NumericArg<C>>(
    array: A & ArrayArgInput<C, A>,
    n: N & NumericArgInput<C, N>,
    position?: N & NumericArgInput<C, N>,
  ) => Ret<C, ResolveType<C, A>> = (array, n, position) =>
    position !== undefined ?
      this.varargs("slice")(array, n, position)
    : this.varargs("slice")(array, n);

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/sortArray/
  sortArray: <const T extends { input: ArrayArg<C>; sortBy: Dict<1 | -1> | 1 | -1 }>(
    options: T & { input: T["input"] & ArrayArgInput<C, T["input"]> },
  ) => Ret<C, ResolveType<C, T["input"]>> = this.options("sortArray");
  // zip with proper tuple typing
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/zip/
  zip: {
    <const T extends readonly ArrayArg<C>[]>(options: {
      inputs: readonly [...T];
      useLongestLength: true;
      defaults: Readonly<{ [K in keyof T]: ElemType<ResolveType<C, T[K]>> }>;
    }): Ret<C, { -readonly [K in keyof T]: ElemType<ResolveType<C, T[K]>> }[]>;
    <const T extends readonly ArrayArg<C>[]>(options: {
      inputs: readonly [...T];
      useLongestLength: true;
      defaults?: Readonly<{ [K in keyof T]?: ElemType<ResolveType<C, T[K]>> }>;
    }): Ret<C, { -readonly [K in keyof T]: ElemType<ResolveType<C, T[K]>> | null }[]>;
    <const T extends readonly ArrayArg<C>[]>(options: {
      inputs: readonly [...T];
      useLongestLength?: false;
      defaults?: never;
    }): Ret<C, { -readonly [K in keyof T]: ElemType<ResolveType<C, T[K]>> }[]>;
  } = this.options("zip");

  // ==========================================
  // Comparison Operators
  // ==========================================

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/cmp/
  cmp: <const T extends ExprArg<C>>(
    left: ExprArgInput<C, T>,
    right: ExprArgInput<C, T>,
  ) => Ret<C, -1 | 0 | 1> = this.varargs("cmp");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/eq/
  eq: ComparisonOp<C> = this.varargs("eq");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/gt/
  gt: ComparisonOp<C> = this.varargs("gt");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/gte/
  gte: ComparisonOp<C> = this.varargs("gte");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/lt/
  lt: ComparisonOp<C> = this.varargs("lt");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/lte/
  lte: ComparisonOp<C> = this.varargs("lte");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/ne/
  ne: ComparisonOp<C> = this.varargs("ne");

  // Boolean operators (fully typed from ExprBuilder)
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/and/
  and: VarargsBooleanOp<C> = this.varargs("and");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/or/
  or: VarargsBooleanOp<C> = this.varargs("or");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/nor/
  nor: VarargsBooleanOp<C> = this.varargs("nor");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/not/
  not: <const T extends ExprArg<C>>(exp: ExprArgInput<C, T>) => Ret<C, boolean> = this.id("not");

  // Conditional operators (fully typed from ExprBuilder)
  /** Evaluates a boolean expression and returns one of two values.
   * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/cond/ */
  cond: <
    const B extends BooleanArg<C>,
    const T extends ExprArg<C>,
    const E extends ExprArg<C>,
  >(options: {
    if: B & BooleanArgInput<C, B>;
    then: T & ExprArgInput<C, T>;
    else: E & ExprArgInput<C, E>;
  }) => Ret<C, ResolveType<C, T> | ResolveType<C, E>> = this.options("cond");

  /** Returns the first non-null/non-missing expression (null coalescing).
   * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/ifNull/ */
  ifNull: <const T extends ExprArg<C>[]>(
    ...args: { [K in keyof T]: T[K] & ExprArgInput<C, T[K]> }
  ) => Ret<C, FirstNotNil<ResolveTypes<C, T>>> = this.varargs("ifNull");

  /** Evaluates a series of case expressions, returning the value of the first match.
   * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/switch/ */
  switch: <
    const Br extends readonly { case: BooleanArg<C>; then: ExprArg<C> }[],
    const D extends ExprArg<C>,
  >(options: {
    branches: Br;
    default: D;
  }) => Ret<C, ResolveType<C, Br[number]["then"]> | ResolveType<C, D>> = this.options("switch");

  // String operators (fully typed from ExprBuilder)
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/concat/
  concat: <const T extends StringArg<C>[]>(
    ...args: { [K in keyof T]: StringArgInput<C, T[K]> }
  ) => Ret<C, ConcatStrings<{ [K in keyof T]: ResolveType<C, T[K]> }>> = this.varargs("concat");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexOfBytes/
  indexOfBytes: <
    const S extends StringArg<C>,
    const Sub extends StringArg<C>,
    const N extends NumericArg<C>,
  >(
    string: StringArgInput<C, S>,
    substring: StringArgInput<C, Sub>,
    start?: N & NumericArgInput<C, N>,
    end?: N & NumericArgInput<C, N>,
  ) => Ret<C, number> = (string, substring, start, end) => {
    const args: unknown[] = [string, substring];
    if (start !== undefined) args.push(start);
    if (end !== undefined) args.push(end);
    return this.varargs("indexOfBytes")(...args);
  };

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexOfCP/
  indexOfCP: <
    const S extends StringArg<C>,
    const Sub extends StringArg<C>,
    const N extends NumericArg<C>,
  >(
    string: StringArgInput<C, S>,
    substring: StringArgInput<C, Sub>,
    start?: N & NumericArgInput<C, N>,
    end?: N & NumericArgInput<C, N>,
  ) => Ret<C, number> = (string, substring, start, end) => {
    const args: unknown[] = [string, substring];
    if (start !== undefined) args.push(start);
    if (end !== undefined) args.push(end);
    return this.varargs("indexOfCP")(...args);
  };

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/ltrim/
  ltrim: <const T extends StringArg<C>>(options: {
    input: StringArgInput<C, T>;
    chars?: StringArgInput<C, StringArg<C>>;
  }) => Ret<C, string> = this.options("ltrim");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/regexFind/
  regexFind: <const T extends StringArg<C>>(options: {
    input: StringArgInput<C, T>;
    regex: StringArgInput<C, StringArg<C>> | RegExp;
    options?: StringArgInput<C, StringArg<C>>;
  }) => Ret<C, { match: string; idx: number; captures: string[] } | null> =
    this.options("regexFind");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/regexFindAll/
  regexFindAll: <const T extends StringArg<C>>(options: {
    input: StringArgInput<C, T>;
    regex: StringArgInput<C, StringArg<C>> | RegExp;
    options?: StringArgInput<C, StringArg<C>>;
  }) => Ret<C, { match: string; idx: number; captures: string[] }[]> = this.options("regexFindAll");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/regexMatch/
  regexMatch: <const T extends StringArg<C>>(options: {
    input: StringArgInput<C, T>;
    regex: StringArgInput<C, StringArg<C>> | RegExp;
    options?: StringArgInput<C, StringArg<C>>;
  }) => Ret<C, boolean> = this.options("regexMatch");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceAll/
  replaceAll: <const T extends StringArg<C>>(options: {
    input: StringArgInput<C, T>;
    find: StringArgInput<C, StringArg<C>>;
    replacement: StringArgInput<C, StringArg<C>>;
  }) => Ret<C, string> = this.options("replaceAll");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceOne/
  replaceOne: <const T extends StringArg<C>>(options: {
    input: StringArgInput<C, T>;
    find: StringArgInput<C, StringArg<C>>;
    replacement: StringArgInput<C, StringArg<C>>;
  }) => Ret<C, string> = this.options("replaceOne");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/rtrim/
  rtrim: <const T extends StringArg<C>>(options: {
    input: StringArgInput<C, T>;
    chars?: StringArgInput<C, StringArg<C>>;
  }) => Ret<C, string> = this.options("rtrim");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/split/
  split: <const S extends StringArg<C>, const D extends StringArg<C>>(
    string: StringArgInput<C, S>,
    delimiter: StringArgInput<C, D>,
  ) => Ret<C, string[]> = this.varargs("split");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/strcasecmp/
  strcasecmp: BinaryStringToNumber<C> = this.varargs("strcasecmp");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/strLenBytes/
  strLenBytes: UnaryStringToNumber<C> = this.id("strLenBytes");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/strLenCP/
  strLenCP: UnaryStringToNumber<C> = this.id("strLenCP");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/substr/
  substr: <const S extends StringArg<C>, const N extends NumericArg<C>>(
    string: StringArgInput<C, S>,
    start: (N & NumericArgInput<C, N>) | number,
    length: (N & NumericArgInput<C, N>) | number,
  ) => Ret<C, string> = this.varargs("substr");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/substrBytes/
  substrBytes: <const S extends StringArg<C>, const N extends NumericArg<C>>(
    string: StringArgInput<C, S>,
    start: N & NumericArgInput<C, N>,
    length: N & NumericArgInput<C, N>,
  ) => Ret<C, string> = this.varargs("substrBytes");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/substrCP/
  substrCP: <const S extends StringArg<C>, const N extends NumericArg<C>>(
    string: StringArgInput<C, S>,
    start: N & NumericArgInput<C, N>,
    length: N & NumericArgInput<C, N>,
  ) => Ret<C, string> = this.varargs("substrCP");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toLower/
  toLower: UnaryStringToString<C> = this.id("toLower");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toUpper/
  toUpper: UnaryStringToString<C> = this.id("toUpper");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/trim/
  trim: <const T extends StringArg<C>>(options: {
    input: StringArgInput<C, T>;
    chars?: StringArgInput<C, StringArg<C>>;
  }) => Ret<C, string> = this.options("trim");

  // Set operators (fully typed from ExprBuilder)
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/allElementsTrue/
  allElementsTrue: UnaryArrayToBool<C> = this.id("allElementsTrue");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/anyElementTrue/
  anyElementTrue: UnaryArrayToBool<C> = this.id("anyElementTrue");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/setDifference/
  setDifference: <const A extends ArrayArg<C>, const B extends ArrayArg<C>>(
    left: A & ArrayArgInput<C, A>,
    right: B & ArrayArgInput<C, B>,
  ) => Ret<C, ElemType<ResolveType<C, A>>[]> = this.varargs("setDifference");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/setEquals/
  setEquals: <const T extends ArrayArg<C>[]>(
    ...args: { [K in keyof T]: T[K] & ArrayArgInput<C, T[K]> }
  ) => Ret<C, boolean> = this.varargs("setEquals");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/setIntersection/
  setIntersection: <const T extends ArrayArg<C>[]>(
    ...args: { [K in keyof T]: T[K] & ArrayArgInput<C, T[K]> }
  ) => Ret<C, ElemType<ResolveType<C, T[number]>>[]> = this.varargs("setIntersection");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/setIsSubset/
  setIsSubset: BinaryArrayOp<C> = this.varargs("setIsSubset");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/setUnion/
  setUnion: <const T extends (ArrayArg<C> | ArrayLiteral<C>)[]>(
    ...args: T
  ) => Ret<C, ElemType<ResolveType<C, T[number]>>[]> = this.varargs("setUnion");

  // Date operators (fully typed from ExprBuilder)
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateAdd/
  dateAdd: <const T extends DateArg<C>, const N extends NumericArg<C>>(options: {
    startDate: T & DateArgInput<C, T>;
    unit: string;
    amount: N & NumericArgInput<C, N>;
    timezone?: StringArg<C>;
  }) => Ret<C, Date> = this.options("dateAdd");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateDiff/
  dateDiff: <const S extends DateArg<C>, const E extends DateArg<C>>(options: {
    startDate: S & DateArgInput<C, S>;
    endDate: E & DateArgInput<C, E>;
    unit: string;
    timezone?: StringArg<C>;
    startOfWeek?: string;
  }) => Ret<C, number> = this.options("dateDiff");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromParts/
  dateFromParts: <const T>(opts: T) => Ret<C, Date> = this.options("dateFromParts");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromString/
  dateFromString: <const D extends StringArg<C>>(options: {
    dateString: D;
    format?: StringArg<C>;
    timezone?: StringArg<C>;
    onError?: ExprArgInput<C, ExprArg<C>>;
    onNull?: ExprArgInput<C, ExprArg<C>>;
  }) => Ret<C, Date> = this.options("dateFromString");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateSubtract/
  dateSubtract: <const T extends DateArg<C>, const N extends NumericArg<C>>(options: {
    startDate: T & DateArgInput<C, T>;
    unit: string;
    amount: N & NumericArgInput<C, N>;
    timezone?: StringArg<C>;
  }) => Ret<C, Date> = this.options("dateSubtract");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToParts/
  dateToParts: <
    const T extends DateArg<C>,
    const I extends boolean | undefined = undefined,
  >(options: {
    date: T & DateArgInput<C, T>;
    timezone?: StringArg<C>;
    iso8601?: I;
  }) => Ret<
    C,
    I extends true ?
      {
        isoWeekYear: number;
        isoWeek: number;
        isoDayOfWeek: number;
        hour: number;
        minute: number;
        second: number;
        millisecond: number;
      }
    : {
        year: number;
        month: number;
        day: number;
        hour: number;
        minute: number;
        second: number;
        millisecond: number;
      }
  > = this.options("dateToParts");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToString/
  dateToString: <const T extends DateArg<C>>(options: {
    date: T & DateArgInput<C, T>;
    format?: StringArg<C>;
    timezone?: StringArg<C>;
    onNull?: ExprArgInput<C, ExprArg<C>>;
  }) => Ret<C, string> = this.options("dateToString");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateTrunc/
  dateTrunc: <const T extends DateArg<C>, const N extends NumericArg<C>>(options: {
    date: T & DateArgInput<C, T>;
    unit: string;
    binSize?: N & NumericArgInput<C, N>;
    timezone?: StringArg<C>;
    startOfWeek?: string;
  }) => Ret<C, Date> = this.options("dateTrunc");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfMonth/
  dayOfMonth: UnaryDateToNumber<C> = this.id("dayOfMonth");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfWeek/
  dayOfWeek: UnaryDateToNumber<C> = this.id("dayOfWeek");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfYear/
  dayOfYear: UnaryDateToNumber<C> = this.id("dayOfYear");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/hour/
  hour: UnaryDateToNumber<C> = this.id("hour");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/isoDayOfWeek/
  isoDayOfWeek: UnaryDateToNumber<C> = this.id("isoDayOfWeek");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/isoWeek/
  isoWeek: UnaryDateToNumber<C> = this.id("isoWeek");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/isoWeekYear/
  isoWeekYear: UnaryDateToNumber<C> = this.id("isoWeekYear");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/millisecond/
  millisecond: UnaryDateToNumber<C> = this.id("millisecond");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/minute/
  minute: UnaryDateToNumber<C> = this.id("minute");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/month/
  month: UnaryDateToNumber<C> = this.id("month");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/second/
  second: UnaryDateToNumber<C> = this.id("second");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/week/
  week: UnaryDateToNumber<C> = this.id("week");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/year/
  year: UnaryDateToNumber<C> = this.id("year");

  // Type conversion operators (fully typed from ExprBuilder)
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/convert/
  convert: {
    <const I extends ExprArg<C>, const To extends ConvertTo, const ON extends ExprArg<C>>(options: {
      input: ExprArgInput<C, I>;
      to: To;
      onError?: ExprArgInput<C, ExprArg<C>>;
      onNull: ExprArgInput<C, ON>;
    }): Ret<C, ConvertOutput<To> | ResolveType<C, ON>>;
    <const I extends ExprArg<C>, const To extends ConvertTo>(options: {
      input: ExprArgInput<C, I>;
      to: To;
      onError?: ExprArgInput<C, ExprArg<C>>;
      onNull?: undefined;
    }): Ret<C, NullishResult<C, I, ConvertOutput<To>>>;
  } = this.options("convert");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/isNumber/
  isNumber: UnaryTypeConversion<C, boolean> = this.id("isNumber");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toBool/
  toBool: UnaryTypeConversion<C, boolean> = this.id("toBool");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toDate/
  toDate: UnaryTypeConversion<C, Date> = this.id("toDate");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toDecimal/
  toDecimal: UnaryTypeConversion<C, Decimal128> = this.id("toDecimal");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toDouble/
  toDouble: UnaryTypeConversion<C, number> = this.id("toDouble");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toInt/
  toInt: UnaryTypeConversion<C, number> = this.id("toInt");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toLong/
  toLong: UnaryTypeConversion<C, number> = this.id("toLong");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toObjectId/
  toObjectId: UnaryTypeConversion<C, ObjectId> = this.id("toObjectId");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toString/
  toString: UnaryTypeConversion<C, string> = this.id("toString");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/type/
  type: <const T extends ExprArg<C>>(
    exp: ExprArgInput<C, T>,
  ) => Ret<
    C,
    | "double"
    | "string"
    | "object"
    | "array"
    | "binData"
    | "undefined"
    | "objectId"
    | "bool"
    | "date"
    | "null"
    | "regex"
    | "dbPointer"
    | "javascript"
    | "javascriptWithScope"
    | "symbol"
    | "int"
    | "timestamp"
    | "long"
    | "decimal"
    | "missing"
    | "minKey"
    | "maxKey"
  > = this.id("type");

  // Object operators (properly typed from ExprBuilder)
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/getField/
  getField: <const F extends string, const I extends ExprArg<C> = never>(
    options: F | { field: F; input?: ExprArgInput<C, I> },
  ) => Ret<
    C,
    [I] extends [never] ?
      F extends keyof C ?
        C[F]
      : never
    : F extends keyof ResolveType<C, I> ? ResolveType<C, I>[F]
    : never
  > = options =>
    typeof options === "string" ? this.id("getField")(options) : this.options("getField")(options);

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/mergeObjects/
  mergeObjects: {
    // Overload for DeepSpecInput (mix of field refs and object literals)
    <const T extends [DeepSpecInput<C, any>, ...DeepSpecInput<C, any>[]]>(
      ...args: T
    ): Ret<C, MergeObjects<C, T>>;
    // Overload for Dict<ExprArgInput> with ShallowMergeObjects
    <const T extends Dict<ExprArgInput<C, any>>[]>(
      ...args: T
    ): Ret<
      C,
      ShallowMergeObjectsOverride<{
        [I in keyof T]: { [K in keyof T[I]]: T[I][K] extends Ret<any, infer U> ? U : T[I][K] };
      }>
    >;
    // Original overload for ExprArg
    <const T extends [ExprArg<C>, ...ExprArg<C>[]]>(
      ...args: ExprArgInputs<C, T>
    ): Ret<C, MergeObjects<C, T>>;
  } = this.flexible("mergeObjects");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/setField/
  setField: <
    const F extends string,
    const I extends ExprArg<C>,
    const V extends ExprArg<C>,
  >(options: {
    field: F;
    input: ExprArgInput<C, I>;
    value: ExprArgInput<C, V>;
  }) => Ret<C, ResolveType<C, I> & Record<F, ResolveType<C, V>>> = this.options("setField");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/unsetField/
  unsetField: <const F extends string, const I extends ExprArg<C>>(options: {
    field: F;
    input: ExprArgInput<C, I>;
  }) => Ret<C, Omit<ResolveType<C, I>, F>> = this.options("unsetField");

  // Variable operators (fully typed from ExprBuilder)
  /** Binds variables for use within a scoped expression.
   * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/let/ */
  let: <
    const V extends Dict<ExprArg<C>>,
    const I extends ExprArg<MergeVars<C, ResolveVarSpec<C, V>>>,
  >(options: {
    vars: { [K in keyof V]: ExprArgInput<C, V[K]> };
    in: ($: BaseBuilder<MergeVars<C, ResolveVarSpec<C, V>>>) => I;
  }) => Ret<C, ResolveType<MergeVars<C, ResolveVarSpec<C, V>>, I>> = this.options("let");

  // Literal operator (fully typed from ExprBuilder)
  /** Returns a value without parsing — prevents field path or operator interpretation.
   * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/literal/ */
  literal: <const T>(value: T) => Ret<C, T> = this.id("literal");

  // Misc operators (fully typed from ExprBuilder)
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/meta/
  meta: <const T extends "textScore" | "searchScore" | "searchHighlights" | "indexKey">(
    metaType: T,
  ) => Ret<
    C,
    T extends "textScore" | "searchScore" ? number
    : T extends "searchHighlights" ?
      { path: string; texts: { value: string; type: "text" | "hit" }[] }[]
    : T extends "indexKey" ? Dict<string>
    : never
  > = this.id("meta");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/sampleRate/
  sampleRate: <const T extends NumericArg<C>>(rate: T & NumericArgInput<C, T>) => Ret<C, boolean> =
    this.id("sampleRate");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/binarySize/
  binarySize: UnaryTypeConversion<C, number> = this.id("binarySize");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/bsonSize/
  bsonSize: UnaryTypeConversion<C, number> = this.id("bsonSize");

  // Bitwise operators (fully typed from ExprBuilder)
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitAnd/
  bitAnd: VarargsNumericOp<C> = this.varargs("bitAnd");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitOr/
  bitOr: VarargsNumericOp<C> = this.varargs("bitOr");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitXor/
  bitXor: VarargsNumericOp<C> = this.varargs("bitXor");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitNot/
  bitNot: UnaryNumericOp<C> = this.id("bitNot");
  // Trigonometric operators (fully typed from ExprBuilder)
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/sin/
  sin: UnaryNumericOp<C> = this.id("sin");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/cos/
  cos: UnaryNumericOp<C> = this.id("cos");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/tan/
  tan: UnaryNumericOp<C> = this.id("tan");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/asin/
  asin: UnaryNumericOp<C> = this.id("asin");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/acos/
  acos: UnaryNumericOp<C> = this.id("acos");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/atan/
  atan: UnaryNumericOp<C> = this.id("atan");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/atan2/
  atan2: BinaryNumericOp<C> = this.varargs("atan2");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/asinh/
  asinh: UnaryNumericOp<C> = this.id("asinh");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/acosh/
  acosh: UnaryNumericOp<C> = this.id("acosh");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/atanh/
  atanh: UnaryNumericOp<C> = this.id("atanh");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/sinh/
  sinh: UnaryNumericOp<C> = this.id("sinh");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/cosh/
  cosh: UnaryNumericOp<C> = this.id("cosh");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/tanh/
  tanh: UnaryNumericOp<C> = this.id("tanh");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/degreesToRadians/
  degreesToRadians: UnaryNumericOp<C> = this.id("degreesToRadians");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/radiansToDegrees/
  radiansToDegrees: UnaryNumericOp<C> = this.id("radiansToDegrees");

  // Dual-context operators (work as both expressions and accumulators)
  // These use AccumulatorExprArg to reject bare strings — use "$field" not "field"
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/sum/
  sum: {
    <const T extends AccumulatorExprArg<C>>(exp: AccumulatorArgInput<C, T>): Ret<C, number>;
    <const T extends AccumulatorExprArg<C>[]>(...args: AccumulatorArgInputs<C, T>): Ret<C, number>;
  } = this.flexible("sum");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/avg/
  avg: {
    <const T extends AccumulatorExprArg<C>>(
      exp: AccumulatorArgInput<C, T>,
    ): Ret<C, NullishResult<C, T, number>>;
    <const T extends AccumulatorExprArg<C>[]>(
      ...args: AccumulatorArgInputs<C, T>
    ): Ret<C, NullishResultFromArgs<C, T, number>>;
  } = this.flexible("avg");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/max/
  max: {
    <const T extends AccumulatorExprArg<C>>(
      exp: AccumulatorArgInput<C, T>,
    ): Ret<C, ResolveType<C, T>>;
    <const T extends AccumulatorExprArg<C>[]>(
      ...args: AccumulatorArgInputs<C, T>
    ): Ret<C, ResolveType<C, T[number]>>;
  } = this.flexible("max");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/min/
  min: {
    <const T extends AccumulatorExprArg<C>>(
      exp: AccumulatorArgInput<C, T>,
    ): Ret<C, ResolveType<C, T>>;
    <const T extends AccumulatorExprArg<C>[]>(
      ...args: AccumulatorArgInputs<C, T>
    ): Ret<C, ResolveType<C, T[number]>>;
  } = this.flexible("min");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/stdDevPop/
  stdDevPop: {
    <const T extends AccumulatorExprArg<C>>(
      exp: AccumulatorArgInput<C, T>,
    ): Ret<C, NullishResult<C, T, number>>;
    <const T extends AccumulatorExprArg<C>[]>(
      ...args: AccumulatorArgInputs<C, T>
    ): Ret<C, NullishResultFromArgs<C, T, number>>;
  } = this.flexible("stdDevPop");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/stdDevSamp/
  stdDevSamp: {
    <const T extends AccumulatorExprArg<C>>(
      exp: AccumulatorArgInput<C, T>,
    ): Ret<C, NullishResult<C, T, number>>;
    <const T extends AccumulatorExprArg<C>[]>(
      ...args: AccumulatorArgInputs<C, T>
    ): Ret<C, NullishResultFromArgs<C, T, number>>;
  } = this.flexible("stdDevSamp");
}

/**
 * Expression builder providing type-safe access to all MongoDB aggregation operators.
 *
 * Passed as `$` in stage callbacks (e.g., `$project($ => ...)`, `$match($ => ...)`).
 * Exposes arithmetic, string, array, date, conditional, and comparison operators
 * that mirror the MongoDB aggregation expression language with full type inference.
 */
export class ExprBuilder<C> extends BaseBuilder<C> {
  /** Include this field in `$project` output (equivalent to `1`). */
  readonly include = 1 as const;
  /** Exclude this field from `$project` output (equivalent to `0`). */
  readonly exclude = 0 as const;
}

// Accumulator Builder: Expression operators + accumulator operators
export class AccumulatorBuilder<C> extends BaseBuilder<C, "accumulator"> {
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/addToSet/
  addToSet: <const T extends ExprArg<C>>(exp: ExprArgInput<C, T>) => Ret<C, ResolveType<C, T>[]> =
    this.id("addToSet");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/count-accumulator/
  count: <const T extends Dict<never>>(options: T) => Ret<C, number> = this.options("count");

  // push with overloads for object literal vs single expression
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/push/
  push: <const T>(exp: T & DeepSpecInput<C, T>) => Ret<C, DeepSpecResolve<C, T>[]> =
    this.id("push");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/bottom/
  bottom: <const TOutput extends ExprArg<C>, const TSort extends SortBySpec<C>>(options: {
    output: ExprArgInput<C, TOutput>;
    sortBy: TSort;
  }) => Ret<C, ResolveType<C, TOutput>> = this.options("bottom");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/bottomN/
  bottomN: <
    const TOutput extends ExprArg<C>,
    const TSort extends SortBySpec<C>,
    const N extends NumericArg<C>,
  >(options: {
    output: ExprArgInput<C, TOutput>;
    sortBy: TSort;
    n: N & NumericArgInput<C, N>;
  }) => Ret<C, ResolveType<C, TOutput>[]> = this.options("bottomN");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/top/
  top: <const T extends SortBySpec<C>, const O extends ExprArg<C>>(options: {
    output: ExprArgInput<C, O>;
    sortBy: T;
  }) => Ret<C, ResolveType<C, O>> = this.options("top");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/topN/
  topN: <
    const T extends SortBySpec<C>,
    const N extends NumericArg<C>,
    const O extends ExprArg<C>,
  >(options: {
    output: ExprArgInput<C, O>;
    sortBy: T;
    n: N & NumericArgInput<C, N>;
  }) => Ret<C, ResolveType<C, O>[]> = this.options("topN");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/accumulator/
  accumulator: {
    <
      const InitFn extends (...args: any[]) => any,
      const AccExprs extends readonly ExprArg<C>[],
      const Result,
    >(
      options: TypedAccumulator<
        InitFn,
        { [K in keyof AccExprs]: ResolveType<C, AccExprs[K]> },
        Result
      > &
        Omit<
          TypedAccumulator<InitFn, { [K in keyof AccExprs]: ResolveType<C, AccExprs[K]> }, Result>,
          "accumulate" | "accumulateArgs"
        > & {
          accumulate: (
            state: ReturnType<InitFn>,
            ...args: { [K in keyof AccExprs]: ResolveType<C, AccExprs[K]> }
          ) => ReturnType<InitFn>;
          accumulateArgs: readonly [...AccExprs] & {
            [K in keyof AccExprs]: ExprArgInput<C, AccExprs[K]>;
          };
          finalize: (state: ReturnType<InitFn>) => Result;
        },
    ): Ret<C, Result>;
    <InitFn extends (...args: any[]) => any, AccArgs extends readonly unknown[], Result>(
      options: TypedAccumulator<InitFn, AccArgs, Result> & {
        finalize: (state: ReturnType<InitFn>) => Result;
      },
    ): Ret<C, Result>;
    <InitFn extends (...args: any[]) => any, AccExprs extends readonly ExprArg<C>[]>(
      options: TypedAccumulator<
        InitFn,
        { [K in keyof AccExprs]: ResolveType<C, AccExprs[K]> },
        ReturnType<InitFn>
      > &
        Omit<
          TypedAccumulator<
            InitFn,
            { [K in keyof AccExprs]: ResolveType<C, AccExprs[K]> },
            ReturnType<InitFn>
          >,
          "accumulate" | "accumulateArgs"
        > & {
          accumulate: (
            state: ReturnType<InitFn>,
            ...args: { [K in keyof AccExprs]: ResolveType<C, AccExprs[K]> }
          ) => ReturnType<InitFn>;
          accumulateArgs: readonly [...AccExprs] & {
            [K in keyof AccExprs]: ExprArgInput<C, AccExprs[K]>;
          };
          finalize?: undefined;
        },
    ): Ret<C, ReturnType<InitFn>>;
    <InitFn extends (...args: any[]) => any, AccArgs extends readonly unknown[]>(
      options: TypedAccumulator<InitFn, AccArgs, ReturnType<InitFn>> & {
        finalize?: undefined;
      },
    ): Ret<C, ReturnType<InitFn>>;
  } = (options: TypedAccumulator<any, any, any>) =>
    this.options("accumulator")(resolveAccumulator(options));

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/median/
  median: <const T extends NumericArg<C>>(options: {
    input: T & NumericArgInput<C, T>;
    method?: "approximate";
  }) => Ret<C, number> = this.options("median");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/percentile/
  percentile: <const T extends NumericArg<C>, const P extends readonly number[]>(options: {
    input: T & NumericArgInput<C, T>;
    p: P;
    method?: "approximate";
  }) => Ret<C, { [K in keyof P]: number }> = this.options("percentile");
}

// Window Builder: Accumulator operators + window operators
export class WindowBuilder<C> extends AccumulatorBuilder<C> {
  // Window-specific operators (properly typed from ExprBuilder)
  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/denseRank/
  denseRank: () => Ret<C, number> = this.nullary("denseRank");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/covariancePop/
  covariancePop: <const X extends NumericArg<C>, const Y extends NumericArg<C>>(
    x: X & NumericArgInput<C, X>,
    y: Y & NumericArgInput<C, Y>,
  ) => Ret<C, number | null> = this.varargs("covariancePop");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/covarianceSamp/
  covarianceSamp: <const X extends NumericArg<C>, const Y extends NumericArg<C>>(
    x: X & NumericArgInput<C, X>,
    y: Y & NumericArgInput<C, Y>,
  ) => Ret<C, number | null> = this.varargs("covarianceSamp");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/derivative/
  derivative: <const T extends NumericArg<C>>(options: {
    input: T & NumericArgInput<C, T>;
    unit?: TimeUnit;
    window?: WindowSpec;
  }) => Ret<C, number> = options => {
    const resolved = this._resolve(options) as Dict<unknown>;
    if ("window" in resolved) {
      const { window, ...operatorArgs } = resolved;
      return new Ret({ $derivative: operatorArgs, window });
    }
    return new Ret({ $derivative: resolved });
  };

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/documentNumber/
  documentNumber: () => Ret<C, number> = this.nullary("documentNumber");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/expMovingAvg/
  expMovingAvg: <const T extends NumericArg<C>>(options: {
    input: T & NumericArgInput<C, T>;
    N?: NumericArg<C>;
    alpha?: NumericArg<C>;
    window?: WindowSpec;
  }) => Ret<C, number> = options => {
    const resolved = this._resolve(options) as Dict<unknown>;
    if ("window" in resolved) {
      const { window, ...operatorArgs } = resolved;
      return new Ret({ $expMovingAvg: operatorArgs, window });
    }
    return new Ret({ $expMovingAvg: resolved });
  };

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/integral/
  integral: <const T extends NumericArg<C>>(options: {
    input: T & NumericArgInput<C, T>;
    unit?: TimeUnit;
    window?: WindowSpec;
  }) => Ret<C, number> = options => {
    const resolved = this._resolve(options) as Dict<unknown>;
    if ("window" in resolved) {
      const { window, ...operatorArgs } = resolved;
      return new Ret({ $integral: operatorArgs, window });
    }
    return new Ret({ $integral: resolved });
  };

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/linearFill/
  linearFill: <const T extends ExprArg<C>>(exp: ExprArgInput<C, T>) => Ret<C, ResolveType<C, T>> =
    this.id("linearFill");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/locf/
  locf: <const T extends ExprArg<C>>(exp: ExprArgInput<C, T>) => Ret<C, ResolveType<C, T>> =
    this.id("locf");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/percentRank/
  percentRank: () => Ret<C, number> = this.nullary("percentRank");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/rank/
  rank: () => Ret<C, number> = this.nullary("rank");

  // https://www.mongodb.com/docs/manual/reference/operator/aggregation/shift/
  shift: <const T extends ExprArg<C>, const B extends NumericArg<C>>(options: {
    output: ExprArgInput<C, T>;
    by: B & NumericArgInput<C, B>;
    default?: ExprArgInput<C, ExprArg<C>>;
  }) => Ret<C, ResolveType<C, T>> = this.options("shift");
}
