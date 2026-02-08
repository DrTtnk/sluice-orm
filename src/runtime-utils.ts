import { ObjectId } from "mongodb";

import { ExprBuilder, Ret, type RuntimeValue } from "./builder.js";
import type { Dict } from "./type-utils.js";

type Stage = Dict<unknown>;
type RuntimeAgg = { stages?: Stage[] } & Dict<unknown>;

// Deep unwrap Ret objects to get runtime values
export const unwrapRet = (value: unknown): unknown => {
  const _foo = value as RuntimeValue;
  if (_foo instanceof Ret) return unwrapRet(_foo.__fn);
  if (Array.isArray(_foo)) return _foo.map(unwrapRet);
  if (_foo instanceof Date) return _foo;
  if (_foo instanceof ObjectId) return _foo;
  if (_foo instanceof RegExp) return _foo;
  if (typeof _foo !== "object" || _foo === null) return _foo;

  return Object.entries(_foo).reduce((acc, [k, v]) => ({ ...acc, [k]: unwrapRet(v) }), {});
};

type StageSpec<C> = unknown | ((builder: ExprBuilder<C>) => unknown);

export const resolveStage = <C>(stage: StageSpec<C>): unknown =>
  unwrapRet(typeof stage === "function" ? stage(new ExprBuilder<C>()) : stage);

export const pushStage =
  <TOptions>(fn: (options: TOptions) => Stage) =>
  (options: TOptions) =>
  (agg: RuntimeAgg): RuntimeAgg => ({ ...agg, stages: [...(agg.stages ?? []), fn(options)] });
