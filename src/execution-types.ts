import type { Effect } from "effect";

import type { MongoError } from "./registryEffect.js";

export type ExecutionMode = "promise" | "effect";
export type OperationResult<T, Mode extends ExecutionMode> =
  Mode extends "effect" ? Effect.Effect<T, MongoError> : Promise<T>;
