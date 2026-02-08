// eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
export const functionToString = (fn: Function): string => {
  const fnStr = fn.toString();

  if (!fnStr.includes("=>")) return fnStr;

  // Handles: (x, y) => ..., x => ..., (x) => ...
  const paramMatch = /^(?:\(([^)]*)\)|(\w+))\s*=>/.exec(fnStr);
  if (!paramMatch) throw new Error(`Cannot parse arrow function parameters: ${fnStr}`);

  const params = paramMatch[1] ?? paramMatch[2];

  const arrowIndex = fnStr.indexOf("=>");
  const body = fnStr.slice(arrowIndex + 2).trim();

  // Check if body is wrapped in braces or is an expression
  const hasBlock = body.startsWith("{");
  return hasBlock ? `function(${params}) ${body}` : `function(${params}) { return ${body}; }`;
};

export type TypedAccumulator<
  InitFn extends (...args: any[]) => any,
  AccArgs extends readonly unknown[],
  Result = ReturnType<InitFn>,
> = {
  init: InitFn;
  initArgs: Parameters<InitFn>;
  accumulate: (state: ReturnType<InitFn>, ...args: [...AccArgs]) => ReturnType<InitFn>;
  accumulateArgs: Readonly<{ [K in keyof AccArgs]: string }>;
  merge: (state1: ReturnType<InitFn>, state2: ReturnType<InitFn>) => ReturnType<InitFn>;
  finalize?: (state: ReturnType<InitFn>) => Result;
  lang: "js";
};

export const resolveAccumulator = <
  InitFn extends (...args: any[]) => any,
  AccArgs extends readonly unknown[],
  Result,
>(
  config: TypedAccumulator<InitFn, AccArgs, Result>,
) => {
  const result = {
    init: functionToString(config.init),
    initArgs: config.initArgs,
    accumulate: functionToString(config.accumulate),
    accumulateArgs: config.accumulateArgs,
    merge: functionToString(config.merge),
    lang: config.lang,
  };

  return config.finalize ? { ...result, finalize: functionToString(config.finalize) } : result;
};
