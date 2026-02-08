import { expectType } from "tsd";

import type { ShallowMergeObjectsOverride } from "../../../src/type-utils.js";

// Last-wins merge for overlapping keys
{
  type Result = ShallowMergeObjectsOverride<[{ a: number }, { a: string }]>;
  expectType<Result>({} as { a: string });
}

// Last-wins with multiple objects
{
  type Result = ShallowMergeObjectsOverride<
    [
      { a: number; b: string },
      { a: boolean; c: number; f?: number },
      { a: null; d: Date; f: number },
    ]
  >;
  expectType<Result>({} as { a: null; b: string; c: number; d: Date; f: number });
}

// Null and undefined are ignored at merge boundary
{
  type Result = ShallowMergeObjectsOverride<[{ a: number }, { a: string | null }]>;
  expectType<Result>({} as { a: string | null });
}
