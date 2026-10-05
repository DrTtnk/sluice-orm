import type { Simplify } from "type-fest";

import type { OpaqueError } from "./type-errors.js";

type KeysWithFlag<P, Flag> = { [K in keyof P]: P[K] extends Flag ? K : never }[keyof P];
type IsDynamic<P> =
  true extends { [K in keyof P]: 0 | 1 extends P[K] ? true : false }[keyof P] ? true : false;
type Projected<C, P> = Simplify<
  IsDynamic<P> extends true ? Partial<C>
  : Exclude<KeysWithFlag<P, 0>, "_id"> extends never ?
    KeysWithFlag<P, 1> extends never ?
      Omit<C, KeysWithFlag<P, 0>>
    : Pick<C, Extract<KeysWithFlag<P, 1>, keyof C>> &
        (P extends { _id: 0 } ? unknown : Pick<C, Extract<"_id", keyof C>>)
  : Omit<C, KeysWithFlag<P, 0>>
>;

export type FindResult<C, Options> =
  Options extends { projection: infer P } ? Projected<C, P>
  : "projection" extends keyof Options ? Partial<C>
  : C;
export type ValidateProjectionOptions<Options> =
  Options extends { projection: infer P } ?
    IsDynamic<P> extends true ? unknown
    : Exclude<KeysWithFlag<P, 0>, "_id"> extends never ? unknown
    : Exclude<KeysWithFlag<P, 1>, "_id"> extends never ? unknown
    : OpaqueError<"Projection cannot mix inclusion and exclusion except for _id">
  : unknown;
