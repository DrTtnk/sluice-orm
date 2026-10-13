/**
 * A pipeline update accepts upsert and hint, but not arrayFilters: MongoDB rejects arrayFilters with a pipeline.
 */
import { Schema as S } from "@effect/schema";
import type { Db } from "mongodb";

import { collection } from "../../../src/sluice.js";

const ScoreSchema = S.Struct({ _id: S.String, score: S.Number });

const mockDb = {} as Db;
const scores = collection("scores", ScoreSchema, mockDb.collection("scores"));

scores.updateOne(
  () => ({ _id: "a" }),
  $ => $.pipe($.set(() => ({ score: 1 }))),
  // @ts-expect-error - arrayFilters is not allowed with a pipeline update
  { arrayFilters: [{ x: 1 }] },
);

scores.updateMany(
  () => ({ _id: "a" }),
  $ => $.pipe($.set(() => ({ score: 1 }))),
  // @ts-expect-error - unknown option
  { upsrt: true },
);

scores.findOneAndUpdate(
  () => ({ _id: "a" }),
  $ => $.pipe($.set(() => ({ score: 1 }))),
  // @ts-expect-error - arrayFilters is not allowed with a pipeline update
  { arrayFilters: [{ x: 1 }] },
);
