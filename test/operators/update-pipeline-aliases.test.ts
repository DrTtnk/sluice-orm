/**
 * Tests for update pipeline stage aliases
 */

import { Schema as S } from "@effect/schema";
import { registry } from "@sluice/sluice";
import { $addFields, $replaceWith, $set, $unset } from "@sluice/sluice";
import type { Db } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { setup, teardown } from "../utils/setup.js";

const ComplexSchema = S.Struct({
  _id: S.String,
  score: S.Number,
  history: S.Array(S.Number),
  meta: S.Struct({
    version: S.Number,
    updatedAt: S.Date,
    tags: S.Array(S.String),
  }),
});

describe("Update Pipeline Stage Aliases", () => {
  const dbRegistry = registry("8.0", { complex: ComplexSchema });
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;
  }, 60000);

  afterAll(async () => {
    await teardown();
  });

  it("should support $addFields as alias for $set", async () => {
    const complexColl = dbRegistry(db).complex;

    await complexColl
      .insertOne({
        _id: "alias1",
        score: 10,
        history: [],
        meta: {
          version: 1,
          updatedAt: new Date(),
          tags: [],
        },
      })
      .execute();

    await complexColl
      .updateOne(
        $ => ({ _id: "alias1" }),
        $ =>
          $.pipe(
            $addFields($ => ({ newField: $.add("$score", 5) })),
            $unset("newField"),
          ),
      )
      .execute();

    const doc = await complexColl.findOne($ => ({ _id: "alias1" })).toOne();
    expect(doc?.score).toBe(10);
  });

  it("should support $replaceWith as alias for $replaceRoot", async () => {
    const complexColl = dbRegistry(db).complex;

    await complexColl
      .insertOne({
        _id: "alias2",
        score: 20,
        history: [],
        meta: {
          version: 1,
          updatedAt: new Date(),
          tags: [],
        },
      })
      .execute();

    await complexColl
      .updateOne(
        $ => ({ _id: "alias2" }),
        $ => $.pipe($replaceWith($ => $.mergeObjects("$$ROOT", { score: 100 }))),
      )
      .execute();

    const doc = await complexColl.findOne($ => ({ _id: "alias2" })).toOne();
    expect(doc?.score).toBe(100);
  });

  it("should allow mixing aliases with regular stages", async () => {
    const complexColl = dbRegistry(db).complex;

    await complexColl
      .insertOne({
        _id: "alias3",
        score: 30,
        history: [1, 2],
        meta: {
          version: 1,
          updatedAt: new Date(),
          tags: [],
        },
      })
      .execute();

    await complexColl
      .updateOne(
        $ => ({ _id: "alias3" }),
        $ =>
          $.pipe(
            $addFields($ => ({ temp: $.multiply("$score", 2) })),
            $set($ => ({ history: $.concatArrays("$history", ["$temp"]) })),
            $unset("temp"),
          ),
      )
      .execute();

    const doc = await complexColl.findOne($ => ({ _id: "alias3" })).toOne();
    expect(doc?.history).toEqual([1, 2, 60]); // 30 * 2 = 60
  });
});
