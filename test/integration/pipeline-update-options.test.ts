import { Schema as S } from "@effect/schema";
import type { Db } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { registry } from "../../src/registry.js";
import { setup, teardown } from "../utils/setup.js";

const ScoreSchema = S.Struct({
  _id: S.String,
  score: S.Number,
  label: S.String,
});

describe("Pipeline-form updates accept options and every update method", () => {
  const dbRegistry = registry("8.0", { scores: ScoreSchema });
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
  }, 120000);

  afterAll(teardown);

  it("updateOne with a pipeline and upsert creates the document", async () => {
    const scores = dbRegistry(db).scores;

    const result = await scores
      .updateOne(
        () => ({ _id: "upserted" }),
        $ => $.pipe($.set(() => ({ score: 5, label: "created" }))),
        { upsert: true },
      )
      .execute();

    expect(result.upsertedCount).toBe(1);
    expect(await scores.findOne(() => ({ _id: "upserted" })).toOne()).toEqual({
      _id: "upserted",
      score: 5,
      label: "created",
    });
  });

  it("updateMany with a pipeline accepts a hint", async () => {
    const scores = dbRegistry(db).scores;
    await scores
      .insertMany([
        { _id: "many-1", score: 1, label: "many" },
        { _id: "many-2", score: 2, label: "many" },
      ])
      .execute();

    const result = await scores
      .updateMany(
        () => ({ label: "many" }),
        $ => $.pipe($.set($ => ({ score: $.add("$score", 10) }))),
        { hint: { _id: 1 } },
      )
      .execute();

    expect(result.modifiedCount).toBe(2);
    expect((await scores.findOne(() => ({ _id: "many-2" })).toOne())?.score).toBe(12);
  });

  it("findOneAndUpdate with a pipeline returns the updated document", async () => {
    const scores = dbRegistry(db).scores;
    await scores.insertOne({ _id: "find-and-update", score: 1, label: "one" }).execute();

    const updated = await scores
      .findOneAndUpdate(
        () => ({ _id: "find-and-update" }),
        $ => $.pipe($.set($ => ({ score: $.add("$score", 10) }))),
        { returnDocument: "after" },
      )
      .execute();

    expect(updated?.score).toBe(11);
  });

  it("bulkWrite updateOne and updateMany accept a pipeline", async () => {
    const scores = dbRegistry(db).scores;
    await scores
      .insertMany([
        { _id: "bulk-1", score: 1, label: "bulk" },
        { _id: "bulk-2", score: 2, label: "bulk" },
        { _id: "bulk-3", score: 3, label: "other" },
      ])
      .execute();

    const result = await scores
      .bulkWrite([
        {
          updateOne: {
            filter: { _id: "bulk-1" },
            update: $ => $.pipe($.set($ => ({ score: $.add("$score", 100) }))),
          },
        },
        {
          updateMany: {
            filter: { label: "bulk" },
            update: $ => $.pipe($.set($ => ({ label: $.concat("$label", "-done") }))),
          },
        },
      ])
      .execute();

    expect(result.modifiedCount).toBe(3);
    expect(await scores.findOne(() => ({ _id: "bulk-1" })).toOne()).toEqual({
      _id: "bulk-1",
      score: 101,
      label: "bulk-done",
    });
  });
});
