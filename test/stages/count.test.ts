// Runtime equivalent of count.test-d.ts
import { Schema as S } from "@effect/schema";
import { $count, $match, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const ScoreSchema = S.Struct({
  _id: ObjectIdSchema,
  subject: S.String,
  score: S.Number,
});

type Score = typeof ScoreSchema.Type;

const dbRegistry = registry("8.0", { scores: ScoreSchema });

describe("Count Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .scores.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          subject: "Math",
          score: 85,
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          subject: "English",
          score: 92,
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          subject: "Science",
          score: 78,
        },
        {
          _id: new ObjectId("000000000000000000000004"),
          subject: "History",
          score: 88,
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should count with basic field name", async () => {
    const ResultSchema = S.Struct({ total: S.Number });

    const results = await dbRegistry(db).scores.aggregate($count("total")).toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should count with descriptive field name", async () => {
    const ResultSchema = S.Struct({ passing_scores: S.Number });

    const results = await dbRegistry(db).scores.aggregate($count("passing_scores")).toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should count after match", async () => {
    const ResultSchema = S.Struct({ highScores: S.Number });

    const results = await dbRegistry(db)
      .scores.aggregate(
        $match($ => ({ score: { $gt: 80 } })),
        $count("highScores"),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should count with literal field name", async () => {
    const ResultSchema = S.Struct({ myCount: S.Number });

    const results = await dbRegistry(db).scores.aggregate($count("myCount")).toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
