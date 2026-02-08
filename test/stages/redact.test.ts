// Runtime equivalent of redact.test-d.ts
import { Schema as S } from "@effect/schema";
import { $match, $redact, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const ForecastSchema = S.Struct({
  _id: ObjectIdSchema,
  year: S.Number,
  title: S.String,
  tags: S.Array(S.String),
  subspecies: S.optional(
    S.Struct({
      name: S.String,
      tags: S.Array(S.String),
    }),
  ),
});

type Forecast = typeof ForecastSchema.Type;

const dbRegistry = registry("8.0", { forecasts: ForecastSchema });

describe("Redact Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .forecasts.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          year: 2024,
          title: "Weather Forecast",
          tags: ["weather", "forecast"],
          subspecies: {
            name: "Rain",
            tags: ["wet"],
          },
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          year: 2023,
          title: "Climate Report",
          tags: ["climate"],
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should redact with KEEP", async () => {
    const ResultSchema = ForecastSchema;

    const results = await dbRegistry(db).forecasts.aggregate($redact("$$KEEP")).toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should redact with PRUNE", async () => {
    const ResultSchema = ForecastSchema;

    const results = await dbRegistry(db).forecasts.aggregate($redact("$$PRUNE")).toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should redact with DESCEND", async () => {
    const ResultSchema = ForecastSchema;

    const results = await dbRegistry(db).forecasts.aggregate($redact("$$DESCEND")).toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should redact after match", async () => {
    const ResultSchema = ForecastSchema;

    const results = await dbRegistry(db)
      .forecasts.aggregate(
        $match($ => ({ year: 2024 })),
        $redact("$$KEEP"),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
