import { Schema as S } from "@effect/schema";
import { $densify, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const WeatherSchema = S.Struct({
  _id: ObjectIdSchema,
  timestamp: S.Date,
  temp: S.Number,
});

const CoffeeSchema = S.Struct({
  _id: ObjectIdSchema,
  altitude: S.Number,
  variety: S.String,
  score: S.Number,
});

const dbRegistry = registry("8.0", {
  weather: WeatherSchema,
  coffee: CoffeeSchema,
});

describe("Densify Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .weather.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          timestamp: new Date("2021-05-18T00:00:00Z"),
          temp: 12,
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          timestamp: new Date("2021-05-18T03:00:00Z"),
          temp: 15,
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          timestamp: new Date("2021-05-18T06:00:00Z"),
          temp: 18,
        },
      ])
      .execute();

    await dbRegistry(db)
      .coffee.insertMany([
        {
          _id: new ObjectId("000000000000000000000004"),
          altitude: 600,
          variety: "Arabica",
          score: 85,
        },
        {
          _id: new ObjectId("000000000000000000000005"),
          altitude: 1000,
          variety: "Arabica",
          score: 88,
        },
        {
          _id: new ObjectId("000000000000000000000006"),
          altitude: 750,
          variety: "Robusta",
          score: 82,
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should densify date field with time units", async () => {
    const ResultSchema = S.Struct({
      _id: S.optional(ObjectIdSchema),
      timestamp: S.Date,
      temp: S.optional(S.Number),
    });

    const results = await dbRegistry(db)
      .weather.aggregate(
        $densify({
          field: "timestamp",
          range: {
            step: 1,
            unit: "hour",
            bounds: [new Date("2021-05-18T00:00:00Z"), new Date("2021-05-18T08:00:00Z")],
          },
        }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should densify numeric field", async () => {
    const ResultSchema = S.Struct({
      _id: S.optional(ObjectIdSchema),
      altitude: S.Number,
      variety: S.optional(S.String),
      score: S.optional(S.Number),
    });

    const results = await dbRegistry(db)
      .coffee.aggregate(
        $densify({
          field: "altitude",
          range: {
            step: 200,
            bounds: "full",
          },
        }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should densify with partition", async () => {
    const ResultSchema = S.Struct({
      _id: S.optional(ObjectIdSchema),
      altitude: S.Number,
      variety: S.String,
      score: S.optional(S.Number),
    });

    const results = await dbRegistry(db)
      .coffee.aggregate(
        $densify({
          field: "altitude",
          partitionByFields: ["variety"],
          range: {
            step: 100,
            bounds: "partition",
          },
        }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
