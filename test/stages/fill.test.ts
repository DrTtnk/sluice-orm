import { Schema as S } from "@effect/schema";
import { $fill, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const DailySalesSchema = S.Struct({
  _id: ObjectIdSchema,
  date: S.Date,
  bootsSold: S.NullOr(S.Number),
  sandalsSold: S.NullOr(S.Number),
});

const HourlyReadingSchema = S.Struct({
  _id: ObjectIdSchema,
  timestamp: S.Date,
  temperature: S.NullOr(S.Number),
  humidity: S.NullOr(S.Number),
  sensorId: S.String,
});

const dbRegistry = registry("8.0", {
  sales: DailySalesSchema,
  readings: HourlyReadingSchema,
});

describe("Fill Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .sales.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          date: new Date("2024-01-01"),
          bootsSold: 10,
          sandalsSold: null,
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          date: new Date("2024-01-02"),
          bootsSold: null,
          sandalsSold: 5,
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          date: new Date("2024-01-03"),
          bootsSold: 12,
          sandalsSold: 8,
        },
      ])
      .execute();

    await dbRegistry(db)
      .readings.insertMany([
        {
          _id: new ObjectId("000000000000000000000004"),
          timestamp: new Date("2024-01-01T00:00:00Z"),
          temperature: null,
          humidity: 50,
          sensorId: "A",
        },
        {
          _id: new ObjectId("000000000000000000000005"),
          timestamp: new Date("2024-01-01T01:00:00Z"),
          temperature: 20,
          humidity: null,
          sensorId: "A",
        },
        {
          _id: new ObjectId("000000000000000000000006"),
          timestamp: new Date("2024-01-01T02:00:00Z"),
          temperature: 22,
          humidity: 55,
          sensorId: "A",
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should fill with last observation carried forward", async () => {
    const ResultSchema = DailySalesSchema;

    const results = await dbRegistry(db)
      .sales.aggregate(
        $fill({
          sortBy: {
            _id: 1,
            date: 1,
            bootsSold: 1,
            sandalsSold: 1,
          },
          output: { bootsSold: { method: "locf" } },
        }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should fill with constant value", async () => {
    const ResultSchema = HourlyReadingSchema;

    const results = await dbRegistry(db)
      .readings.aggregate(
        $fill({
          sortBy: {
            _id: 1,
            timestamp: 1,
            temperature: 1,
            humidity: 1,
            sensorId: 1,
          },
          output: {
            temperature: { value: 0 },
            humidity: { value: 50 },
          },
        }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should fill with linear interpolation", async () => {
    const ResultSchema = HourlyReadingSchema;

    const results = await dbRegistry(db)
      .readings.aggregate(
        $fill({
          sortBy: { timestamp: 1 },
          output: {
            temperature: { method: "linear" },
            humidity: { method: "linear" },
          },
        }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });
});
