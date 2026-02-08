import { Schema as S } from "@effect/schema";
import { $setWindowFields, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const CakeSaleSchema = S.Struct({
  _id: ObjectIdSchema,
  type: S.String,
  orderDate: S.Date,
  state: S.String,
  price: S.Number,
  quantity: S.Number,
});

const dbRegistry = registry("8.0", { cakeSales: CakeSaleSchema });

describe("SetWindowFields Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .cakeSales.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          type: "chocolate",
          orderDate: new Date("2024-01-01"),
          state: "CA",
          price: 13,
          quantity: 120,
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          type: "vanilla",
          orderDate: new Date("2024-01-02"),
          state: "CA",
          price: 12,
          quantity: 145,
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          type: "chocolate",
          orderDate: new Date("2024-01-03"),
          state: "NY",
          price: 13,
          quantity: 100,
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should compute cumulative quantity for each state", async () => {
    const ResultSchema = S.Struct({
      ...CakeSaleSchema.fields,
      cumulativeQuantityForState: S.Number,
    });

    const results = await dbRegistry(db)
      .cakeSales.aggregate(
        $setWindowFields($ => ({
          partitionBy: "$state",
          sortBy: { orderDate: 1 },
          output: {
            cumulativeQuantityForState: {
              $sum: "$quantity",
              window: { documents: ["unbounded", "current"] },
            },
          },
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should compute moving average quantity", async () => {
    const ResultSchema = S.Struct({
      ...CakeSaleSchema.fields,
      averageQuantity: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .cakeSales.aggregate(
        $setWindowFields($ => ({
          partitionBy: { $year: "$orderDate" },
          sortBy: { orderDate: 1 },
          output: {
            averageQuantity: {
              $avg: "$quantity",
              window: { documents: [-1, 0] },
            },
          },
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should compare with previous values using $shift", async () => {
    const ResultSchema = S.Struct({
      ...CakeSaleSchema.fields,
      previousPrice: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .cakeSales.aggregate(
        $setWindowFields($ => ({
          partitionBy: "$type",
          sortBy: { orderDate: 1 },
          output: {
            previousPrice: {
              $shift: {
                output: "$price",
                by: -1,
              },
            },
          },
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
