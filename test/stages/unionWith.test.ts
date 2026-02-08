// Runtime equivalent of unionWith.test-d.ts
import { Schema as S } from "@effect/schema";
import { $unionWith, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const SaleSchema = S.Struct({
  _id: ObjectIdSchema,
  product: S.String,
  amount: S.Number,
  date: S.Date,
});

const ArchivedSaleSchema = S.Struct({
  _id: ObjectIdSchema,
  product: S.String,
  amount: S.Number,
  archived: S.Literal(true),
});

type Sale = typeof SaleSchema.Type;
type ArchivedSale = typeof ArchivedSaleSchema.Type;

const dbRegistry = registry("8.0", {
  sales: SaleSchema,
  moreSales: SaleSchema,
  archivedSales: ArchivedSaleSchema,
});

describe("UnionWith Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .sales.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          product: "Widget",
          amount: 100,
          date: new Date("2024-01-01"),
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          product: "Gadget",
          amount: 200,
          date: new Date("2024-01-02"),
        },
      ])
      .execute();

    await dbRegistry(db)
      .moreSales.insertMany([
        {
          _id: new ObjectId("000000000000000000000003"),
          product: "Doohickey",
          amount: 150,
          date: new Date("2024-01-03"),
        },
      ])
      .execute();

    await dbRegistry(db)
      .archivedSales.insertMany([
        {
          _id: new ObjectId("000000000000000000000004"),
          product: "OldWidget",
          amount: 50,
          archived: true,
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should union with same type collection", async () => {
    const ResultSchema = SaleSchema;

    const results = await dbRegistry(db)
      .sales.aggregate($unionWith({ coll: dbRegistry(db).moreSales }))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should union with different but compatible type", async () => {
    const ResultSchema = S.Union(SaleSchema, ArchivedSaleSchema);

    const results = await dbRegistry(db)
      .sales.aggregate($unionWith({ coll: dbRegistry(db).archivedSales }))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
