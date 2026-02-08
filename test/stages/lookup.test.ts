// Runtime equivalent of lookup.test-d.ts
import { Schema as S } from "@effect/schema";
import { $lookup, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const OrderSchema = S.Struct({
  _id: ObjectIdSchema,
  item: S.String,
  ordered: S.Number,
  customerId: S.String,
});

const InventorySchema = S.Struct({
  _id: ObjectIdSchema,
  sku: S.String,
  description: S.String,
  instock: S.Number,
});

const CustomerSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  email: S.String,
});

type Order = typeof OrderSchema.Type;
type Inventory = typeof InventorySchema.Type;
type Customer = typeof CustomerSchema.Type;

const dbRegistry = registry("8.0", {
  orders: OrderSchema,
  inventory: InventorySchema,
  customers: CustomerSchema,
});

describe("Lookup Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .orders.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          item: "widget",
          ordered: 5,
          customerId: "c1",
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          item: "gadget",
          ordered: 3,
          customerId: "c2",
        },
      ])
      .execute();

    await dbRegistry(db)
      .inventory.insertMany([
        {
          _id: new ObjectId("000000000000000000000003"),
          sku: "widget",
          description: "Widget desc",
          instock: 10,
        },
        {
          _id: new ObjectId("000000000000000000000004"),
          sku: "gadget",
          description: "Gadget desc",
          instock: 5,
        },
      ])
      .execute();

    await dbRegistry(db)
      .customers.insertMany([
        {
          _id: new ObjectId("000000000000000000000005"),
          name: "Alice",
          email: "alice@example.com",
        },
        {
          _id: new ObjectId("000000000000000000000006"),
          name: "Bob",
          email: "bob@example.com",
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should lookup basic equality join", async () => {
    const ResultSchema = S.Struct({
      ...OrderSchema.fields,
      inventory_docs: S.Array(InventorySchema),
    });

    const results = await dbRegistry(db)
      .orders.aggregate(
        $lookup({
          from: dbRegistry(db).inventory,
          localField: "item",
          foreignField: "sku",
          as: "inventory_docs",
        }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should lookup customer data", async () => {
    const ResultSchema = S.Struct({
      ...OrderSchema.fields,
      customer: S.Array(CustomerSchema),
    });

    const results = await dbRegistry(db)
      .orders.aggregate(
        $lookup({
          from: dbRegistry(db).customers,
          localField: "customerId",
          foreignField: "_id",
          as: "customer",
        }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should chain multiple lookups", async () => {
    const ResultSchema = S.Struct({
      ...OrderSchema.fields,
      items: S.Array(InventorySchema),
      customer: S.Array(CustomerSchema),
    });

    const results = await dbRegistry(db)
      .orders.aggregate(
        $lookup({
          from: dbRegistry(db).inventory,
          localField: "item",
          foreignField: "sku",
          as: "items",
        }),
        $lookup({
          from: dbRegistry(db).customers,
          localField: "customerId",
          foreignField: "_id",
          as: "customer",
        }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
