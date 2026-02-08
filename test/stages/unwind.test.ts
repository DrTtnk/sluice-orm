// Runtime equivalent of unwind.test-d.ts
import { Schema as S } from "@effect/schema";
import { $unwind, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const OrderSchema = S.Struct({
  _id: ObjectIdSchema,
  customerId: S.String,
  items: S.Array(
    S.Struct({
      productId: S.String,
      quantity: S.Number,
      price: S.Number,
    }),
  ),
  status: S.String,
});

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  tags: S.Array(S.String),
  scores: S.Array(S.Number),
  addresses: S.Array(
    S.Struct({
      city: S.String,
      zip: S.Number,
      roads: S.Array(S.String),
    }),
  ),
});

type Order = typeof OrderSchema.Type;
type User = typeof UserSchema.Type;

const dbRegistry = registry("8.0", {
  users: UserSchema,
  orders: OrderSchema,
});

describe("Unwind Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .users.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          name: "Alice",
          tags: ["mongodb", "database"],
          scores: [85, 90],
          addresses: [
            {
              city: "NYC",
              zip: 10001,
              roads: ["5th Ave", "Broadway"],
            },
            {
              city: "LA",
              zip: 90001,
              roads: ["Sunset Blvd"],
            },
          ],
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          name: "Bob",
          tags: ["nosql"],
          scores: [95],
          addresses: [
            {
              city: "SF",
              zip: 94102,
              roads: ["Market St"],
            },
          ],
        },
      ])
      .execute();

    await dbRegistry(db)
      .orders.insertMany([
        {
          _id: new ObjectId("000000000000000000000003"),
          customerId: "c1",
          items: [
            {
              productId: "p1",
              quantity: 2,
              price: 10,
            },
            {
              productId: "p2",
              quantity: 1,
              price: 20,
            },
          ],
          status: "shipped",
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should unwind simple string array", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      tags: S.String,
      scores: S.Array(S.Number),
      addresses: S.Array(
        S.Struct({
          city: S.String,
          zip: S.Number,
          roads: S.Array(S.String),
        }),
      ),
    });

    // 2 - Perform aggregation
    const results = await dbRegistry(db).users.aggregate($unwind("$tags")).toList();

    // 3 - Runtime validation & type assertion
    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should unwind number array", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      tags: S.Array(S.String),
      scores: S.Number,
      addresses: S.Array(
        S.Struct({
          city: S.String,
          zip: S.Number,
          roads: S.Array(S.String),
        }),
      ),
    });

    // 2 - Perform aggregation
    const results = await dbRegistry(db).users.aggregate($unwind("$scores")).toList();

    // 3 - Runtime validation & type assertion
    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should unwind object array", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      tags: S.Array(S.String),
      scores: S.Array(S.Number),
      addresses: S.Struct({
        city: S.String,
        zip: S.Number,
        roads: S.Array(S.String),
      }),
    });

    // 2 - Perform aggregation
    const results = await dbRegistry(db).users.aggregate($unwind("$addresses")).toList();

    // 3 - Runtime validation & type assertion
    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should unwind nested array items", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      customerId: S.String,
      items: S.Struct({
        productId: S.String,
        quantity: S.Number,
        price: S.Number,
      }),
      status: S.String,
    });

    // 2 - Perform aggregation
    const results = await dbRegistry(db).orders.aggregate($unwind("$items")).toList();

    // 3 - Runtime validation & type assertion
    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should unwind with includeArrayIndex", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      tags: S.String,
      scores: S.Array(S.Number),
      addresses: S.Array(
        S.Struct({
          city: S.String,
          zip: S.Number,
          roads: S.Array(S.String),
        }),
      ),
      tagIndex: S.Number,
    });

    // 2 - Perform aggregation
    const results = await dbRegistry(db)
      .users.aggregate(
        $unwind({
          path: "$tags",
          includeArrayIndex: "tagIndex",
        }),
      )
      .toList();

    // 3 - Runtime validation & type assertion
    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should unwind multiple arrays in sequence", async () => {
    // 1 - Expected result schema
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      tags: S.String,
      scores: S.Array(S.Number),
      addresses: S.Struct({
        city: S.String,
        zip: S.Number,
        roads: S.Array(S.String),
      }),
    });

    // 2 - Perform aggregation
    const results = await dbRegistry(db)
      .users.aggregate($unwind("$tags"), $unwind("$addresses"))
      .toList();

    // 3 - Runtime validation & type assertion
    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
