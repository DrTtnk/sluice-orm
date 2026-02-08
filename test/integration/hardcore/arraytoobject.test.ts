/**
 * Tests $arrayToObject with dynamic key generation and $replaceRoot
 * Tests: Dynamic property names from array data, schema challenges with Unknown types
 */
import { Schema as S } from "@effect/schema";
import { Db, ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $match, $replaceRoot, $sort, registry } from "../../../src/sluice.js";
import { ObjectIdSchema } from "../../utils/common-schemas.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";

describe("Hardcore: $arrayToObject with dynamic key generation", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should handle $arrayToObject with dynamic key generation and $replaceRoot", async () => {
    // 1. Define Result Schema (dynamic keys are tricky!)
    const Tags = S.Literal("admin", "developer", "ts", "js", "mongo");

    // ObjectId string brand - validates 24-character hex strings
    const ObjectIdString = S.String.pipe(S.pattern(/^[0-9a-f]{24}$/i), S.brand("ObjectIdString"));

    const UserWithOrdersSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      email: S.String,
      tags: S.Array(Tags),
      orders: S.Array(
        S.Struct({
          orderId: ObjectIdSchema,
          total: S.Number,
        }),
      ),
    });

    const TagFlagsSchema = S.Record({
      key: S.TemplateLiteral(S.Literal("has_tag_"), Tags),
      value: S.Literal(true),
    }).pipe(S.partial);

    const TotalsSchema = S.Record({
      key: ObjectIdString,
      value: S.Number,
    });

    const ResultSchema = S.Struct({ _id: ObjectIdSchema }).pipe(
      S.extend(TagFlagsSchema),
      S.extend(TotalsSchema),
    );

    const usersWithOrders = registry("8.0", { usersWithOrders: UserWithOrdersSchema });

    // 2. Insert Test Data
    const user1Id = new ObjectId();
    const user2Id = new ObjectId();
    const order1Id = new ObjectId();
    const order2Id = new ObjectId();
    const order3Id = new ObjectId();

    await usersWithOrders(db)
      .usersWithOrders.insertMany([
        {
          _id: user1Id,
          name: "Alice",
          email: "alice@example.com",
          tags: ["admin", "developer", "ts"],
          orders: [
            {
              orderId: order1Id,
              total: 100,
            },
            {
              orderId: order2Id,
              total: 200,
            },
          ],
        },
        {
          _id: user2Id,
          name: "Bob",
          email: "bob@example.com",
          tags: ["admin", "js", "mongo"],
          orders: [
            {
              orderId: order3Id,
              total: 300,
            },
          ],
        },
      ])
      .execute();

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("usersWithOrders")
      .aggregate([
        {
          $replaceRoot: {
            newRoot: {
              $mergeObjects: [
                { _id: "$_id" },
                {
                  $arrayToObject: {
                    $map: {
                      input: "$tags",
                      as: "tag",
                      in: {
                        k: { $concat: ["has_tag_", "$$tag"] },
                        v: true,
                      },
                    },
                  },
                },
                {
                  $arrayToObject: {
                    $map: {
                      input: "$orders",
                      as: "order",
                      in: {
                        k: { $toString: "$$order.orderId" },
                        v: "$$order.total",
                      },
                    },
                  },
                },
              ],
            },
          },
        },
        { $match: { has_tag_admin: true } },
        { $sort: { _id: 1 } },
      ])
      .toArray();

    // 4. Validate Raw Query
    expect(rawResults.length).toBeGreaterThan(0);

    // 5. Execute Sluice Query
    const sluiceResults = await usersWithOrders(db)
      .usersWithOrders.aggregate(
        $replaceRoot($ => ({
          newRoot: $.mergeObjects(
            { _id: "$_id" },
            $.arrayToObject(
              $.map({
                input: "$tags",
                as: "tag",
                in: $ => ({
                  k: $.concat("has_tag_", "$$tag"),
                  v: true,
                }),
              }),
            ),
            $.arrayToObject(
              $.map({
                input: "$orders",
                as: "order",
                in: $ => ({
                  k: $.toString("$$order.orderId"),
                  v: "$$order.total",
                }),
              }),
            ),
          ),
        })),
        $match($ => ({ has_tag_admin: true })),
        $sort({ _id: 1 }),
      )
      .toList();

    // 6. Validate Type
    assertSync(S.Array(ResultSchema), sluiceResults);
    // 7. Validate Equivalence
    expect(sluiceResults).toEqual(rawResults);
  });
});
