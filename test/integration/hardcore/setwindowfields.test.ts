/**
 * Tests complex $setWindowFields with obscure window functions
 * Tests: $locf, $derivative, $integral, $rank with dynamic partitioning
 */
import { Schema as S } from "@effect/schema";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  $project,
  $setWindowFields,
  $sort,
  registry,
} from "../../../src/sluice.js";
import { ObjectIdSchema } from "../../utils/common-schemas.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";

describe("Hardcore: Complex $setWindowFields with obscure window functions", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should handle complex $setWindowFields with obscure window functions", async () => {
    // 1. Define Result Schema
    const MatrixUserSchema = S.Struct({
      _id: ObjectIdSchema,
      createdAt: S.Date,
      name: S.String,
      email: S.String,
      age: S.Number,
      tags: S.Array(S.String),
      matrixMetadata: S.Array(S.Array(S.Array(S.Struct({ rows: S.Number })))),
      profile: S.Struct({
        bio: S.String,
        avatar: S.String,
      }),
      orders: S.Array(
        S.Struct({
          orderId: ObjectIdSchema,
          total: S.Number,
          items: S.Array(S.String),
        }),
      ),
    });

    const ItemSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      partition: S.String,
      subDomain: S.String,
      matrixGrowthVelocity: S.NullOr(S.Number),
      economicMomentum: S.Number,
      filledAvatar: S.String,
      itemDensityRank: S.Number,
      isHighValue: S.Boolean,
    });
    const ResultSchema = S.Array(ItemSchema);

    const matrixUsers = registry("8.0", { matrixUsers: MatrixUserSchema });

    // 2. Insert Test Data
    const user1Id = new ObjectId();
    const user2Id = new ObjectId();
    const user3Id = new ObjectId();

    await matrixUsers(db)
      .matrixUsers.insertMany([
        {
          _id: user1Id,
          createdAt: new Date("2023-01-01"),
          name: "Alice Smith",
          email: "alice@example.com",
          age: 25,
          tags: ["premium", "active"],
          matrixMetadata: [[[{ rows: 10 }]], [[{ rows: 15 }]]],
          profile: {
            bio: "Software engineer",
            avatar: "avatar1.png",
          },
          orders: [
            {
              orderId: new ObjectId(),
              total: 150,
              items: ["item1", "item2", "item3"],
            },
            {
              orderId: new ObjectId(),
              total: 250,
              items: ["item4", "item5"],
            },
          ],
        },
        {
          _id: user2Id,
          createdAt: new Date("2023-01-15"),
          name: "Bob Jones",
          email: "bob@gmail.com",
          age: 35,
          tags: ["regular"],
          matrixMetadata: [[[{ rows: 20 }]], [[{ rows: 25 }]]],
          profile: {
            bio: "Data analyst",
            avatar: "avatar2.png",
          },
          orders: [
            {
              orderId: new ObjectId(),
              total: 300,
              items: ["item6", "item7", "item8", "item9"],
            },
          ],
        },
        {
          _id: user3Id,
          createdAt: new Date("2023-02-01"),
          name: "Carol Wilson",
          email: "carol@yahoo.com",
          age: 75,
          tags: ["senior", "loyal"],
          matrixMetadata: [[[{ rows: 30 }]], [[{ rows: 35 }]]],
          profile: {
            bio: "Retired teacher",
            avatar: "avatar3.png",
          },
          orders: [
            {
              orderId: new ObjectId(),
              total: 100,
              items: ["item10"],
            },
          ],
        },
      ])
      .execute();

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("matrixUsers")
      .aggregate([
        {
          $setWindowFields: {
            partitionBy: {
              $concat: [
                { $substr: ["$email", 0, { $indexOfBytes: ["$email", "@"] }] },
                "-",
                {
                  $switch: {
                    branches: [
                      {
                        case: { $lt: ["$age", 18] },
                        then: "minor",
                      },
                      {
                        case: { $lt: ["$age", 65] },
                        then: "adult",
                      },
                    ],
                    default: "senior",
                  },
                },
              ],
            },
            sortBy: { createdAt: 1 },
            output: {
              filledAvatar: { $locf: "$profile.avatar" },
              matrixGrowthVelocity: {
                $derivative: {
                  input: {
                    $getField: {
                      field: "rows",
                      input: {
                        $arrayElemAt: [
                          { $arrayElemAt: [{ $arrayElemAt: ["$matrixMetadata", 0] }, 0] },
                          0,
                        ],
                      },
                    },
                  },
                  unit: "week",
                },
                window: { documents: [-1, "current"] },
              },
              economicMomentum: {
                $integral: {
                  input: { $avg: "$orders.total" },
                  unit: "day",
                },
                window: {
                  range: [-30, "current"],
                  unit: "day",
                },
              },
              itemDensityRank: { $rank: {} },
            },
          },
        },
        {
          $project: {
            name: 1,
            partition: {
              $concat: [
                { $substr: ["$email", 0, { $indexOfBytes: ["$email", "@"] }] },
                "-",
                {
                  $switch: {
                    branches: [
                      {
                        case: { $lt: ["$age", 18] },
                        then: "minor",
                      },
                      {
                        case: { $lt: ["$age", 65] },
                        then: "adult",
                      },
                    ],
                    default: "senior",
                  },
                },
              ],
            },
            subDomain: {
              $substr: ["$email", { $add: [{ $indexOfBytes: ["$email", "@"] }, 1] }, -1],
            },
            matrixGrowthVelocity: 1,
            economicMomentum: 1,
            filledAvatar: 1,
            itemDensityRank: 1,
            isHighValue: { $gt: ["$economicMomentum", 1000] },
          },
        },
        { $sort: { _id: 1 } },
      ])
      .toArray();

    // 4. Validate Raw Query
    expect(rawResults.length).toBeGreaterThan(0);

    // 5. Execute Sluice Query
    const sluiceResults = await matrixUsers(db)
      .matrixUsers.aggregate(
        $setWindowFields($ => ({
          partitionBy: $.concat(
            $.substr("$email", 0, $.indexOfBytes("$email", "@")),
            "-",
            $.switch({
              branches: [
                {
                  case: $.lt("$age", 18),
                  then: "minor",
                },
                {
                  case: $.lt("$age", 65),
                  then: "adult",
                },
              ],
              default: "senior",
            }),
          ),
          sortBy: { createdAt: 1 },
          output: {
            filledAvatar: $.locf("$profile.avatar"),
            matrixGrowthVelocity: $.derivative({
              input: $.getField({
                field: "rows",
                input: $.arrayElemAt($.arrayElemAt($.arrayElemAt("$matrixMetadata", 0), 0), 0),
              }),
              unit: "week",
              window: { documents: [-1, "current"] },
            }),
            economicMomentum: $.integral({
              input: $.avg("$orders.total"),
              unit: "day",
              window: {
                range: [-30, "current"],
                unit: "day",
              },
            }),
            itemDensityRank: $.rank(),
          },
        })),
        $project($ => ({
          name: 1,
          partition: $.concat(
            $.substr("$email", 0, $.indexOfBytes("$email", "@")),
            "-",
            $.switch({
              branches: [
                {
                  case: $.lt("$age", 18),
                  then: "minor",
                },
                {
                  case: $.lt("$age", 65),
                  then: "adult",
                },
              ],
              default: "senior",
            }),
          ),
          subDomain: $.substr("$email", $.add($.indexOfBytes("$email", "@"), 1), -1),
          matrixGrowthVelocity: 1,
          economicMomentum: 1,
          filledAvatar: 1,
          itemDensityRank: 1,
          isHighValue: $.gt("$economicMomentum", 1000),
        })),
        $sort({ _id: 1 }),
      )
      .toList();

    // 6. Validate Type
    expect(sluiceResults.length).toBeGreaterThan(0);
    assertSync(ResultSchema, sluiceResults);
    expectType<typeof ResultSchema.Type>({} as typeof sluiceResults);

    // 7. Validate Equivalence
    expect(sluiceResults).toEqual(rawResults);
  });
});
