/**
 * Tests triple-nested $reduce with $filter and $map
 * Tests: $$value shadowing, nested array traversal, type inference challenges
 */
import { Schema as S } from "@effect/schema";
import { Db, ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $project, $sort, registry } from "../../../src/sluice.js";
import { ObjectIdSchema } from "../../utils/common-schemas.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";

describe("Hardcore: Triple-nested $reduce with $$value shadowing", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should handle triple-nested $reduce with $filter and $map", async () => {
    // 1. Define Result Schema
    const MatrixUserSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      matrixMetadata: S.Array(S.Array(S.Array(S.Struct({ rows: S.Number })))),
    });

    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      totalEvenRows: S.Number,
    });

    const matrixUsers = registry("8.0", { matrixUsers: MatrixUserSchema });

    // 2. Insert Test Data
    const user1Id = new ObjectId();
    const user2Id = new ObjectId();

    await matrixUsers(db)
      .matrixUsers.insertMany([
        {
          _id: user1Id,
          name: "Alice",
          matrixMetadata: [
            [[{ rows: 10 }, { rows: 15 }], [{ rows: 20 }]],
            [[{ rows: 3 }], [{ rows: 8 }, { rows: 7 }]],
          ],
        },
        {
          _id: user2Id,
          name: "Bob",
          matrixMetadata: [[[{ rows: 4 }, { rows: 5 }, { rows: 6 }]]],
        },
      ])
      .execute();

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("matrixUsers")
      .aggregate([
        {
          $project: {
            name: 1,
            totalEvenRows: {
              $reduce: {
                input: "$matrixMetadata",
                initialValue: 0,
                in: {
                  $add: [
                    "$$value",
                    {
                      $reduce: {
                        input: "$$this",
                        initialValue: 0,
                        in: {
                          $add: [
                            "$$value",
                            {
                              $sum: {
                                $map: {
                                  input: {
                                    $filter: {
                                      input: "$$this",
                                      as: "rowObj",
                                      cond: { $eq: [{ $mod: ["$$rowObj.rows", 2] }, 0] },
                                    },
                                  },
                                  as: "filteredRow",
                                  in: "$$filteredRow.rows",
                                },
                              },
                            },
                          ],
                        },
                      },
                    },
                  ],
                },
              },
            },
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
        $project($ => ({
          name: 1,
          totalEvenRows: $.reduce({
            input: "$matrixMetadata",
            initialValue: 0,
            in: $ =>
              $.add(
                "$$value",
                $.reduce({
                  input: "$$this",
                  initialValue: 0,
                  in: $ =>
                    $.add(
                      "$$value",
                      $.sum(
                        $.map({
                          input: $.filter({
                            input: "$$this",
                            as: "rowObj",
                            cond: $ => $.eq($.mod("$$rowObj.rows", 2), 0), // User note: we have to enforce the use of $ here, like we did with map and reduce
                          }),
                          as: "filteredRow",
                          in: "$$filteredRow.rows",
                        }),
                      ),
                    ),
                }),
              ),
          }),
        })),
        $sort({ _id: 1 }),
      )
      .toList();

    // 6. Validate Type
    // Note: _id is implicitly included by MongoDB but not tracked in sluice type after $project
    // The runtime result has _id, but the type only shows explicitly projected fields
    assertSync(S.Array(ResultSchema), sluiceResults);

    // 7. Validate Equivalence
    expect(sluiceResults).toEqual(rawResults);
  });
});
