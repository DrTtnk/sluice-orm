import { Schema as S } from "@effect/schema";
import { Db } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $facet, $group, $limit, $match, $project, $sort } from "../../../src/sluice.js";
import { ObjectIdSchema } from "../../utils/common-schemas.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";
import { monster, seedMonsters } from "./setup.js";

describe("Hardcore: $facet for multiple parallel pipelines", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    await seedMonsters(db);
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should use $facet for multiple parallel pipelines", async () => {
    // 1. Define Result Schema
    const ItemSchema = S.Struct({
      byStatus: S.Array(
        S.Struct({
          _id: S.String,
          count: S.Number,
        }),
      ),
      topScorers: S.Array(
        S.Struct({
          _id: ObjectIdSchema,
          name: S.String,
          score: S.Number,
        }),
      ),
      recent: S.Array(
        S.Struct({
          _id: ObjectIdSchema,
          name: S.String,
          createdAt: S.Date,
        }),
      ),
    });
    const ResultSchema = S.Array(ItemSchema);

    // 2. Test data already seeded

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("monsters")
      .aggregate([
        {
          $facet: {
            byStatus: [
              {
                $group: {
                  _id: "$status",
                  count: { $sum: 1 },
                },
              },
              { $sort: { _id: 1 } },
            ],
            topScorers: [
              { $match: { score: { $gt: 80 } } },
              {
                $project: {
                  _id: 1,
                  name: 1,
                  score: 1,
                },
              },
              { $limit: 5 },
              { $sort: { _id: 1 } },
            ],
            recent: [
              { $sort: { createdAt: -1 } },
              { $limit: 10 },
              {
                $project: {
                  _id: 1,
                  name: 1,
                  createdAt: 1,
                },
              },
            ],
          },
        },
      ])
      .toArray();

    // 4. Validate Raw Query
    expect(rawResults.length).toBeGreaterThan(0);

    // 5. Execute Sluice Query
    const sluiceResults = await monster(db)
      .monsters.aggregate(
        $facet($ => ({
          byStatus: $.pipe(
            $group($ => ({
              _id: "$status",
              count: $.sum(1),
            })),
            $sort({ _id: 1 }),
          ),
          topScorers: $.pipe(
            $match($ => ({ score: { $gt: 80 } })),
            $project($ => ({
              _id: 1,
              name: 1,
              score: 1,
            })),
            $limit(5),
            $sort({ _id: 1 }),
          ),
          recent: $.pipe(
            $sort({ createdAt: -1 }),
            $limit(10),
            $project($ => ({
              _id: 1,
              name: 1,
              createdAt: 1,
            })),
          ),
        })),
      )
      .toList();

    // 6. Validate Type
    expect(sluiceResults.length).toBeGreaterThan(0);
    assertSync(ResultSchema, sluiceResults);

    // 7. Validate Equivalence
    expect(sluiceResults).toEqual(rawResults);
  });
});
