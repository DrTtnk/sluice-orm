import { Schema as S } from "@effect/schema";
import { Db } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $group, $sort } from "../../../src/sluice.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";
import { monster, seedMonsters } from "./setup.js";

describe("Hardcore: $group by complex object", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    await seedMonsters(db);
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should group by complex object with multiple fields", async () => {
    // 1. Define Result Schema
    const ItemSchema = S.Struct({
      _id: S.Struct({
        status: S.String,
        level: S.Number,
        year: S.Number,
      }),
      avgScore: S.NullOr(S.Number),
    });
    const ResultSchema = S.Array(ItemSchema);

    // 2. Test data already seeded

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("monsters")
      .aggregate([
        {
          $group: {
            _id: {
              status: "$status",
              level: "$level",
              year: { $year: "$createdAt" },
            },
            avgScore: { $avg: "$score" },
          },
        },
        {
          $sort: {
            "_id.status": 1,
            "_id.level": 1,
            "_id.year": 1,
          },
        },
      ])
      .toArray();

    // 5. Execute Sluice Query
    const sluiceResults = await monster(db)
      .monsters.aggregate(
        $group($ => ({
          _id: {
            status: "$status",
            level: "$level",
            year: $.year("$createdAt"),
          },
          avgScore: $.avg("$score"),
        })),
        $sort({
          "_id.status": 1,
          "_id.level": 1,
          "_id.year": 1,
        }),
      )
      .toList();

    // 6. Validate Type
    assertSync(ResultSchema, sluiceResults);
    expect(sluiceResults.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as typeof sluiceResults);

    // 7. Validate Equivalence
    expect(sluiceResults).toEqual(rawResults);
  });
});
