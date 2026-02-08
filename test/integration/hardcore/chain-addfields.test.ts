import { Schema as S } from "@effect/schema";
import { Db } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $addFields, $match, $sort } from "../../../src/sluice.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";
import { monster, MonsterSchema, seedMonsters } from "./setup.js";

describe("Hardcore: Chain multiple $addFields stages", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    await seedMonsters(db);
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should chain multiple $addFields stages", async () => {
    // 1. Define Result Schema
    const ItemSchema = MonsterSchema.pipe(
      S.extend(
        S.Struct({
          itemCount: S.Number,
          viewScore: S.Number,
          scorePerItem: S.Number,
          engagement: S.Number,
        }),
      ),
    );
    const ResultSchema = S.Array(ItemSchema);

    // 2. Test data already seeded

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("monsters")
      .aggregate([
        { $match: { active: true } },
        {
          $addFields: {
            itemCount: { $size: "$items" },
            viewScore: "$metadata.counts.views",
          },
        },
        {
          $addFields: {
            scorePerItem: { $divide: ["$score", "$itemCount"] },
            engagement: { $add: ["$viewScore", "$metadata.counts.likes"] },
          },
        },
        { $sort: { _id: 1 } },
      ])
      .toArray();

    // 4. Validate Raw Query
    expect(rawResults.length).toBeGreaterThan(0);

    // 5. Execute Sluice Query
    const sluiceResults = await monster(db)
      .monsters.aggregate(
        $match($ => ({ active: true })),
        $addFields($ => ({
          itemCount: $.size("$items"),
          viewScore: "$metadata.counts.views",
        })),
        $addFields($ => ({
          scorePerItem: $.divide("$score", "$itemCount"),
          engagement: $.add("$viewScore", "$metadata.counts.likes"),
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
