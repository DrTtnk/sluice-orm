import { Schema as S } from "@effect/schema";
import { Db } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $addFields } from "../../../src/sluice.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";
import { m1Id, monster, MonsterSchema, seedMonsters } from "./setup.js";

describe("Hardcore: Type coercion operators", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    await seedMonsters(db);
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should use type coercion operators", async () => {
    // 1. Define Result Schema
    const ResultSchema = MonsterSchema.pipe(
      S.extend(
        S.Struct({
          scoreAsInt: S.Number,
          scoreAsBool: S.Boolean,
        }),
      ),
    );

    // 2. Test data already seeded

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("monsters")
      .aggregate([
        {
          $addFields: {
            scoreAsInt: { $toInt: "$score" },
            scoreAsBool: { $toBool: "$score" },
          },
        },
      ])
      .toArray();

    // 4. Validate Raw Query
    expect(rawResults.length).toBeGreaterThan(0);

    // 5. Execute Sluice Query
    const sluiceResults = await monster(db)
      .monsters.aggregate(
        $addFields($ => ({
          scoreAsInt: $.toInt("$score"),
          scoreAsBool: $.toBool("$score"),
        })),
      )
      .toList();

    // 6. Validate Type
    expect(sluiceResults.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), sluiceResults);
    expectType<readonly (typeof ResultSchema.Type)[]>({} as typeof sluiceResults);

    // 7. Validate Equivalence
    expect(sluiceResults).toEqual(rawResults);
    const m1 = sluiceResults.find(m => m._id.toString() === m1Id.toString());
    expect(m1?.scoreAsInt).toBe(10);
    expect(m1?.scoreAsBool).toBe(true);
  });
});
