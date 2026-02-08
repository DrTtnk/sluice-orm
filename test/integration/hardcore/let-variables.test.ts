import { Schema as S } from "@effect/schema";
import { Db } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $addFields, $sort } from "../../../src/sluice.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";
import { monster, MonsterSchema, seedMonsters } from "./setup.js";

describe("Hardcore: $let for local variables", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    await seedMonsters(db);
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should use $let for local variables in expressions", async () => {
    // 1. Define Result Schema
    const ItemSchema = MonsterSchema.pipe(S.extend(S.Struct({ itemValue: S.Number })));
    const ResultSchema = S.Array(ItemSchema);

    // 2. Test data already seeded

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("monsters")
      .aggregate([
        {
          $addFields: {
            itemValue: {
              $let: {
                vars: {
                  totalItems: { $size: "$items" },
                  totalScore: "$score",
                },
                in: { $multiply: ["$$totalItems", "$$totalScore"] },
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
    const sluiceResults = await monster(db)
      .monsters.aggregate(
        $addFields($ => ({
          itemValue: $.let({
            vars: {
              totalItems: $.size("$items"),
              totalScore: "$score",
            },
            in: $ => $.multiply("$$totalItems", "$$totalScore"),
          }),
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
