import { Schema as S } from "@effect/schema";
import { Db } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $sortByCount } from "../../../src/sluice.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";
import { monster, seedMonsters } from "./setup.js";

describe("Hardcore: $sortByCount with union field", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    await seedMonsters(db);
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should use $sortByCount with union field", async () => {
    // 1. Define Result Schema
    const ItemSchema = S.Struct({
      _id: S.Array(S.Number),
      count: S.Number,
    });
    const ResultSchema = S.Array(ItemSchema);

    // 2. Test data already seeded

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("monsters")
      .aggregate([{ $sortByCount: "$items.quantity" }])
      .toArray();

    // 5. Execute Sluice Query
    const sluiceResults = await monster(db)
      .monsters.aggregate($sortByCount("$items.quantity"))
      .toList();

    // 6. Validate Type
    assertSync(ResultSchema, sluiceResults);
    expect(sluiceResults.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as typeof sluiceResults);

    // 7. Validate Equivalence
    expect(sluiceResults).toEqual(rawResults);
  });
});
