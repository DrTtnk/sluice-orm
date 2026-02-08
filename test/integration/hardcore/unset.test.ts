import { Schema as S } from "@effect/schema";
import { Db } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $sort, $unset } from "../../../src/sluice.js";
import { ObjectIdSchema } from "../../utils/common-schemas.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";
import { monster, seedMonsters } from "./setup.js";

describe("Hardcore: $unset to remove fields", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    await seedMonsters(db);
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should use $unset to remove fields", async () => {
    // 1. Define Result Schema
    const ItemSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      score: S.Number,
      active: S.Boolean,
      createdAt: S.Date,
      deletedAt: S.NullOr(S.Date),
      legacyScore: S.NullOr(S.Number),
      description: S.optional(S.String),
      priority: S.optional(S.Number),
      status: S.Literal("draft", "published", "archived"),
      level: S.Literal(1, 2, 3, 4, 5),
      tags: S.Array(S.String),
      scores: S.Array(S.Number),
      items: S.Array(
        S.Struct({
          id: S.String,
          name: S.String,
          price: S.Number,
          quantity: S.Number,
          discounts: S.Array(
            S.Struct({
              code: S.String,
              percent: S.Number,
              validUntil: S.Date,
            }),
          ),
        }),
      ),
      coords: S.Tuple(S.Number, S.Number),
    });
    const ResultSchema = S.Array(ItemSchema);

    // 2. Test data already seeded

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("monsters")
      .aggregate([{ $unset: "metadata" }, { $sort: { _id: 1 } }])
      .toArray();

    // 4. Validate Raw Query
    expect(rawResults.length).toBeGreaterThan(0);

    // 5. Execute Sluice Query
    const sluiceResults = await monster(db)
      .monsters.aggregate($unset("metadata"), $sort({ _id: 1 }))
      .toList();

    // 6. Validate Type
    expect(sluiceResults.length).toBeGreaterThan(0);
    assertSync(ResultSchema, sluiceResults);
    expectType<typeof ResultSchema.Type>({} as typeof sluiceResults);

    // 7. Validate Equivalence
    expect(sluiceResults).toEqual(rawResults);
  });
});
