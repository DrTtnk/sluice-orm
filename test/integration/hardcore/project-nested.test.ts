import { Schema as S } from "@effect/schema";
import { Db } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $project, $sort } from "../../../src/sluice.js";
import { ObjectIdSchema } from "../../utils/common-schemas.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";
import { monster, seedMonsters } from "./setup.js";

describe("Hardcore: $project nested fields and array operators", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    await seedMonsters(db);
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should project nested fields and use array operators", async () => {
    // 1. Define Result Schema
    const ItemSchema = S.Struct({
      _id: ObjectIdSchema,
      version: S.String,
      viewCount: S.Number,
      likeCount: S.Number,
      firstTag: S.String,
      itemNames: S.Array(S.String),
    });
    const ResultSchema = S.Array(ItemSchema);

    // 2. Test data already seeded

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("monsters")
      .aggregate([
        {
          $project: {
            _id: 1,
            version: "$metadata.version",
            viewCount: "$metadata.counts.views",
            likeCount: "$metadata.counts.likes",
            firstTag: { $arrayElemAt: ["$tags", 0] },
            itemNames: {
              $map: {
                input: "$items",
                in: "$$this.name",
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
        $project($ => ({
          _id: 1,
          version: "$metadata.version",
          viewCount: "$metadata.counts.views",
          likeCount: "$metadata.counts.likes",
          firstTag: $.arrayElemAt("$tags", 0),
          itemNames: $.map({
            input: "$items",
            in: "$$this.name",
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
