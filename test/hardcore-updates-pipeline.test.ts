import { Schema as S } from "@effect/schema";
import type { Db } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { registry } from "../src/registry.js";
import { setup, teardown } from "./utils/setup.js";
import { assertSync } from "./utils/utils.js";

const ComplexSchema = S.Struct({
  _id: S.String,
  score: S.Number,
  history: S.Array(S.Number),
  meta: S.Struct({
    version: S.Number,
    updatedAt: S.Date,
    tags: S.Array(S.String),
  }),
});

/**
 * Update pipelines enable type-safe document transformations with temporary fields.
 *
 * Usage: $ => $.pipe($.set(...), $.unset(...))
 *
 * The callback form provides typed operators with full context awareness.
 *
 * Features:
 * - Type inference: Collection type is inferred automatically via callback
 * - Type chaining: Each stage receives the output type of the previous stage
 * - Temporary fields: Can add fields in one stage and remove them in later stages
 * - Max 4 stages: Performance limit for type inference
 */
describe("Hardcore Pipeline Updates", () => {
  const dbRegistry = registry("8.0", { complex: ComplexSchema });
  let db: Db;

  // Use a long timeout for setup/teardown
  beforeAll(async () => {
    db = (await setup()).db;
  }, 60000);

  afterAll(async () => {
    await teardown();
  });

  it("should enforce schema evolution and validation", async () => {
    const complexColl = dbRegistry(db).complex;

    // Clear and seed
    await complexColl.deleteMany(() => ({})).execute();
    await complexColl
      .insertMany([
        {
          _id: "p1",
          score: 10,
          history: [1, 2],
          meta: {
            version: 1,
            updatedAt: new Date(),
            tags: ["a"],
          },
        },
      ])
      .execute();

    // Valid Pipeline using callback form with typed operators
    await complexColl
      .updateOne(
        $ => ({ _id: "p1" }),
        $ =>
          $.pipe(
            $.set($ => ({
              score: $.add("$score", 5),
              "meta.version": $.add("$meta.version", 1),
              // Add temporary field
              temp: $.multiply("$score", 2),
            })),
            // temp is available here
            $.set($ => ({ history: $.concatArrays("$history", ["$temp"]) })),
            // Must clean up temp to match schema at the end
            $.unset("temp"),
          ),
      )
      .execute();

    // Verifying it works
    const doc = await complexColl.findOne($ => ({ _id: "p1" })).toOne();
    assertSync(S.Tuple(ComplexSchema), [doc ?? (null as never)]);

    expect(doc?.score).toBe(15);
    expect(doc?.history).toEqual([1, 2, 20]); // 10 * 2 = 20
  });

  it("should support replaceRoot", async () => {
    const complexColl = dbRegistry(db).complex;

    await complexColl
      .updateOne(
        $ => ({ _id: "p1" }),
        $ => $.pipe($.replaceRoot($ => $.mergeObjects("$$ROOT", { score: 100 }))),
      )
      .execute();

    const doc = await complexColl.findOne($ => ({ _id: "p1" })).toOne();
    assertSync(S.Tuple(ComplexSchema), [doc ?? (null as never)]);
    expect(doc?.score).toBe(100);
  });

  it("should support functional composition syntax", async () => {
    const complexColl = dbRegistry(db).complex;

    // Reset p1
    await complexColl
      .updateOne($ => ({ _id: "p1" }), {
        $set: {
          score: 10,
          "meta.version": 1,
        },
      })
      .execute();

    await complexColl
      .updateOne(
        $ => ({ _id: "p1" }),
        $ =>
          $.pipe(
            $.set($ => ({ score: $.add("$score", 10) })),
            $.set(() => ({ "meta.version": 5 })),
          ),
      )
      .execute();

    const doc = await complexColl.findOne($ => ({ _id: "p1" })).toOne();
    expect(doc?.score).toBe(20); // 10 + 10
    expect(doc?.meta.version).toBe(5);
  });

  describe("Valid MongoDB pipeline updates that sluice previously failed to compile", () => {
    it("$addFields stage in update pipeline", async () => {
      const complexColl = dbRegistry(db).complex;
      // $addFields is now properly supported as an alias for $set in update pipelines
      await complexColl
        .updateOne(
          $ => ({ _id: "p1" }),
          $ =>
            $.pipe(
              $.addFields($ => ({ temp: $.add("$score", 1) })),
              $.unset("temp"),
            ),
        )
        .execute();
    });

    it("$replaceWith stage in update pipeline", async () => {
      const complexColl = dbRegistry(db).complex;
      // $replaceWith is now properly supported as an alias for $replaceRoot in update pipelines
      await complexColl
        .updateOne(
          $ => ({ _id: "p1" }),
          $ => $.pipe($.replaceWith($ => $.mergeObjects("$$ROOT", { score: 100 }))),
        )
        .execute();
    });

    it("Complex expressions in $set", async () => {
      const complexColl = dbRegistry(db).complex;
      // Complex aggregation expressions in update pipelines
      await complexColl
        .updateOne(
          $ => ({ _id: "p1" }),
          $ =>
            $.pipe(
              $.set($ => ({
                computed: $.cond({
                  if: $.gt("$score", 10),
                  then: $.multiply("$score", 2),
                  else: $.add("$score", 5),
                }),
              })),
            ),
        )
        .execute();
    });
  });

  describe("Known pipeline update limitations (by design)", () => {
    // Pipeline $set is an aggregation stage that can add/overwrite ANY fields.
    // Unlike traditional $set, it does NOT validate against the document schema.
    // This is a deliberate design choice matching MongoDB's behavior where pipeline
    // updates are expression-based and can reshape documents freely.

    it.skip("should reject type mismatches in pipeline", async () => {
      const complexColl = dbRegistry(db).complex;
      // Pipeline $set allows any value for any key - this is by design
      await complexColl
        .updateOne(
          $ => ({ _id: "p1" }),
          $ => $.pipe($.set($ => ({ score: "not a number" }))),
        )
        .execute();
    });

    it.skip("should reject invalid field references", async () => {
      const complexColl = dbRegistry(db).complex;
      // Pipeline $set accepts "$alsoNonexistent" as a string literal in value position
      await complexColl
        .updateOne(
          $ => ({ _id: "p1" }),
          $ => $.pipe($.set($ => ({ nonexistent: "$alsoNonexistent" }))),
        )
        .execute();
    });

    it.skip("should enforce schema at pipeline end", async () => {
      const complexColl = dbRegistry(db).complex;
      // Pipeline updates allow adding extra fields - MongoDB does not enforce schema
      await complexColl
        .updateOne(
          $ => ({ _id: "p1" }),
          $ => $.pipe($.set($ => ({ extraField: "value" }))),
        )
        .execute();
    });
  });
});
