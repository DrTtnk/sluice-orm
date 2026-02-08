/**
 * Tests $accumulator with custom JavaScript functions
 * Tests: Custom init/accumulate/merge/finalize lifecycle
 * NOTE: Arrow functions DO NOT work - must use traditional function syntax!
 */
import { Schema as S } from "@effect/schema";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $group, $sort, registry } from "../../../src/sluice.js";
import { ObjectIdSchema } from "../../utils/common-schemas.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";

describe("Hardcore: $accumulator with custom JavaScript functions", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should handle $accumulator with custom JavaScript functions", async () => {
    // 1. Define Result Schema
    const UserSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      email: S.String,
      age: S.Number,
      tags: S.Array(S.String),
    });

    const ItemSchema = S.Struct({
      _id: S.Number,
      emailChecksum: S.String,
    });
    const ResultSchema = S.Array(ItemSchema);

    const users = registry("8.0", { users: UserSchema });

    // 2. Insert Test Data
    await users(db)
      .users.insertMany([
        {
          _id: new ObjectId(),
          name: "Alice",
          email: "alice@example.com",
          age: 25,
          tags: ["admin", "developer"],
        },
        {
          _id: new ObjectId(),
          name: "Bob",
          email: "bob@example.com",
          age: 25,
          tags: ["user"],
        },
        {
          _id: new ObjectId(),
          name: "Carol",
          email: "carol@test.com",
          age: 35,
          tags: ["admin", "manager"],
        },
        {
          _id: new ObjectId(),
          name: "Dave",
          email: "dave@test.com",
          age: 35,
          tags: ["developer"],
        },
      ])
      .execute();

    // 3. Execute Raw MongoDB Query
    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("users")
      .aggregate([
        {
          $group: {
            _id: "$age",
            emailChecksum: {
              $accumulator: {
                init: "function(seedValue, salt) { return { hash: seedValue, count: 0, salt: salt }; }",
                initArgs: [0, "mySalt_"],
                accumulate:
                  "function(state, email) { for (var i = 0; i < email.length; i++) { var char = email.charCodeAt(i); state.hash = ((state.hash << 5) - state.hash) + char; state.hash |= 0; } state.count++; return state; }",
                accumulateArgs: ["$email"],
                merge:
                  "function(state1, state2) { return { hash: state1.hash + state2.hash, count: state1.count + state2.count, salt: state1.salt }; }",
                finalize:
                  "function(state) { return state.salt + state.hash.toString(16) + '_' + state.count; }",
                lang: "js",
              },
            },
          },
        },
        { $sort: { _id: 1 } },
      ])
      .toArray();

    // 4. Validate Raw Query
    expect(rawResults.length).toBeGreaterThan(0);

    // 5. Execute Sluice Query - Using typed inline functions
    const sluiceResults = await users(db)
      .users.aggregate(
        $group($ => ({
          _id: "$age",
          emailChecksum: $.accumulator({
            init: (seedValue: number, salt: string) => ({
              hash: seedValue,
              count: 0,
              salt,
            }),
            initArgs: [0, "mySalt_"],
            accumulate: (state, email) => {
              for (let i = 0; i < email.length; i++) {
                const char = email.charCodeAt(i);
                state.hash = (state.hash << 5) - state.hash + char;
                state.hash |= 0;
              }
              state.count++;
              return state;
            },
            accumulateArgs: ["$email"],
            merge: (state1, state2) => ({
              hash: state1.hash + state2.hash,
              count: state1.count + state2.count,
              salt: state1.salt,
            }),
            finalize: state => state.salt + state.hash.toString(16) + "_" + String(state.count),
            lang: "js",
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
