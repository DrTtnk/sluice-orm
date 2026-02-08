/**
 * Runtime Tests: Callback API for $set and $replaceRoot
 *
 * Validates callback API works in aggregation pipelines
 */
import { Schema as S } from "@effect/schema";
import { $match, $replaceRoot, $set, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { setup, teardown } from "../utils/setup.js";

const UserSchema = S.Struct({
  _id: S.String,
  name: S.String,
  age: S.Number,
  score: S.optional(S.Number),
});

const dbRegistry = registry("8.0", { users: UserSchema });

describe("Callback API in Aggregation Pipelines", () => {
  let db: Db;
  let r: ReturnType<typeof dbRegistry>;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;
    r = dbRegistry(db);

    await db.collection("users").insertOne({
      _id: "user1" as any, // Schema uses string _id
      name: "Alice",
      age: 30,
      score: 0,
    });
  });

  afterAll(async () => {
    await teardown();
  });

  it("$set with callback API", async () => {
    const result = await r.users
      .aggregate(
        $match($ => ({
          _id: "user1",
        })),
        $set($ => ({
          doubleAge: $.multiply(2, "$age"),
          upperName: $.toUpper("$name"),
        })),
      )
      .toList();

    const doc = result[0] as any;
    expect(doc.doubleAge).toBe(60);
    expect(doc.upperName).toBe("ALICE");
  });

  it("$set with direct object", async () => {
    const result = await r.users
      .aggregate(
        $match($ => ({
          _id: "user1",
        })),
        $set($ => ({
          constant: $.literal(100),
        })),
      )
      .toList();

    const doc = result[0] as any;
    expect(doc.constant).toBe(100);
  });

  it("$replaceRoot with callback API (options form)", async () => {
    const result = await r.users
      .aggregate(
        $match($ => ({
          _id: "user1",
        })),
        $replaceRoot($ => ({
          _id: "$name",
          totalScore: $.add("$age", $.ifNull("$score", 0)),
        })),
      )
      .toList();

    const doc = result[0] as any;
    expect(doc._id).toBe("Alice");
    expect(doc.totalScore).toBe(30);
  });

  it("$replaceRoot with direct expression", async () => {
    const result = await r.users
      .aggregate(
        $match($ => ({
          _id: "user1",
        })),
        $replaceRoot({
          newRoot: { _id: "$name", justAge: "$age" } as const,
        }),
      )
      .toList();

    const doc = result[0] as any;
    expect(doc._id).toBe("Alice");
    expect(doc.justAge).toBe(30);
  });
});
