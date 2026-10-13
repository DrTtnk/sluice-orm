/* eslint-disable custom/aggregate-must-tolist -- most tests here only inspect the built MQL */
import { Schema as S } from "@effect/schema";
import { type Db, ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  $listLocalSessions,
  $listSessions,
  $planCacheStats,
  $shardedDataDistribution,
  registry,
} from "../../src/sluice.js";
import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

const DocSchema = S.Struct({ _id: ObjectIdSchema, name: S.String });

const dbReg = registry("8.0", { admin_docs: DocSchema });

describe("Administrative stages", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
  }, 120000);

  afterAll(teardown);

  it("$planCacheStats builds an empty-spec stage", () => {
    expect(JSON.parse(dbReg(db).admin_docs.aggregate($planCacheStats()).toMQL())).toEqual([{ $planCacheStats: {} }]);
  });

  it("$shardedDataDistribution builds an empty-spec stage", () => {
    expect(JSON.parse(dbReg(db).admin_docs.aggregate($shardedDataDistribution()).toMQL())).toEqual([
      { $shardedDataDistribution: {} },
    ]);
  });

  it("$listLocalSessions forwards its options", () => {
    expect(
      JSON.parse(
        dbReg(db)
          .admin_docs.aggregate($listLocalSessions({ allUsers: true }))
          .toMQL(),
      ),
    ).toEqual([{ $listLocalSessions: { allUsers: true } }]);
  });

  it("$listSessions forwards its options", () => {
    expect(
      JSON.parse(
        dbReg(db)
          .admin_docs.aggregate($listSessions({ allUsers: true }))
          .toMQL(),
      ),
    ).toEqual([{ $listSessions: { allUsers: true } }]);
  });

  // $listLocalSessions, $listSessions and $shardedDataDistribution run only at database level ({ aggregate: 1 }).
  // Sluice has no database-level aggregate yet, so only their MQL is tested here.
  it("$planCacheStats runs against an existing collection", async () => {
    const coll = dbReg(db).admin_docs;
    await coll.insertOne({ _id: new ObjectId(), name: "cached" }).execute();

    expect(Array.isArray(await coll.aggregate($planCacheStats()).toList())).toBe(true);
  });
});
