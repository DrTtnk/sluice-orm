// Runtime equivalent of bucket.test-d.ts
import { Schema as S } from "@effect/schema";
import { $bucket, $bucketAuto, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const ArtworkSchema = S.Struct({
  _id: ObjectIdSchema,
  title: S.String,
  artist: S.String,
  price: S.Number,
  year: S.Number,
});

type Artwork = typeof ArtworkSchema.Type;

const dbRegistry = registry("8.0", { artwork: ArtworkSchema });

describe("Bucket Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .artwork.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          title: "Art 1",
          artist: "Artist A",
          price: 100,
          year: 1990,
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          title: "Art 2",
          artist: "Artist B",
          price: 250,
          year: 2000,
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          title: "Art 3",
          artist: "Artist C",
          price: 450,
          year: 2010,
        },
        {
          _id: new ObjectId("000000000000000000000004"),
          title: "Art 4",
          artist: "Artist D",
          price: 350,
          year: 1995,
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should bucket by price with boundaries", async () => {
    const ResultSchema = S.Struct({
      _id: S.Union(S.Literal(0), S.Literal(200), S.Literal(400), S.Literal(500)),
      count: S.Number,
    });

    const results = await dbRegistry(db)
      .artwork.aggregate(
        $bucket({
          groupBy: "$price",
          boundaries: [0, 200, 400, 500],
        }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should bucket with default value", async () => {
    const ResultSchema = S.Struct({
      _id: S.Union(S.Literal(0), S.Literal(200), S.Literal(400), S.Literal("Other")),
      count: S.Number,
    });

    const results = await dbRegistry(db)
      .artwork.aggregate(
        $bucket({
          groupBy: "$price",
          boundaries: [0, 200, 400],
          default: "Other",
        }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should bucketAuto by price", async () => {
    const ResultSchema = S.Struct({
      _id: S.Struct({
        min: S.Number,
        max: S.Number,
      }),
      count: S.Number,
    });

    const results = await dbRegistry(db)
      .artwork.aggregate(
        $bucketAuto({
          groupBy: "$price",
          buckets: 4,
        }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
