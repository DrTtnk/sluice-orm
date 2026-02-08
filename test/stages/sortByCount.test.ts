// Runtime equivalent of sortByCount.test-d.ts
import { Schema as S } from "@effect/schema";
import { $sortByCount, $unwind, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const ExhibitSchema = S.Struct({
  _id: ObjectIdSchema,
  title: S.String,
  artist: S.String,
  tags: S.Array(S.String),
  year: S.Number,
});

type Exhibit = typeof ExhibitSchema.Type;

const dbRegistry = registry("8.0", { exhibits: ExhibitSchema });

describe("SortByCount Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .exhibits.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          title: "Painting 1",
          artist: "Picasso",
          tags: ["modern", "art"],
          year: 1950,
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          title: "Painting 2",
          artist: "Picasso",
          tags: ["modern"],
          year: 1955,
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          title: "Sculpture 1",
          artist: "Rodin",
          tags: ["classic", "art"],
          year: 1920,
        },
        {
          _id: new ObjectId("000000000000000000000004"),
          title: "Drawing 1",
          artist: "DaVinci",
          tags: ["classic"],
          year: 1500,
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should count by string field", async () => {
    const ResultSchema = S.Struct({
      _id: S.String, // $sortByCount groups by artist (a string field)
      count: S.Number,
    });

    const results = await dbRegistry(db).exhibits.aggregate($sortByCount("$artist")).toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should count by number field", async () => {
    const ResultSchema = S.Struct({
      _id: S.Number,
      count: S.Number,
    });

    const results = await dbRegistry(db).exhibits.aggregate($sortByCount("$year")).toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should count by tags after unwind", async () => {
    const ResultSchema = S.Struct({
      _id: S.String, // $sortByCount groups by tags (a string array element)
      count: S.Number,
    });

    const results = await dbRegistry(db)
      .exhibits.aggregate($unwind("$tags"), $sortByCount("$tags"))
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
