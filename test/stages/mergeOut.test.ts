import { Schema as S } from "@effect/schema";
import { $group, $merge, $out, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

const ZipcodeEntrySchema = S.Struct({
  _id: ObjectIdSchema,
  city: S.String,
  state: S.String,
  pop: S.Number,
});

const StatePopulationSchema = S.Struct({
  _id: S.String,
  totalPop: S.Number,
});

const BookSchema = S.Struct({
  _id: ObjectIdSchema,
  title: S.String,
  author: S.String,
  copies: S.Number,
});

const AuthorSchema = S.Struct({
  _id: S.String,
  books: S.Array(S.String),
});

const dbRegistry = registry("8.0", {
  zipcodes: ZipcodeEntrySchema,
  statePopulations: StatePopulationSchema,
  books: BookSchema,
  authors: AuthorSchema,
});

describe("Merge and Out Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .zipcodes.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          city: "New York",
          state: "NY",
          pop: 8000000,
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          city: "Los Angeles",
          state: "CA",
          pop: 4000000,
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          city: "San Francisco",
          state: "CA",
          pop: 900000,
        },
      ])
      .execute();

    await dbRegistry(db)
      .books.insertMany([
        {
          _id: new ObjectId("000000000000000000000004"),
          title: "Book A",
          author: "Author 1",
          copies: 100,
        },
        {
          _id: new ObjectId("000000000000000000000005"),
          title: "Book B",
          author: "Author 1",
          copies: 50,
        },
        {
          _id: new ObjectId("000000000000000000000006"),
          title: "Book C",
          author: "Author 2",
          copies: 75,
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should merge to typed collection", async () => {
    const ResultSchema = S.Tuple();

    const results = await dbRegistry(db)
      .zipcodes.aggregate(
        $group($ => ({
          _id: "$state",
          totalPop: $.sum("$pop"),
        })),
        $merge({ into: dbRegistry(db).statePopulations }),
      )
      .toList();

    expectType<never[]>({} as typeof results);
  });

  it("should merge with options", async () => {
    const ResultSchema = S.Tuple();

    const results = await dbRegistry(db)
      .zipcodes.aggregate(
        $group($ => ({
          _id: "$state",
          totalPop: $.sum("$pop"),
        })),
        $merge({
          into: dbRegistry(db).statePopulations,
          on: "_id",
          whenMatched: "merge",
          whenNotMatched: "insert",
        }),
      )
      .toList();

    expectType<never[]>({} as typeof results);
  });

  it("should out to typed collection", async () => {
    const ResultSchema = S.Tuple();

    const results = await dbRegistry(db)
      .books.aggregate(
        $group($ => ({
          _id: "$author",
          books: $.push("$title"),
        })),
        $out(dbRegistry(db).authors),
      )
      .toList();

    expectType<never[]>({} as typeof results);
  });
});
