// Runtime equivalent of unset.test-d.ts
import { Schema as S } from "@effect/schema";
import { $unset, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const BookSchema = S.Struct({
  _id: ObjectIdSchema,
  title: S.String,
  isbn: S.String,
  author: S.Struct({
    first: S.String,
    last: S.String,
  }),
  copies: S.Number,
});

type Book = typeof BookSchema.Type;

const dbRegistry = registry("8.0", { books: BookSchema });

describe("Unset Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .books.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          title: "MongoDB Guide",
          isbn: "123-456",
          author: {
            first: "John",
            last: "Doe",
          },
          copies: 5,
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          title: "NoSQL Patterns",
          isbn: "789-012",
          author: {
            first: "Jane",
            last: "Smith",
          },
          copies: 3,
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should unset single field", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      title: S.String,
      isbn: S.String,
      author: S.Struct({
        first: S.String,
        last: S.String,
      }),
    });

    const results = await dbRegistry(db).books.aggregate($unset("copies")).toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should unset multiple fields", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      title: S.String,
      author: S.Struct({
        first: S.String,
        last: S.String,
      }),
    });

    const results = await dbRegistry(db).books.aggregate($unset("isbn", "copies")).toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should unset _id field", async () => {
    const ResultSchema = S.Struct({
      title: S.String,
      isbn: S.String,
      author: S.Struct({
        first: S.String,
        last: S.String,
      }),
      copies: S.Number,
    });

    const results = await dbRegistry(db).books.aggregate($unset("_id")).toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });
});
