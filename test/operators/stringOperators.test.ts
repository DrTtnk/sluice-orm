import { Schema as S } from "@effect/schema";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $addFields, $match, $project, registry } from "../../src/sluice.js";
import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

const DocSchema = S.Struct({
  _id: ObjectIdSchema,
  text: S.String,
  email: S.String,
  code: S.String,
});

const dbReg = registry("8.0", { strings: DocSchema });

describe("String Operators Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const ctx = await setup();
    db = ctx.db;

    await dbReg(db)
      .strings.insertMany([
        {
          _id: new ObjectId(),
          text: "  Hello World  ",
          email: "Alice@Example.COM",
          code: "ABC-123-DEF",
        },
        {
          _id: new ObjectId(),
          text: "  café résumé  ",
          email: "Bob@test.org",
          code: "XYZ-789-QRS",
        },
      ])
      .execute();
  });

  afterAll(teardown);

  it("$trim, $ltrim, $rtrim should strip whitespace", async () => {
    const coll = dbReg(db).strings;
    const result = await coll
      .aggregate(
        $match(() => ({ email: "Alice@Example.COM" })),
        $project($ => ({
          trimmed: $.trim({ input: "$text" }),
          ltrimmed: $.ltrim({ input: "$text" }),
          rtrimmed: $.rtrim({ input: "$text" }),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.trimmed).toBe("Hello World");
    expect(result[0]?.ltrimmed).toBe("Hello World  ");
    expect(result[0]?.rtrimmed).toBe("  Hello World");
  });

  it("$regexFind should return first match", async () => {
    const coll = dbReg(db).strings;
    const result = await coll
      .aggregate(
        $match(() => ({ email: "Alice@Example.COM" })),
        $project($ => ({
          found: $.regexFind({ input: "$code", regex: /(\d+)/ }),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.found).toMatchObject({ match: "123", idx: 4 });
  });

  it("$regexFindAll should return all matches", async () => {
    const coll = dbReg(db).strings;
    const result = await coll
      .aggregate(
        $match(() => ({ email: "Alice@Example.COM" })),
        $project($ => ({
          matches: $.regexFindAll({ input: "$code", regex: /[A-Z]+/ }),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.matches).toHaveLength(2); // "ABC" and "DEF"
    expect(result[0]?.matches.map((m: { match: string }) => m.match)).toEqual(["ABC", "DEF"]);
  });

  it("$regexMatch should return boolean", async () => {
    const coll = dbReg(db).strings;
    const result = await coll
      .aggregate(
        $project($ => ({
          email: 1,
          hasDigits: $.regexMatch({ input: "$code", regex: /\d+/ }),
        })),
      )
      .toList();

    expect(result).toHaveLength(2);
    for (const doc of result) {
      expect(doc.hasDigits).toBe(true);
    }
  });

  it("$replaceAll should replace all occurrences", async () => {
    const coll = dbReg(db).strings;
    const result = await coll
      .aggregate(
        $match(() => ({ email: "Alice@Example.COM" })),
        $project($ => ({
          replaced: $.replaceAll({ input: "$code", find: "-", replacement: "." }),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.replaced).toBe("ABC.123.DEF");
  });

  it("$replaceOne should replace first occurrence", async () => {
    const coll = dbReg(db).strings;
    const result = await coll
      .aggregate(
        $match(() => ({ email: "Alice@Example.COM" })),
        $project($ => ({
          replaced: $.replaceOne({ input: "$code", find: "-", replacement: "." }),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.replaced).toBe("ABC.123-DEF");
  });

  it("$indexOfCP should find codepoint position", async () => {
    const coll = dbReg(db).strings;
    const result = await coll
      .aggregate(
        $project($ => ({
          email: 1,
          atPos: $.indexOfCP("$email", "@"),
        })),
      )
      .toList();

    expect(result).toHaveLength(2);
    // "Alice@Example.COM" -> @ at position 5
    const alice = result.find((d: { email: string }) => d.email === "Alice@Example.COM");
    expect(alice?.atPos).toBe(5);
    // "Bob@test.org" -> @ at position 3
    const bob = result.find((d: { email: string }) => d.email === "Bob@test.org");
    expect(bob?.atPos).toBe(3);
  });

  it("$substrCP should extract by codepoint", async () => {
    const coll = dbReg(db).strings;
    const result = await coll
      .aggregate(
        $match(() => ({ email: "Bob@test.org" })),
        $project($ => ({
          // "café résumé" -> extract "café" (4 codepoints)
          sub: $.substrCP("$text", 2, 4), // skip leading spaces
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.sub).toBe("café");
  });

  it("$split should split string into array", async () => {
    const coll = dbReg(db).strings;
    const result = await coll
      .aggregate(
        $match(() => ({ email: "Alice@Example.COM" })),
        $project($ => ({
          parts: $.split("$code", "-"),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.parts).toEqual(["ABC", "123", "DEF"]);
  });

  it("$strcasecmp should compare case-insensitively", async () => {
    const coll = dbReg(db).strings;
    const result = await coll
      .aggregate(
        $match(() => ({ email: "Alice@Example.COM" })),
        $addFields($ => ({
          cmp: $.strcasecmp("$email", "alice@example.com"),
        })),
        $project($ => ({ cmp: 1 })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.cmp).toBe(0); // equal when case-insensitive
  });
});
