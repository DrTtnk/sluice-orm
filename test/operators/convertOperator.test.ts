import { Schema as S } from "@effect/schema";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $addFields, $match, $project, registry } from "../../src/sluice.js";
import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

const DocSchema = S.Struct({
  _id: ObjectIdSchema,
  strNum: S.String,
  value: S.Number,
  flag: S.Boolean,
  date: S.Date,
  invalid: S.String,
});

const dbReg = registry("8.0", { conversions: DocSchema });

describe("$convert Operator Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const ctx = await setup();
    db = ctx.db;

    await dbReg(db)
      .conversions.insertMany([
        {
          _id: new ObjectId(),
          strNum: "42",
          value: 3.14,
          flag: true,
          date: new Date("2024-06-15"),
          invalid: "not_a_number",
        },
        {
          _id: new ObjectId(),
          strNum: "100",
          value: 0,
          flag: false,
          date: new Date("2020-01-01"),
          invalid: "NaN",
        },
      ])
      .execute();
  });

  afterAll(teardown);

  it("should convert string to int", async () => {
    const coll = dbReg(db).conversions;
    const result = await coll
      .aggregate(
        $match(() => ({ strNum: "42" })),
        $project($ => ({
          asInt: $.convert({ input: "$strNum", to: "int" }),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.asInt).toBe(42);
  });

  it("should convert string to double", async () => {
    const coll = dbReg(db).conversions;
    const result = await coll
      .aggregate(
        $match(() => ({ strNum: "42" })),
        $project($ => ({
          asDouble: $.convert({ input: "$strNum", to: "double" }),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.asDouble).toBe(42);
  });

  it("should convert number to string", async () => {
    const coll = dbReg(db).conversions;
    const result = await coll
      .aggregate(
        $match(() => ({ strNum: "42" })),
        $project($ => ({
          asString: $.convert({ input: "$value", to: "string" }),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.asString).toBe("3.14");
  });

  it("should convert boolean to int", async () => {
    const coll = dbReg(db).conversions;
    const result = await coll
      .aggregate(
        $project($ => ({
          strNum: 1,
          flagAsInt: $.convert({ input: "$flag", to: "int" }),
        })),
      )
      .toList();

    expect(result).toHaveLength(2);
    // true -> 1, false -> 0
    const trueDoc = result.find((d: { strNum: string }) => d.strNum === "42");
    const falseDoc = result.find((d: { strNum: string }) => d.strNum === "100");
    expect(trueDoc?.flagAsInt).toBe(1);
    expect(falseDoc?.flagAsInt).toBe(0);
  });

  it("should use onError for invalid conversion", async () => {
    const coll = dbReg(db).conversions;
    const result = await coll
      .aggregate(
        $match(() => ({ strNum: "42" })),
        $project($ => ({
          safe: $.convert({ input: "$invalid", to: "int", onError: -1 }),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.safe).toBe(-1); // "not_a_number" can't convert to int
  });

  it("should use onNull for null input", async () => {
    const coll = dbReg(db).conversions;
    const result = await coll
      .aggregate(
        $match(() => ({ strNum: "42" })),
        $addFields($ => ({
          nullField: $.literal(null),
        })),
        $project($ => ({
          withDefault: $.convert({ input: "$nullField", to: "int", onNull: 0 }),
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    expect(result[0]?.withDefault).toBe(0);
  });

  it("$toBool should convert values", async () => {
    const coll = dbReg(db).conversions;
    const result = await coll
      .aggregate(
        $project($ => ({
          strNum: 1,
          numAsBool: $.toBool("$value"),
        })),
      )
      .toList();

    expect(result).toHaveLength(2);
    // 3.14 -> true, 0 -> false
    const nonZero = result.find((d: { strNum: string }) => d.strNum === "42");
    const zero = result.find((d: { strNum: string }) => d.strNum === "100");
    expect(nonZero?.numAsBool).toBe(true);
    expect(zero?.numAsBool).toBe(false);
  });
});
