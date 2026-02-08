/**
 * Tests for type-safe $accumulator with arrow functions
 *
 * This test demonstrates that arrow functions work correctly in $accumulator
 * by being automatically converted to traditional function syntax at runtime.
 */
import { Schema as S } from "@effect/schema";
import { Db } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { $group, $sort, registry } from "../../../src/sluice.js";
import { setup, teardown } from "../../utils/setup.js";
import { assertSync } from "../../utils/utils.js";

describe("Type-safe $accumulator with arrow functions", () => {
  let db: Db;

  const ItemSchema = S.Struct({
    _id: S.Number,
    category: S.String,
    value: S.Number,
  });

  const items = registry("8.0", { items: ItemSchema });

  beforeAll(async () => {
    db = (await setup()).db;
  }, 120000);

  afterAll(async () => {
    await teardown();
  });

  it("should work with simple arrow functions", async () => {
    // 0. Clear collection
    await db.collection("items").deleteMany({});

    // 1. Define result schema
    const ResultSchema = S.Struct({
      _id: S.String,
      total: S.Number,
    });

    // 2. Seed test data
    await items(db)
      .items.insertMany([
        {
          _id: 1,
          category: "A",
          value: 10,
        },
        {
          _id: 2,
          category: "A",
          value: 20,
        },
        {
          _id: 3,
          category: "B",
          value: 30,
        },
        {
          _id: 4,
          category: "B",
          value: 40,
        },
      ])
      .execute();

    // 3. Execute with sluice using arrow functions
    type SumState = { sum: number };

    const results = await items(db)
      .items.aggregate(
        $group($ => ({
          _id: "$category",
          total: $.accumulator({
            // Arrow function with expression body
            init: () => ({ sum: 0 }),
            initArgs: [],
            // Arrow function with block body
            accumulate: (state, value) => {
              state.sum += value;
              return state;
            },
            accumulateArgs: ["$value"],
            // Arrow function with expression body (object literal)
            merge: (s1, s2) => ({ sum: s1.sum + s2.sum }),
            // Arrow function with expression body
            finalize: state => state.sum,
            lang: "js",
          }),
        })),
        $sort({ _id: 1 }),
      )
      .toList();

    // 4. Validate
    assertSync(S.Array(ResultSchema), results);
    expectType<readonly (typeof ResultSchema.Type)[]>({} as typeof results);

    expect(results).toEqual([
      {
        _id: "A",
        total: 30,
      },
      {
        _id: "B",
        total: 70,
      },
    ]);
  });

  it("should have proper type inference for function parameters", async () => {
    // 0. Clear collection
    await db.collection("items").deleteMany({});

    // 2. Seed test data
    await items(db)
      .items.insertMany([
        {
          _id: 5,
          category: "C",
          value: 5,
        },
        {
          _id: 6,
          category: "C",
          value: 15,
        },
      ])
      .execute();

    // This test verifies that TypeScript correctly infers types
    type CountState = { count: number; max: number };

    const results = await items(db)
      .items.aggregate(
        $group($ => ({
          _id: "$category",
          stats: $.accumulator({
            // init parameters should be inferred as [number]
            init: (startMax: number) => ({
              count: 0,
              max: startMax,
            }),
            initArgs: [0],
            // accumulate parameters should be inferred as (state: CountState, value: number)
            accumulate: (state, value) => ({
              count: state.count + 1,
              max: Math.max(state.max, value),
            }),
            accumulateArgs: ["$value"],
            // merge parameters should be inferred as (s1: CountState, s2: CountState)
            merge: (s1, s2) => ({
              count: s1.count + s2.count,
              max: Math.max(s1.max, s2.max),
            }),
            // finalize parameter should be inferred as (state: CountState)
            finalize: state => ({
              count: state.count,
              max: state.max,
            }),
            lang: "js",
          }),
        })),
        $sort({ _id: 1 }),
      )
      .toList();

    expect(results).toHaveLength(1);
    expect(results[0]?._id).toBe("C");
    expect(results[0]?.stats.count).toBe(2);
    expect(results[0]?.stats.max).toBe(15);
  });

  it("should work with complex state types", async () => {
    // 0. Clear collection
    await db.collection("items").deleteMany({});

    // Test with complex nested state
    type ComplexState = {
      values: number[];
      metadata: {
        count: number;
        sum: number;
        min: number;
        max: number;
      };
    };

    await items(db)
      .items.insertMany([
        {
          _id: 7,
          category: "D",
          value: 100,
        },
        {
          _id: 8,
          category: "D",
          value: 200,
        },
        {
          _id: 9,
          category: "D",
          value: 150,
        },
      ])
      .execute();

    const results = await items(db)
      .items.aggregate(
        $group($ => ({
          _id: "$category",
          analysis: $.accumulator({
            init: () => ({
              values: [] as number[],
              metadata: {
                count: 0,
                sum: 0,
                min: Infinity,
                max: -Infinity,
              },
            }),
            initArgs: [],
            accumulate: (state, value) => {
              state.values.push(value);
              state.metadata.count++;
              state.metadata.sum += value;
              state.metadata.min = Math.min(state.metadata.min, value);
              state.metadata.max = Math.max(state.metadata.max, value);
              return state;
            },
            accumulateArgs: ["$value"], // ToDo the auto-completion here doens't work
            merge: (s1, s2) => ({
              values: [...s1.values, ...s2.values],
              metadata: {
                count: s1.metadata.count + s2.metadata.count,
                sum: s1.metadata.sum + s2.metadata.sum,
                min: Math.min(s1.metadata.min, s2.metadata.min),
                max: Math.max(s1.metadata.max, s2.metadata.max),
              },
            }),
            lang: "js",
          }),
        })),
        $sort({ _id: 1 }),
      )
      .toList();

    expect(results).toHaveLength(1);
    expect(results[0]?._id).toBe("D");
    expect(results[0]?.analysis.metadata.count).toBe(3);
    expect(results[0]?.analysis.metadata.sum).toBe(450);
    expect(results[0]?.analysis.metadata.min).toBe(100);
    expect(results[0]?.analysis.metadata.max).toBe(200);
    expect(results[0]?.analysis.values).toEqual([100, 200, 150]);
  });
});
