import { Schema as S } from "@effect/schema";
import { $group, $project, $sort, registry } from "@sluice/sluice";
import { Db } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

// Schema for our test data
const DualSemanticsTestSchema = S.Struct({
  _id: S.Union(ObjectIdSchema, S.Number),
  group: S.String,
  val: S.Number,
  arr: S.Array(S.Number),
  obj: S.Struct({ x: S.Number }),
  obj2: S.Struct({ y: S.Number }),
});

// Registry definition
const dbRegistry = registry("8.0", {
  dual_ops_test: DualSemanticsTestSchema,
});

describe("Dual Semantics Operators Integration", () => {
  let db: Db;

  beforeAll(async () => {
    ({ db } = await setup());

    // Clean up
    await dbRegistry(db)
      .dual_ops_test.deleteMany(() => ({}))
      .execute();

    // Insert test data
    await dbRegistry(db)
      .dual_ops_test.insertMany([
        {
          _id: 1,
          group: "A",
          val: 10,
          arr: [1, 2, 3],
          obj: { x: 1 },
          obj2: { y: 2 },
        },
        {
          _id: 2,
          group: "A",
          val: 20,
          arr: [4, 5, 6],
          obj: { x: 2 },
          obj2: { y: 3 },
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should execute a pipeline using dual operators in both $project and $group stages", async () => {
    const boundRegistry = dbRegistry(db);

    // Stage 1: $project (Expression Context)

    const result = await boundRegistry.dual_ops_test
      .aggregate(
        $sort({ _id: 1 }), // Ensure stable order for first/last
        $project($ => ({
          group: "$group",
          // Arithmetic/Array reducers
          e_sum_args: $.sum("$val", 5), // Varargs: 10+5=15, 20+5=25
          e_sum_arr: $.sum("$arr"), // Array: sum([1,2,3])=6, sum([4,5,6])=15

          e_avg_args: $.avg("$val", 0), // Varargs: avg(10, 0)=5, avg(20, 0)=10
          e_avg_arr: $.avg("$arr"), // Array: avg([1,2,3])=2

          e_max_args: $.max("$val", 15), // Max(10, 15)=15, Max(20, 15)=20
          e_max_arr: $.max("$arr"), // Max([1,2,3])=3

          e_min_args: $.min("$val", 15), // Min(10, 15)=10, Min(20, 15)=15
          e_min_arr: $.min("$arr"), // Min([1,2,3])=1

          // Merge Objects
          e_merge: $.mergeObjects("$obj", "$obj2", { z: 9 }),

          // Statistics
          e_stdDevPop_args: $.stdDevPop(0, 10), // PopDev(0,10) = 5
          e_stdDevPop_arr: $.stdDevPop(0, 10),

          e_stdDevSamp_args: $.stdDevSamp(0, 10), // SampDev(0,10) approx 7.07
          e_stdDevSamp_arr: $.stdDevSamp(0, 10),

          // Array First/Last
          e_first: $.first("$arr"), // 1, 4
          e_last: $.last("$arr"), // 3, 6
        })),

        // Stage 2: $group (Accumulator Context)
        $group($ => ({
          _id: "$group",

          a_sum: $.sum("$e_sum_args"), // Sum of (15 + 25) = 40
          a_avg: $.avg("$e_sum_args"), // Avg of (15, 25) = 20

          a_max: $.max("$e_max_args"), // Max of (15, 20) = 20
          a_min: $.min("$e_min_args"), // Min of (10, 15) = 10

          // Merge Objects Accumulator (merges all 'e_merge' docs in group)
          // {x:1, y:2, z:9} merged with {x:2, y:3, z:9} -> {x:2, y:3, z:9} (last wins)
          a_merge: $.mergeObjects("$e_merge"),

          // Stats Accumulators
          a_stdDevPop: $.stdDevPop("$e_sum_args"), // PopDev(15, 25) = 5
          a_stdDevSamp: $.stdDevSamp("$e_sum_args"), // SampDev(15, 25) approx 7.07

          // First/Last Accumulators
          a_first: $.first("$e_first"), // First doc processed (likely _id:1 -> 1)
          a_last: $.last("$e_last"), // Last doc processed (likely _id:2 -> 6)
        })),
      )
      .toList();

    expect(result).toHaveLength(1);
    const doc = result[0];

    if (!doc) throw new Error("No document returned from aggregation");

    expect(doc._id).toBe("A");
    expect(doc.a_sum).toBe(40); // 15 + 25
    expect(doc.a_avg).toBe(20); // (15+25)/2
    expect(doc.a_max).toBe(20);
    expect(doc.a_min).toBe(10);

    // Check mergeObjects accumulator
    expect(doc.a_merge).toEqual({ x: 2, y: 3, z: 9 });

    // Check stats
    expect(doc.a_stdDevPop).toBe(5);
    expect(doc.a_stdDevSamp).toBeCloseTo(7.07, 2);

    // Check first/last (order dependent, but insertion order usually preserved for test)
    // _id:1 -> e_first: 1, e_last: 3
    // _id:2 -> e_first: 4, e_last: 6
    expect(doc.a_first).toBe(1);
    expect(doc.a_last).toBe(6);
  });
});
