import { Schema as S } from "@effect/schema";
import { $addFields, $group, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const Tags = S.Literal("developer", "senior", "backend", "frontend", "fullstack");

const DocumentSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  scores: S.Array(S.Number),
  grades: S.Array(
    S.Struct({
      subject: S.String,
      score: S.Number,
      date: S.Date,
    }),
  ),
  tags: S.Array(Tags),
  values: S.Array(S.Number),
  start: S.Number,
  end: S.Number,
  step: S.Number,
  count: S.Number,
  items: S.Array(
    S.Struct({
      name: S.String,
      price: S.Number,
      quantity: S.Number,
    }),
  ),
});

const dbRegistry = registry("8.0", { docs: DocumentSchema });

describe("Array Operators Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .docs.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          name: "Alice",
          scores: [85, 92, 88, 90, 95],
          grades: [
            {
              subject: "Math",
              score: 90,
              date: new Date("2024-01-01"),
            },
            {
              subject: "Science",
              score: 85,
              date: new Date("2024-01-02"),
            },
          ],
          tags: ["developer", "senior", "backend"],
          values: [10, 20, 30, 40, 50],
          start: 0,
          end: 10,
          step: 2,
          count: 3,
          items: [
            {
              name: "laptop",
              price: 1200,
              quantity: 1,
            },
            {
              name: "mouse",
              price: 25,
              quantity: 2,
            },
            {
              name: "keyboard",
              price: 75,
              quantity: 1,
            },
          ],
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  // $range tests
  it("should generate basic range from 0 to 10", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      sequence: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ sequence: $.range(0, 10) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should generate range with step", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      evens: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ evens: $.range(0, 20, 2) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should generate range from field values", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      dynamicRange: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ dynamicRange: $.range("$start", "$end") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should generate range with all field values", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      fullRange: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ fullRange: $.range("$start", "$end", "$step") })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should use range in map for indexing", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      indexed: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          indexed: $.map({
            input: $.range(0, $.size("$scores")),
            in: $ => $.add("$$this", 1),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should map with custom as and deep object input", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      itemSummaries: S.Array(
        S.Struct({
          meta: S.Struct({
            name: S.String,
            price: S.Number,
          }),
          quantity: S.Number,
        }),
      ),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          itemSummaries: $.map({
            input: "$items",
            as: "item",
            in: {
              meta: {
                name: "$$item.name",
                price: "$$item.price",
              },
              quantity: "$$item.quantity",
            },
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
    const firstSummary = results[0]?.itemSummaries[0];
    expect(firstSummary?.meta.name).toBe("laptop");
    expect(firstSummary?.meta.price).toBe(1200);
  });

  it("should map with custom as in callback", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      itemLabels: S.Array(S.TemplateLiteral(S.String, S.Literal(" - "), S.String)),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          itemLabels: $.map({
            input: "$items",
            as: "item",
            in: $ => $.concat("$$item.name", " - ", $.toString("$$item.price")),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
    const firstLabel = results[0]?.itemLabels[0];
    expect(firstLabel).toBe("laptop - 1200");
  });

  // $zip tests
  it("should zip two arrays", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      paired: S.Array(S.Tuple(S.Number, Tags)),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ paired: $.zip({ inputs: ["$scores", "$tags"] }) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should zip with useLongestLength", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      pairedFull: S.Array(S.Tuple(S.NullOr(S.Number), S.NullOr(Tags))),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          pairedFull: $.zip({
            inputs: ["$scores", "$tags"],
            useLongestLength: true,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should zip with defaults", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      pairedWithDefaults: S.Array(S.Tuple(S.Number, Tags)),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          pairedWithDefaults: $.zip({
            inputs: ["$scores", "$tags"],
            useLongestLength: true,
            defaults: [0, "backend"],
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should zip three arrays", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      scoreWithTag: S.Array(S.Tuple(S.Number, Tags, S.Number)),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({ scoreWithTag: $.zip({ inputs: ["$scores", "$tags", "$values"] }) })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // $firstN tests
  it("should get first 3 scores", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      topScores: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          topScores: $.firstN({
            input: "$scores",
            n: 3,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should get first N from field", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      firstTags: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          firstTags: $.firstN({
            input: "$tags",
            n: "$count",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should get first N of complex objects", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      topGrades: S.Array(
        S.Struct({
          subject: S.String,
          score: S.Number,
          date: S.Date,
        }),
      ),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          topGrades: $.firstN({
            input: "$grades",
            n: 5,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // $lastN tests
  it("should get last 3 scores", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      recentScores: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          recentScores: $.lastN({
            input: "$scores",
            n: 3,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should get last N tags", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      lastTags: S.Array(Tags),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          lastTags: $.lastN({
            input: "$tags",
            n: 2,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should get last N items", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      recentItems: S.Array(
        S.Struct({
          name: S.String,
          price: S.Number,
          quantity: S.Number,
        }),
      ),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          recentItems: $.lastN({
            input: "$items",
            n: "$count",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // $maxN tests
  it("should get top 3 highest scores", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      topThree: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          topThree: $.maxN({
            input: "$scores",
            n: 3,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should get top N values", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      topValues: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          topValues: $.maxN({
            input: "$values",
            n: "$count",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // $minN tests
  it("should get bottom 3 scores", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      bottomThree: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          bottomThree: $.minN({
            input: "$scores",
            n: 3,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should get min N values", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      lowestValues: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          lowestValues: $.minN({
            input: "$values",
            n: "$count",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // $sortArray tests
  it("should sort numbers ascending", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      sortedScores: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          sortedScores: $.sortArray({
            input: "$scores",
            sortBy: 1,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should sort numbers descending", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      sortedDesc: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          sortedDesc: $.sortArray({
            input: "$scores",
            sortBy: -1,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should sort objects by field", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      sortedGrades: S.Array(
        S.Struct({
          subject: S.String,
          score: S.Number,
          date: S.Date,
        }),
      ),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          sortedGrades: $.sortArray({
            input: "$grades",
            sortBy: { score: -1 },
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should sort items by price then quantity", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      sortedItems: S.Array(
        S.Struct({
          name: S.String,
          price: S.Number,
          quantity: S.Number,
        }),
      ),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          sortedItems: $.sortArray({
            input: "$items",
            sortBy: {
              price: -1,
              quantity: 1,
            },
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should sort tags alphabetically", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      sortedTags: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          sortedTags: $.sortArray({
            input: "$tags",
            sortBy: 1,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // Complex combined operations
  it("should get top 3 scores sorted", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      top3Sorted: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          top3Sorted: $.sortArray({
            input: $.maxN({
              input: "$scores",
              n: 3,
            }),
            sortBy: -1,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should create indexed pairs (index, value)", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      indexedScores: S.Array(S.Tuple(S.Number, S.Number)),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          indexedScores: $.zip({ inputs: [$.range(0, $.size("$scores")), "$scores"] }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should paginate array: skip first N, take next M", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      page: S.Array(S.Number),
    });

    const results = await dbRegistry(db)
      .docs.aggregate($addFields($ => ({ page: $.slice("$scores", 5, 10) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should sort and get top items", async () => {
    const ResultSchema = S.Struct({
      ...DocumentSchema.fields,
      topExpensive: S.Array(
        S.Struct({
          name: S.String,
          price: S.Number,
          quantity: S.Number,
        }),
      ),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $addFields($ => ({
          topExpensive: $.firstN({
            input: $.sortArray({
              input: "$items",
              sortBy: { price: -1 },
            }),
            n: 3,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  // Group context tests
  it("should use firstN/minN as accumulators in $group", async () => {
    const ResultSchema = S.Struct({
      _id: S.Null,
      topScoresPerDoc: S.Array(S.Array(S.Number)),
      bottomValues: S.Array(S.Array(S.Number)),
    });

    const results = await dbRegistry(db)
      .docs.aggregate(
        $group($ => ({
          _id: null,
          topScoresPerDoc: $.firstN({
            input: "$scores",
            n: 3,
          }),
          bottomValues: $.minN({
            input: "$values",
            n: 2,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
