import { Schema as S } from "@effect/schema";
import { $graphLookup, $lookup, $match, $project, $unwind, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const EmployeeSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  reportsTo: S.NullOr(S.String),
  title: S.String,
});

const AirportSchema = S.Struct({
  _id: ObjectIdSchema,
  airport: S.String,
  connects: S.Array(S.String),
});

const CategorySchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  parentId: S.optional(ObjectIdSchema),
});

const ProductSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  categoryId: ObjectIdSchema,
});

const dbRegistry = registry("8.0", {
  employees: EmployeeSchema,
  airports: AirportSchema,
  products: ProductSchema,
  categories: CategorySchema,
});

describe("GraphLookup Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .employees.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          name: "Dev",
          reportsTo: "Manager",
          title: "Developer",
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          name: "Manager",
          reportsTo: "Director",
          title: "Manager",
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          name: "Director",
          reportsTo: null,
          title: "Director",
        },
      ])
      .execute();

    await dbRegistry(db)
      .airports.insertMany([
        {
          _id: new ObjectId("000000000000000000000004"),
          airport: "JFK",
          connects: ["LAX", "ORD"],
        },
        {
          _id: new ObjectId("000000000000000000000005"),
          airport: "LAX",
          connects: ["SFO"],
        },
        {
          _id: new ObjectId("000000000000000000000006"),
          airport: "ORD",
          connects: ["DEN"],
        },
      ])
      .execute();

    await dbRegistry(db)
      .products.insertMany([
        {
          _id: new ObjectId("000000000000000000000007"),
          name: "Laptop",
          categoryId: new ObjectId("000000000000000000000009"),
        },
        {
          _id: new ObjectId("000000000000000000000008"),
          name: "Phone",
          categoryId: new ObjectId("00000000000000000000000a"),
        },
      ])
      .execute();

    await dbRegistry(db)
      .categories.insertMany([
        {
          _id: new ObjectId("000000000000000000000009"),
          name: "Electronics",
        },
        {
          _id: new ObjectId("00000000000000000000000a"),
          name: "Mobile",
          parentId: new ObjectId("000000000000000000000009"),
        },
        {
          _id: new ObjectId("00000000000000000000000b"),
          name: "Technology",
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should find employee reporting hierarchy", async () => {
    const ResultSchema = S.Struct({
      ...EmployeeSchema.fields,
      reportingHierarchy: S.Array(EmployeeSchema),
    });

    const results = await dbRegistry(db)
      .employees.aggregate(
        $match($ => ({ name: "Dev" })),
        $graphLookup({
          from: dbRegistry(db).employees,
          startWith: "$reportsTo",
          connectFromField: "reportsTo",
          connectToField: "name",
          as: "reportingHierarchy",
        }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should handle graph lookup with max depth", async () => {
    const ResultSchema = S.Struct({
      ...EmployeeSchema.fields,
      managers: S.Array(EmployeeSchema),
    });

    const results = await dbRegistry(db)
      .employees.aggregate(
        $graphLookup({
          from: dbRegistry(db).employees,
          startWith: "$reportsTo",
          connectFromField: "reportsTo",
          connectToField: "name",
          as: "managers",
          maxDepth: 3,
        }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should track depth with depthField", async () => {
    const EmployeeWithLevel = S.Struct({
      ...EmployeeSchema.fields,
      level: S.Number,
    });

    const ResultSchema = S.Struct({
      ...EmployeeSchema.fields,
      chain: S.Array(EmployeeWithLevel),
    });

    const results = await dbRegistry(db)
      .employees.aggregate(
        $graphLookup({
          from: dbRegistry(db).employees,
          startWith: "$reportsTo",
          connectFromField: "reportsTo",
          connectToField: "name",
          as: "chain",
          maxDepth: 5,
          depthField: "level",
        }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should traverse airport connections", async () => {
    const ResultSchema = S.Struct({
      ...AirportSchema.fields,
      destinations: S.Array(AirportSchema),
    });

    const results = await dbRegistry(db)
      .airports.aggregate(
        $match($ => ({ airport: "JFK" })),
        $graphLookup({
          from: dbRegistry(db).airports,
          startWith: "$connects",
          connectFromField: "connects",
          connectToField: "airport",
          as: "destinations",
          maxDepth: 2,
        }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should handle graphLookup inside lookup pipeline with unwind and project", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      categoryName: S.String,
      ancestorNames: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .products.aggregate(
        $lookup({
          from: dbRegistry(db).categories,
          localField: "categoryId",
          foreignField: "_id",
          as: "category",
          pipeline: $ =>
            $.pipe(
              $graphLookup({
                from: dbRegistry(db).categories,
                startWith: "$parentId",
                connectFromField: "parentId",
                connectToField: "_id",
                as: "ancestors",
              }),
            ),
        }),
        $unwind("$category"),
        $project($ => ({
          name: 1,
          categoryName: "$category.name",
          ancestorNames: "$category.ancestors.name",
        })),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
  });
});
