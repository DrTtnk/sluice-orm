import { Schema as S } from "@effect/schema";
import { $geoNear, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const PlaceSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  location: S.Struct({
    type: S.Literal("Point"),
    coordinates: S.Tuple(S.Number, S.Number),
  }),
  category: S.String,
});

const dbRegistry = registry("8.0", { places: PlaceSchema });

describe("GeoNear Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .places.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          name: "Central Park",
          location: {
            type: "Point",
            coordinates: [-73.968285, 40.785091],
          },
          category: "Parks",
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          name: "Times Square",
          location: {
            type: "Point",
            coordinates: [-73.985428, 40.758896],
          },
          category: "Landmarks",
        },
        {
          _id: new ObjectId("000000000000000000000003"),
          name: "Prospect Park",
          location: {
            type: "Point",
            coordinates: [-73.969139, 40.660204],
          },
          category: "Parks",
        },
      ])
      .execute();

    await db.collection("places").createIndex({ location: "2dsphere" });
  });

  afterAll(async () => {
    await teardown();
  });

  it("should find nearby places with distance", async () => {
    const ResultSchema = S.Struct({
      ...PlaceSchema.fields,
      distance: S.Number,
    });

    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("places")
      .aggregate([
        {
          $geoNear: {
            near: {
              type: "Point",
              coordinates: [-73.99279, 40.719296],
            },
            distanceField: "distance",
            spherical: true,
          },
        },
      ])
      .toArray();

    const results = await dbRegistry(db)
      .places.aggregate(
        $geoNear({
          near: {
            type: "Point",
            coordinates: [-73.99279, 40.719296],
          },
          distanceField: "distance",
          spherical: true,
        }),
      )
      .toList();

    expect(rawResults.length).greaterThan(0);
    expect(results.length).greaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
    expect(results).toEqual(rawResults);
  });

  it("should handle geoNear with all options", async () => {
    const ResultSchema = S.Struct({
      ...PlaceSchema.fields,
      dist: S.Struct({
        calculated: S.Number,
        location: PlaceSchema.fields.location,
      }),
    });

    // eslint-disable-next-line custom/aggregate-must-tolist
    const rawResults = await db
      .collection("places")
      .aggregate([
        {
          $geoNear: {
            near: {
              type: "Point",
              coordinates: [-73.99279, 40.719296],
            },
            distanceField: "dist.calculated",
            spherical: true,
            maxDistance: 20000,
            minDistance: 10,
            query: { category: "Parks" },
            distanceMultiplier: 0.001,
            includeLocs: "dist.location",
            key: "location",
          },
        },
      ])
      .toArray();

    const results = await dbRegistry(db)
      .places.aggregate(
        $geoNear({
          near: {
            type: "Point",
            coordinates: [-73.99279, 40.719296],
          },
          distanceField: "dist.calculated",
          spherical: true,
          maxDistance: 20000,
          minDistance: 10,
          query: { category: "Parks" },
          distanceMultiplier: 0.001,
          includeLocs: "dist.location",
          key: "location",
        }),
      )
      .toList();

    expect(rawResults.length).greaterThan(0);
    expect(results.length).greaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
    expect(results).toEqual(rawResults);
  });
});
