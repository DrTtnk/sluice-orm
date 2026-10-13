/**
 * Boolean operators accept any expression, but a field reference must still point to an existing field.
 */
import { Schema as S } from "@effect/schema";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";

import { $project, collection } from "../../../src/sluice.js";

const ObjectIdSchema = S.instanceOf(_ObjectId);

const TestSchema = S.Struct({
  _id: ObjectIdSchema,
  num: S.Number,
});

const mockDb = {} as Db;
const docs = collection("docs", TestSchema, mockDb.collection("docs"));

docs.aggregate(
  $project($ => ({
    // @ts-expect-error - unknown field in $and
    andUnknown: $.and("$missing", true),
    // @ts-expect-error - unknown field in $or
    orUnknown: $.or(true, "$missing"),
    // @ts-expect-error - unknown field in $cond.if
    condUnknown: $.cond({ if: "$missing", then: 1, else: 2 }),
  })),
);
