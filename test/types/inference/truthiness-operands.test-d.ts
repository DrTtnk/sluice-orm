/**
 * $and, $or, $nor, $cond.if and $switch.case accept any expression. MongoDB treats null, missing, 0 and false as false.
 * @see https://www.mongodb.com/docs/v8.0/reference/operator/aggregation/and/
 */
import { Schema as S } from "@effect/schema";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";
import { expectType } from "tsd";

import { $project, collection } from "../../../src/sluice.js";

const ObjectIdSchema = S.instanceOf(_ObjectId);

const TestSchema = S.Struct({
  _id: ObjectIdSchema,
  str: S.String,
  num: S.Number,
  nullableNum: S.Union(S.Number, S.Null),
  flag: S.Boolean,
});

const mockDb = {} as Db;
const docs = collection("docs", TestSchema, mockDb.collection("docs"));

const results = docs
  .aggregate(
    $project($ => ({
      booleans: $.and("$flag", true),
      numberAndBoolean: $.and("$num", true),
      stringOrBoolean: $.or("$str", "$flag"),
      nullable: $.and("$nullableNum"),
      literals: $.and(1, 0),
      comparison: $.or($.gt("$num", 1), "$flag"),
      condNumber: $.cond({ if: "$num", then: "yes", else: "no" }),
      condString: $.cond({ if: "$str", then: 1, else: 2 }),
      condBoolean: $.cond({ if: "$flag", then: 1, else: 2 }),
      switched: $.switch({ branches: [{ case: "$nullableNum", then: "a" }], default: "b" }),
    })),
  )
  .toList();

type Doc = Awaited<typeof results>[number];

expectType<boolean>({} as Doc["booleans"]);
expectType<boolean>({} as Doc["numberAndBoolean"]);
expectType<boolean>({} as Doc["stringOrBoolean"]);
expectType<boolean>({} as Doc["nullable"]);
expectType<boolean>({} as Doc["literals"]);
expectType<boolean>({} as Doc["comparison"]);
expectType<"yes" | "no">({} as Doc["condNumber"]);
expectType<1 | 2>({} as Doc["condString"]);
expectType<1 | 2>({} as Doc["condBoolean"]);
expectType<"a" | "b">({} as Doc["switched"]);
