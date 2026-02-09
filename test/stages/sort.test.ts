import { Schema as S } from "@effect/schema";
import { $addFields, $limit, $match, $skip, $sort, registry } from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";

/* eslint-disable @typescript-eslint/no-non-null-assertion */

const StudentSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  grade: S.Number,
  age: S.Number,
  subject: S.String,
});

const dbReg = registry("8.0", { students: StudentSchema });

describe("Sort / Limit / Skip Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const ctx = await setup();
    db = ctx.db;

    const coll = dbReg(db).students;
    await coll
      .insertMany([
        { _id: new ObjectId(), name: "Alice", grade: 95, age: 20, subject: "math" },
        { _id: new ObjectId(), name: "Bob", grade: 82, age: 22, subject: "science" },
        { _id: new ObjectId(), name: "Carol", grade: 95, age: 19, subject: "math" },
        { _id: new ObjectId(), name: "Dave", grade: 70, age: 21, subject: "science" },
        { _id: new ObjectId(), name: "Eve", grade: 88, age: 23, subject: "math" },
      ])
      .execute();
  }, 120000);

  afterAll(teardown);

  it("should sort ascending by single field", async () => {
    const coll = dbReg(db).students;
    const result = await coll.aggregate($sort({ grade: 1 })).toList();

    expect(result[0]!.name).toBe("Dave");
  });

  it("should sort descending by single field", async () => {
    const coll = dbReg(db).students;
    const result = await coll.aggregate($sort({ grade: -1 })).toList();

    expect(result[0]!.grade).toBe(95);
    expect(result[4]!.grade).toBe(70);
  });

  it("should sort by compound key", async () => {
    const coll = dbReg(db).students;
    const result = await coll.aggregate($sort({ grade: -1, age: 1 })).toList();

    // Both Alice(95, age 20) and Carol(95, age 19) have grade=95, secondary sort by age asc
    expect(result[0]!.name).toBe("Carol"); // age 19
    expect(result[1]!.name).toBe("Alice"); // age 20
  });

  it("should limit results", async () => {
    const coll = dbReg(db).students;
    const result = await coll.aggregate($sort({ grade: -1 }), $limit(3)).toList();

    expect(result).toHaveLength(3);
    expect(result[0]!.grade).toBe(95);
  });

  it("should skip results", async () => {
    const coll = dbReg(db).students;
    const result = await coll.aggregate($sort({ grade: -1 }), $skip(2)).toList();

    expect(result).toHaveLength(3);
  });

  it("should paginate with skip + limit", async () => {
    const coll = dbReg(db).students;

    const page1 = await coll.aggregate($sort({ grade: -1, name: 1 }), $limit(2)).toList();

    const page2 = await coll.aggregate($sort({ grade: -1, name: 1 }), $skip(2), $limit(2)).toList();

    expect(page1).toHaveLength(2);
    expect(page2).toHaveLength(2);
    const page1Names = page1.map(s => s.name);
    const page2Names = page2.map(s => s.name);
    expect(page1Names.some(n => page2Names.includes(n))).toBe(false);
  });

  it("should sort after match", async () => {
    const coll = dbReg(db).students;
    const result = await coll
      .aggregate(
        $match(() => ({ subject: "math" })),
        $sort({ grade: 1 }),
      )
      .toList();

    expect(result).toHaveLength(3);
    expect(result[0]!.name).toBe("Eve"); // 88
    expect(result[2]!.grade).toBe(95);
  });

  it("should sort on computed field", async () => {
    const coll = dbReg(db).students;
    const result = await coll
      .aggregate(
        $addFields($ => ({ adjustedGrade: $.add("$grade", "$age") })),
        $sort({ adjustedGrade: -1 }),
        $limit(1),
      )
      .toList();

    expect(result).toHaveLength(1);
    // Alice: 95+20=115, Carol: 95+19=114, Eve: 88+23=111
    expect(result[0]!.name).toBe("Alice");
  });
});
