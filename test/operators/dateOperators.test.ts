import { Schema as S } from "@effect/schema";
import { $addFields, $match, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const EventSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  startDate: S.Date,
  endDate: S.Date,
  createdAt: S.Date,
  scheduledFor: S.Date,
  year: S.Number,
  month: S.Number,
  day: S.Number,
  hour: S.Number,
  timezone: S.String,
  dateString: S.String,
  isoDateString: S.String,
});

const dbRegistry = registry("8.0", { events: EventSchema });

describe("Date Operators Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .events.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          name: "Conference",
          startDate: new Date("2024-03-15T09:00:00Z"),
          endDate: new Date("2024-03-17T17:00:00Z"),
          createdAt: new Date("2024-01-01T00:00:00Z"),
          scheduledFor: new Date("2024-03-15T14:00:00Z"),
          year: 2024,
          month: 3,
          day: 15,
          hour: 9,
          timezone: "America/New_York",
          dateString: "2024-03-15",
          isoDateString: "2024-03-15T09:00:00Z",
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  // $dateAdd tests
  it("should add days to date", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      reminderDate: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          reminderDate: $.dateAdd({
            startDate: "$startDate",
            unit: "day",
            amount: -7,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should add hours to date", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      adjustedTime: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          adjustedTime: $.dateAdd({
            startDate: "$scheduledFor",
            unit: "hour",
            amount: 2,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should add months with timezone", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      nextMonth: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          nextMonth: $.dateAdd({
            startDate: "$startDate",
            unit: "month",
            amount: 1,
            timezone: "$timezone",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should add years", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      anniversary: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          anniversary: $.dateAdd({
            startDate: "$createdAt",
            unit: "year",
            amount: 1,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should add minutes using field value", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      delayed: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          delayed: $.dateAdd({
            startDate: "$startDate",
            unit: "minute",
            amount: "$hour",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  // $dateSubtract tests
  it("should subtract days from date", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      weekBefore: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          weekBefore: $.dateSubtract({
            startDate: "$startDate",
            unit: "day",
            amount: 7,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should subtract hours from date", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      twoHoursAgo: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          twoHoursAgo: $.dateSubtract({
            startDate: "$scheduledFor",
            unit: "hour",
            amount: 2,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should subtract with timezone", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      lastMonth: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          lastMonth: $.dateSubtract({
            startDate: "$startDate",
            unit: "month",
            amount: 1,
            timezone: "America/New_York",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  // $dateTrunc tests
  it("should truncate to day", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      dayStart: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          dayStart: $.dateTrunc({
            date: "$startDate",
            unit: "day",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should truncate to month", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      monthStart: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          monthStart: $.dateTrunc({
            date: "$startDate",
            unit: "month",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should truncate to week with timezone", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      weekStart: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          weekStart: $.dateTrunc({
            date: "$startDate",
            unit: "week",
            timezone: "$timezone",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should truncate to hour", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      hourStart: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          hourStart: $.dateTrunc({
            date: "$scheduledFor",
            unit: "hour",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should truncate to year", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      yearStart: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          yearStart: $.dateTrunc({
            date: "$createdAt",
            unit: "year",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  // $dateFromParts tests
  it("should construct date from individual fields", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      constructedDate: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          constructedDate: $.dateFromParts({
            year: "$year",
            month: "$month",
            day: "$day",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should construct date with time", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      fullDate: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          fullDate: $.dateFromParts({
            year: "$year",
            month: "$month",
            day: "$day",
            hour: "$hour",
            minute: 0,
            second: 0,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should construct date with timezone", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      tzDate: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          tzDate: $.dateFromParts({
            year: "$year",
            month: "$month",
            day: "$day",
            timezone: "$timezone",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should construct date with ISO week", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      isoDate: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          isoDate: $.dateFromParts({
            isoWeekYear: 2024,
            isoWeek: 1,
            isoDayOfWeek: 1,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  // $dateFromString tests
  it("should parse ISO date string", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      parsed: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({ parsed: $.dateFromString({ dateString: "$isoDateString" }) })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should parse with format", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      parsedFormatted: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          parsedFormatted: $.dateFromString({
            dateString: "$dateString",
            format: "%Y-%m-%d",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should parse with timezone", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      parsedWithTz: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          parsedWithTz: $.dateFromString({
            dateString: "$dateString",
            timezone: "$timezone",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should parse with onError and onNull", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      safeParsed: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          safeParsed: $.dateFromString({
            dateString: "$dateString",
            onError: new Date(0),
            onNull: new Date(0),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  // $dateToParts tests
  it("should get all date parts", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      parts: S.Struct({
        year: S.Number,
        month: S.Number,
        day: S.Number,
        hour: S.Number,
        minute: S.Number,
        second: S.Number,
        millisecond: S.Number,
      }),
    });

    const results = await dbRegistry(db)
      .events.aggregate($addFields($ => ({ parts: $.dateToParts({ date: "$startDate" }) })))
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should get parts with timezone", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      tzParts: S.Struct({
        year: S.Number,
        month: S.Number,
        day: S.Number,
        hour: S.Number,
        minute: S.Number,
        second: S.Number,
        millisecond: S.Number,
      }),
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          tzParts: $.dateToParts({
            date: "$startDate",
            timezone: "$timezone",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should get ISO parts", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      isoParts: S.Struct({
        isoWeekYear: S.Number,
        isoWeek: S.Number,
        isoDayOfWeek: S.Number,
        hour: S.Number,
        minute: S.Number,
        second: S.Number,
        millisecond: S.Number,
      }),
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          isoParts: $.dateToParts({
            date: "$startDate",
            iso8601: true,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should extract ISO day of week and week number", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      isoDayOfWeek: S.Number,
      weekOfYear: S.Number,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          isoDayOfWeek: $.isoDayOfWeek("$startDate"),
          weekOfYear: $.week("$startDate"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  // Complex date operations
  it("should get start of week and add 7 days", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      nextWeekStart: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          nextWeekStart: $.dateAdd({
            startDate: $.dateTrunc({
              date: "$startDate",
              unit: "week",
            }),
            unit: "week",
            amount: 1,
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should parse string and extract year", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      parsedDate: S.Date,
      yearFromString: S.Number,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({ parsedDate: $.dateFromString({ dateString: "$dateString" }) })),
        $addFields($ => ({ yearFromString: $.year("$parsedDate") })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should construct date then format as string", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      constructedDate: S.Date,
      formatted: S.String,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          constructedDate: $.dateFromParts({
            year: "$year",
            month: "$month",
            day: "$day",
          }),
        })),
        $addFields($ => ({
          formatted: $.dateToString({
            date: "$constructedDate",
            format: "%Y-%m-%d",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should calculate duration using dateSubtract and match", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      oneWeekBefore: S.Date,
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          oneWeekBefore: $.dateSubtract({
            startDate: "$endDate",
            unit: "day",
            amount: 7,
          }),
        })),
        $match($ => ({ $expr: $.gte("$startDate", "$oneWeekBefore") })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });

  it("should truncate then get parts", async () => {
    const ResultSchema = S.Struct({
      ...EventSchema.fields,
      truncated: S.Date,
      dayParts: S.Struct({
        year: S.Number,
        month: S.Number,
        day: S.Number,
        hour: S.Number,
        minute: S.Number,
        second: S.Number,
        millisecond: S.Number,
      }),
    });

    const results = await dbRegistry(db)
      .events.aggregate(
        $addFields($ => ({
          truncated: $.dateTrunc({
            date: "$startDate",
            unit: "day",
          }),
        })),
        $addFields($ => ({ dayParts: $.dateToParts({ date: "$truncated" }) })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
  });
});
