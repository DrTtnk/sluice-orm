import { Schema as S } from "@effect/schema";
import { $group, $limit, $match, $sort, registry } from "@sluice/sluice";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { expectType } from "tsd";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const CallEventSchema = S.Struct({
  _id: ObjectIdSchema,
  organizationId: S.String,
  modelId: S.String,
  environment: S.Literal("DEV", "UAT", "PROD"),
  createdDate: S.Date,
  requestedCache: S.Boolean,
  cacheHit: S.Boolean,
  unlimitedBudgetRequest: S.Boolean,
  response: S.Struct({
    cost: S.Number,
    inputTokens: S.Number,
    outputTokens: S.Number,
    executionTime: S.Number,
    error: S.NullOr(
      S.Struct({
        category: S.String,
        message: S.String,
      }),
    ),
  }),
});

const dbRegistry = registry("8.0", { callEvents: CallEventSchema });

describe("Real World Patterns Runtime Tests", () => {
  let db: Db;

  beforeAll(async () => {
    const res = await setup();
    db = res.db;

    await dbRegistry(db)
      .callEvents.insertMany([
        {
          _id: new ObjectId("000000000000000000000001"),
          organizationId: "org1",
          modelId: "gpt-4",
          environment: "PROD",
          createdDate: new Date("2024-01-15T10:00:00Z"),
          requestedCache: true,
          cacheHit: true,
          unlimitedBudgetRequest: false,
          response: {
            cost: 0.05,
            inputTokens: 100,
            outputTokens: 50,
            executionTime: 1.2,
            error: null,
          },
        },
        {
          _id: new ObjectId("000000000000000000000002"),
          organizationId: "org1",
          modelId: "gpt-4",
          environment: "PROD",
          createdDate: new Date("2024-01-15T11:00:00Z"),
          requestedCache: false,
          cacheHit: false,
          unlimitedBudgetRequest: false,
          response: {
            cost: 0.03,
            inputTokens: 80,
            outputTokens: 40,
            executionTime: 1.5,
            error: {
              category: "timeout",
              message: "Request timed out",
            },
          },
        },
      ])
      .execute();
  });

  afterAll(async () => {
    await teardown();
  });

  it("should aggregate daily analytics", async () => {
    const ResultSchema = S.Struct({
      _id: S.Struct({
        organizationId: S.String,
        createdDate: S.Date,
      }),
      callsTotal: S.Number,
      requestedCacheTotal: S.Number,
      cacheHitTotal: S.Number,
      costTotal: S.Number,
      inputTokensTotal: S.Number,
      outputTokensTotal: S.Number,
      avgExecutionTime: S.NullOr(S.Number),
    });

    const results = await dbRegistry(db)
      .callEvents.aggregate(
        $match($ => ({
          environment: "PROD",
          createdDate: { $gte: new Date("2024-01-01") },
        })),
        $group($ => ({
          _id: {
            organizationId: "$organizationId",
            createdDate: $.dateTrunc({
              date: "$createdDate",
              unit: "day",
            }),
          },
          callsTotal: $.sum(1),
          requestedCacheTotal: $.sum(
            $.cond({
              if: "$requestedCache",
              then: 1,
              else: 0,
            }),
          ),
          cacheHitTotal: $.sum(
            $.cond({
              if: "$cacheHit",
              then: 1,
              else: 0,
            }),
          ),
          costTotal: $.sum(
            $.cond({
              if: $.not("$unlimitedBudgetRequest"),
              then: "$response.cost",
              else: 0,
            }),
          ),
          inputTokensTotal: $.sum("$response.inputTokens"),
          outputTokensTotal: $.sum("$response.outputTokens"),
          avgExecutionTime: $.avg("$response.executionTime"),
        })),
        $sort({ _id: -1 }),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });

  it("should aggregate errors by model", async () => {
    const ResultSchema = S.Struct({
      _id: S.String,
      errorCount: S.Number,
      errors: S.Array(
        S.NullOr(
          S.Struct({
            category: S.String,
            message: S.String,
          }),
        ),
      ),
      avgExecutionTime: S.NullOr(S.Number),
      uniqueOrgs: S.Array(S.String),
    });

    const results = await dbRegistry(db)
      .callEvents.aggregate(
        $match($ => ({ $expr: $.ne("$response.error", null) })),
        $group($ => ({
          _id: "$modelId",
          errorCount: $.sum(1),
          errors: $.push("$response.error"),
          avgExecutionTime: $.avg("$response.executionTime"),
          uniqueOrgs: $.addToSet("$organizationId"),
        })),
        $sort({ errorCount: -1 }),
        $limit(20),
      )
      .toList();

    expect(results.length).toBeGreaterThan(0);
    assertSync(S.Array(ResultSchema), results);
    expectType<typeof ResultSchema.Type>({} as (typeof results)[number]);
  });
});
