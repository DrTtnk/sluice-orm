import { Effect } from "effect";
import { Binary, Decimal128, Long, type Db } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  $group,
  $match,
  $project,
  $set,
  makeMongoDbClientLayer,
  registry,
  registryEffect,
} from "../../src/index.js";
import { setup, teardown } from "../utils/setup.js";

const schemas = {
  reviewUsers: { Type: null! as { _id: string; name: string; age: number; tags: string[] } },
};
describe("Review regressions against MongoDB", () => {
  let db: Db;
  beforeAll(async () => {
    ({ db } = await setup());
    await registry(
      "8.0",
      schemas,
    )(db)
      .reviewUsers.insertMany([
        { _id: "r1", name: "Alice", age: 30, tags: ["a", "b"] },
        { _id: "r2", name: "Bob", age: 15, tags: ["b"] },
      ])
      .execute();
  });
  afterAll(teardown);

  it("resolves CRUD expression callbacks", async () => {
    const users = registry("8.0", schemas)(db).reviewUsers;
    const adults = await users.find($ => ({ $expr: $.gt("$age", 18) })).toList();
    expect(adults.map(user => user.name)).toEqual(["Alice"]);
  });

  it("keeps remaining fields in exclusion projections and replaces field types", async () => {
    const users = registry("8.0", schemas)(db).reviewUsers;
    const excluded = await users
      .aggregate(
        $match(() => ({ _id: "r1" })),
        $project(() => ({ name: 0 })),
      )
      .toList();
    expect(excluded).toEqual([{ _id: "r1", age: 30, tags: ["a", "b"] }]);
    const replaced = await users
      .aggregate(
        $match(() => ({ _id: "r1" })),
        $set($ => ({ age: $.toString("$age") })),
      )
      .toList();
    expect(replaced[0]?.age).toBe("30");
  });

  it("supports immutable aggregation chaining", async () => {
    const users = registry("8.0", schemas)(db).reviewUsers;
    const base = users.aggregate($match(() => ({})));
    const grouped = base.pipe($group($ => ({ _id: null, count: $.sum(1) })));
    expect(base.stages).toHaveLength(1);
    expect(await grouped.toList()).toEqual([{ _id: null, count: 2 }]);
  });

  it("preserves BSON literals and embedded binary buffers", async () => {
    const users = registry("8.0", schemas)(db).reviewUsers;
    const result = await users
      .aggregate(
        $match(() => ({ _id: "r1" })),
        $project($ => ({
          _id: 0,
          decimal: $.literal(Decimal128.fromString("12.34")),
          long: $.literal(Long.fromString("9007199254740993")),
          binary: $.literal(new Binary(Buffer.from([1, 2, 3]))),
        })),
      )
      .toList();
    expect(result[0]?.decimal.toString()).toBe("12.34");
    expect(result[0]?.long.toString()).toBe("9007199254740993");
    expect(result[0]?.binary).toBeInstanceOf(Binary);
  });

  it("uses the same typed queries through Effect", async () => {
    const program = Effect.gen(function* () {
      const { reviewUsers: users } = yield* registryEffect("8.0", schemas);
      const adults = yield* users
        .find($ => ({ $expr: $.gt("$age", 18) }), { projection: { name: 1, _id: 0 } })
        .toList();
      const counts = yield* users
        .aggregate($match(() => ({})))
        .pipe($group($ => ({ _id: null, count: $.sum(1) })))
        .toList();
      return { adults, counts };
    });
    expect(
      await Effect.runPromise(program.pipe(Effect.provide(makeMongoDbClientLayer(db)))),
    ).toEqual({ adults: [{ name: "Alice" }], counts: [{ _id: null, count: 2 }] });
  });
});
