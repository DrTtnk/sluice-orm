import { Effect } from "effect";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { makeMongoDbClientLayer, registryEffect } from "../../src/registryEffect.js";
import { $match, $project } from "../../src/sluice-stages.js";
import { setup, teardown } from "../utils/setup.js";

describe("Effect Registry with Layer", () => {
  const UserSchema = {
    Type: null! as { _id: string; name: string; age: number; active: boolean },
  };

  beforeAll(async () => {
    await setup();
  });

  afterAll(async () => {
    await teardown();
  });

  test("should perform CRUD operations using Effect with Layer", async () => {
    const { client } = await setup();
    const mongoLayer = makeMongoDbClientLayer(client.db("effect_test_crud"));

    const program = Effect.gen(function* () {
      const registry = yield* registryEffect("8.0", { users: UserSchema });

      // Insert
      const insertResult = yield* registry.users
        .insertOne({ _id: "1", name: "Alice", age: 30, active: true })
        .execute();
      expect(insertResult.acknowledged).toBe(true);

      // Find
      const user = yield* registry.users.find(() => ({ _id: "1" })).toOne();
      expect(user).toMatchObject({ _id: "1", name: "Alice", age: 30, active: true });

      // Update
      const updateResult = yield* registry.users
        .updateOne(() => ({ _id: "1" }), { $set: { age: 31 } })
        .execute();
      expect(updateResult.modifiedCount).toBe(1);

      // Verify update
      const updatedUser = yield* registry.users.find(() => ({ _id: "1" })).toOne();
      expect(updatedUser?.age).toBe(31);

      // Delete
      const deleteResult = yield* registry.users.deleteOne(() => ({ _id: "1" })).execute();
      expect(deleteResult.deletedCount).toBe(1);

      return "success";
    });

    const result = await Effect.runPromise(program.pipe(Effect.provide(mongoLayer)));
    expect(result).toBe("success");
  });

  test("should perform aggregation pipeline using Effect with Layer", async () => {
    const { client } = await setup();
    const mongoLayer = makeMongoDbClientLayer(client.db("effect_test_agg"));

    const program = Effect.gen(function* () {
      const registry = yield* registryEffect("8.0", { users: UserSchema });

      // Insert test data
      yield* registry.users
        .insertMany([
          { _id: "u1", name: "Alice", age: 30, active: true },
          { _id: "u2", name: "Bob", age: 25, active: true },
          { _id: "u3", name: "Carol", age: 35, active: false },
        ])
        .execute();

      // Aggregate
      const results = yield* registry.users
        .aggregate(
          $match(() => ({ active: true })),
          $project($ => ({ name: $.include, age: $.include, _id: $.exclude })),
        )
        .toList();

      expect(results).toHaveLength(2);
      expect(results).toEqual(
        expect.arrayContaining([
          { name: "Alice", age: 30 },
          { name: "Bob", age: 25 },
        ]),
      );

      return results;
    });

    await Effect.runPromise(program.pipe(Effect.provide(mongoLayer)));
  });

  test("should compose Effects with dependency injection", async () => {
    const { client } = await setup();
    const mongoLayer = makeMongoDbClientLayer(client.db("effect_test_compose"));

    const program = Effect.gen(function* () {
      const registry = yield* registryEffect("8.0", { users: UserSchema });

      yield* registry.users
        .insertOne({ _id: "compose1", name: "Dave", age: 40, active: true })
        .execute();

      const user = yield* registry.users.find(() => ({ _id: "compose1" })).toOne();

      yield* registry.users.updateOne(() => ({ _id: "compose1" }), { $inc: { age: 1 } }).execute();

      const updatedUser = yield* registry.users.find(() => ({ _id: "compose1" })).toOne();

      return { before: user?.age, after: updatedUser?.age };
    });

    const result = await Effect.runPromise(program.pipe(Effect.provide(mongoLayer)));
    expect(result).toEqual({ before: 40, after: 41 });
  });

  test("should handle errors with tagged MongoError", async () => {
    const { client } = await setup();
    await client.close();
    const mongoLayer = makeMongoDbClientLayer(client.db("closed_db"));

    const program = Effect.gen(function* () {
      const registry = yield* registryEffect("8.0", { users: UserSchema });
      return yield* registry.users.find(() => ({ _id: "nonexistent" })).toOne();
    });

    const result = await Effect.runPromise(program.pipe(Effect.provide(mongoLayer), Effect.either));

    expect(result._tag).toBe("Left");
    if (result._tag === "Left") {
      expect(result.left._tag).toBe("MongoError");
      expect(result.left.operation).toBe("find.toOne");
    }
  });
});
