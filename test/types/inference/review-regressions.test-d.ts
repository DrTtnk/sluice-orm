import { Effect } from "effect";
import type { Db } from "mongodb";
import { expectType } from "tsd";

import { registry, registryEffect, $group, $match, $project, $set } from "../../../src/index.js";

type User = { _id: string; name: string; age: number; tags: string[]; nickname?: string };
const schema = { Type: null! as User };
declare const db: Db;
const users = registry("8.0", { users: schema })(db).users;

expectType<Promise<Omit<User, "name">[]>>(
  users.aggregate($project($ => ({ name: $.exclude }))).toList(),
);
expectType<Promise<Omit<User, "_id">[]>>(
  users.aggregate($project($ => ({ _id: $.exclude }))).toList(),
);
expectType<Promise<{ name: string }[]>>(
  users.aggregate($project($ => ({ name: $.include, _id: $.exclude }))).toList(),
);
expectType<Promise<string[]>>(users.distinct("tags").execute());
expectType<Promise<{ name: string }[]>>(
  users.find(() => ({}), { projection: { name: 1, _id: 0 } }).toList(),
);
expectType<Promise<Omit<User, "name"> | null>>(
  users.findOne(() => ({}), { projection: { name: 0 } }).toOne(),
);
expectType<
  Promise<{ _id: string; name: string; age: string; tags: string[]; nickname?: string }[]>
>(users.aggregate($set($ => ({ age: $.toString("$age") }))).toList());
expectType<Promise<{ _id: null; count: number }[]>>(
  users
    .aggregate($match(() => ({})))
    .pipe($group($ => ({ _id: null, count: $.sum(1) })))
    .toList(),
);
users.find($ => ({ $expr: $.gt("$age", 18) }));
users.updateOne(
  () => ({}),
  $ => $.pipe($.unset("nickname")),
);
users.updateMany(
  () => ({}),
  $ =>
    $.pipe(
      $.set($ => ({ temporary: $.toString("$age") })),
      $.unset("temporary"),
    ),
);
users.updateOne(
  () => ({}),
  // @ts-expect-error - a required field cannot disappear from a schema-preserving update
  $ => $.pipe($.unset("name")),
);
users.updateMany(
  () => ({}),
  // @ts-expect-error - a schema-preserving update cannot change age to a string
  $ => $.pipe($.set($ => ({ age: $.toString("$age") }))),
);
// @ts-expect-error - exclusion cannot be combined with computed fields
users.aggregate($project($ => ({ name: 0, computed: $.add("$age", 1) })));
// @ts-expect-error - invalid mixed CRUD projection
users.find(() => ({}), { projection: { name: 0, age: 1 } });

Effect.gen(function* () {
  const r = yield* registryEffect("8.0", { users: schema });
  expectType<
    Effect.Effect<
      { _id: null; count: number }[],
      import("../../../src/registryEffect.js").MongoError
    >
  >(r.users.aggregate($group($ => ({ _id: null, count: $.sum(1) }))).toList());
  const grouped = yield* r.users
    .aggregate($match(() => ({})))
    .pipe($group($ => ({ _id: null, count: $.sum(1) })))
    .toList();
  expectType<{ _id: null; count: number }[]>(grouped);
  const projected = yield* r.users.find(() => ({}), { projection: { name: 1, _id: 0 } }).toList();
  expectType<{ name: string }[]>(projected);
  const tags = yield* r.users.distinct("tags").execute();
  expectType<string[]>(tags);
  r.users.find($ => ({ $expr: $.gt("$age", 18) }));
  r.users.updateOne(
    () => ({}),
    $ => $.pipe($.set($ => ({ age: $.add("$age", 1) }))),
  );
  // @ts-expect-error - aggregate accepts typed stages, not arbitrary values
  r.users.aggregate(123);
  // @ts-expect-error - numeric updates cannot target string fields
  r.users.updateOne(() => ({}), { $inc: { name: 3 } });
  r.users.updateOne(
    () => ({}),
    // @ts-expect-error - Effect update pipelines preserve the collection schema too
    $ => $.pipe($.unset("name")),
  );
  // @ts-expect-error - missing arrayFilters is rejected in the Effect API
  r.users.updateOne(() => ({}), { $set: { "tags.$[tag]": "x" } });
});

expectType<Promise<{ name: string } | null>>(
  users.findOneAndDelete(() => ({}), { projection: { name: 1, _id: 0 } }).execute(),
);
expectType<Promise<{ name: string } | null>>(
  users
    .findOneAndReplace(
      () => ({}),
      { _id: "u1", name: "Alice", age: 30, tags: [] },
      { projection: { name: 1, _id: 0 } },
    )
    .execute(),
);
expectType<Promise<{ name: string } | null>>(
  users
    .findOneAndUpdate(
      () => ({}),
      { $inc: { age: 1 } },
      { projection: { name: 1, _id: 0 }, returnDocument: "after" },
    )
    .execute(),
);

expectType<Promise<Omit<User, "name">[]>>(
  users.aggregate($project(() => ({ name: 0, _id: 1 }))).toList(),
);
expectType<Promise<Omit<User, "name">[]>>(
  users.find(() => ({}), { projection: { name: 0, _id: 1 } }).toList(),
);
expectType<Promise<{ _id: number }[]>>(users.aggregate($project($ => ({ _id: "$age" }))).toList());
declare const dynamicOptions: import("../../../src/crud.js").FindOptions<User>;
expectType<Promise<Partial<User>[]>>(users.find(() => ({}), dynamicOptions).toList());
expectType<Promise<User[]>>(users.find(() => ({})).toList());
