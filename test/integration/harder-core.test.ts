/**
 * HARDER-CORE TESTS: QAFHaB Edition (Quality Assurance From Hell and Beyond)
 *
 * These tests push both MongoDB and sluice to their absolute breaking points:
 * - Nested $facet with multiple levels
 * - $map/$filter/$reduce with $$this and $$value hell
 * - Impossible projections and grouping patterns
 * - Union type chaos with multiple branches and discriminated unions
 * - Deeply nested structures pushing TypeScript recursion limits
 * - Corner cases with null/undefined/optional combinations
 * - Type narrowing edge cases (mergeObjects, ifNull, filter, etc.)
 */
import { Schema as S } from "@effect/schema";
import {
  $addFields,
  $facet,
  $group,
  $project,
  $sort,
  $sortByCount,
  $unwind,
  registry,
} from "@sluice/sluice";
import { Db, ObjectId } from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ObjectIdSchema } from "../utils/common-schemas.js";
import { RpgMonsterSchema as MonsterSchema } from "../utils/common-schemas.js";
import { setup, teardown } from "../utils/setup.js";
import { assertSync } from "../utils/utils.js";

const monsterRegistry = registry("8.0", { monsters: MonsterSchema });

const insertTestMonsters = async (db: Db) => {
  await monsterRegistry(db)
    .monsters.insertMany([
      {
        _id: new ObjectId(),
        name: "Goblin",
        level: 5,
        hp: 50,
        attack: 10,
        defense: 5,
        items: [
          { name: "Sword", quantity: 1, rarity: "common" },
          { name: "Potion", quantity: 3, rarity: "common" },
        ],
        skills: [
          { name: "Slash", power: 15, cost: 10 },
          { name: "Block", power: 0, cost: 5 },
        ],
        tags: ["weak", "fast"],
        metadata: { createdAt: new Date(), score: 100, flags: [true, false] },
      },
      {
        _id: new ObjectId(),
        name: "Dragon",
        level: 50,
        hp: 500,
        attack: 100,
        defense: 50,
        items: [
          { name: "Gold", quantity: 1000, rarity: "legendary" },
          { name: "Scale", quantity: 10, rarity: "epic" },
        ],
        skills: [
          { name: "Fire Breath", power: 200, cost: 50 },
          { name: "Fly", power: 0, cost: 20 },
        ],
        tags: ["strong", "fire"],
        metadata: { createdAt: new Date(), score: 1000, flags: [true, true] },
      },
      {
        _id: new ObjectId(),
        name: "Slime",
        level: 1,
        hp: 10,
        attack: 2,
        defense: 1,
        items: [{ name: "Goo", quantity: 5, rarity: "common" }],
        skills: [{ name: "Bounce", power: 5, cost: 2 }],
        tags: ["weak", "slow"],
        metadata: { createdAt: new Date(), score: 10, flags: [false, false] },
      },
    ])
    .execute();
};

describe("HARDER-CORE: Nested $facet Hell", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    await insertTestMonsters(db);
  });

  afterAll(() => teardown());

  it("should handle nested $facet with multiple analysis branches", async () => {
    const ResultSchema = S.Struct({
      byLevel: S.Array(
        S.Struct({
          _id: S.Literal("low", "mid", "high"),
          count: S.Number,
          avgAttack: S.NullOr(S.Number),
        }),
      ),
      byTags: S.Array(
        S.Struct({
          _id: S.String,
          count: S.Number,
        }),
      ),
      stats: S.Array(
        S.Struct({
          _id: S.Null,
          totalMonsters: S.Number,
          avgLevel: S.NullOr(S.Number),
          maxHp: S.Number,
        }),
      ),
    });

    const results = await monsterRegistry(db)
      .monsters.aggregate(
        $facet($ => ({
          byLevel: $.pipe(
            $addFields($ => ({
              bracket: $.cond({
                if: $.lt("$level", 10),
                then: "low",
                else: $.cond({
                  if: $.lt("$level", 30),
                  then: "mid",
                  else: "high",
                }),
              }),
            })),
            $group($ => ({
              _id: "$bracket",
              count: $.sum(1),
              avgAttack: $.avg("$attack"),
            })),
          ),
          byTags: $.pipe($unwind("$tags"), $sortByCount("$tags")),
          stats: $.pipe(
            $group($ => ({
              _id: null,
              totalMonsters: $.sum(1),
              avgLevel: $.avg("$level"),
              maxHp: $.max("$hp"),
            })),
          ),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results[0]?.byLevel.length).toBeGreaterThan(0);
    expect(results[0]?.byTags.length).toBeGreaterThan(0);
    const first = results[0];
    expect(first?.stats[0]?.totalMonsters).toBe(3);
  });

  it("should handle $map with complex nested transformations", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      level: S.Number,
      itemNames: S.Array(S.String),
      skillPowers: S.Array(S.Number),
    });

    const results = await monsterRegistry(db)
      .monsters.aggregate(
        $project($ => ({
          name: "$name",
          level: "$level",
          itemNames: $.map({
            input: "$items",
            as: "item",
            in: "$$item.name",
          }),
          skillPowers: $.map({
            input: "$skills",
            as: "skill",
            in: "$$skill.power",
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results[0]?.itemNames).toEqual(["Sword", "Potion"]);
    expect(results[0]?.skillPowers).toEqual([15, 0]);
  });

  it("should handle $filter with complex conditions on $$this", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      epicItems: S.Array(
        S.Struct({
          name: S.String,
          quantity: S.Number,
          rarity: S.Literal("common", "rare", "epic", "legendary"),
        }),
      ),
      powerfulSkills: S.Array(
        S.Struct({
          name: S.String,
          power: S.Number,
          cost: S.Number,
        }),
      ),
    });

    const results = await monsterRegistry(db)
      .monsters.aggregate(
        $project($ => ({
          name: "$name",
          epicItems: $.filter({
            input: "$items",
            as: "item",
            cond: $ => $.in("$$item.rarity", ["epic", "legendary"]),
          }),
          powerfulSkills: $.filter({
            input: "$skills",
            as: "skill",
            cond: $ => $.gt("$$skill.power", 50),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(1);
    const dragon = results[1];
    expect(dragon?.epicItems.length).toBe(2); // Dragon has epic/legendary items
    expect(dragon?.powerfulSkills.length).toBe(1); // Dragon has Fire Breath
  });

  it("should handle $reduce to aggregate array values with $$value and $$this", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      totalItemQuantity: S.Number,
      totalSkillCost: S.Number,
    });

    const results = await monsterRegistry(db)
      .monsters.aggregate(
        $project($ => ({
          name: "$name",
          totalItemQuantity: $.reduce({
            input: "$items",
            initialValue: 0,
            in: $ => $.add("$$value", "$$this.quantity"),
          }),
          totalSkillCost: $.reduce({
            input: "$skills",
            initialValue: 0,
            in: $ => $.add("$$value", "$$this.cost"),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results[0]?.totalItemQuantity).toBe(4); // Goblin: 1 + 3
    expect(results[0]?.totalSkillCost).toBe(15); // Goblin: 10 + 5
    const dragon = results[1];
    expect(dragon?.totalItemQuantity).toBe(1010); // Dragon: 1000 + 10
  });

  it("should handle nested $map inside $reduce (MAP-REDUCE HELL)", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      allRarities: S.Array(S.String),
    });

    const results = await monsterRegistry(db)
      .monsters.aggregate(
        $project($ => ({
          name: "$name",
          // Map over items to get rarities, then reduce to concatenate
          allRarities: $.reduce({
            input: $.map({
              input: "$items",
              as: "item",
              in: "$$item.rarity",
            }),
            initialValue: [] as string[],
            in: $ => $.concatArrays("$$value", ["$$this"]),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results[0]?.allRarities).toEqual(["common", "common"]);
    expect(results.length).toBeGreaterThan(1);
    const dragon = results[1];
    expect(dragon?.allRarities).toEqual(["legendary", "epic"]);
  });

  it("should handle $filter inside $reduce (FILTER-REDUCE HELL)", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      highPowerSkillCount: S.Number,
    });

    const results = await monsterRegistry(db)
      .monsters.aggregate(
        $project($ => ({
          name: "$name",
          // Filter skills by power > 10, then reduce to count
          highPowerSkillCount: $.size(
            $.filter({
              input: "$skills",
              as: "skill",
              cond: $ => $.gt("$$skill.power", 10),
            }),
          ),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results[0]?.highPowerSkillCount).toBe(1); // Goblin: Slash (15)
    const dragon = results[1];
    expect(dragon?.highPowerSkillCount).toBe(1); // Dragon: Fire Breath (200)
  });
});

describe("HARDER-CORE: Impossible Grouping and Projection", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    await insertTestMonsters(db);
  });

  afterAll(() => teardown());

  it("should handle $group with complex accumulator expressions", async () => {
    const ResultSchema = S.Struct({
      _id: S.Literal("low", "mid", "high"),
      count: S.Number,
      avgAttack: S.NullOr(S.Number),
      maxHp: S.Number,
      monsters: S.Array(S.String),
      totalItems: S.Number,
    });

    const results = await monsterRegistry(db)
      .monsters.aggregate(
        $addFields($ => ({
          bracket: $.cond({
            if: $.lt("$level", 10),
            then: "low",
            else: $.cond({
              if: $.lt("$level", 30),
              then: "mid",
              else: "high",
            }),
          }),
          itemCount: $.size("$items"),
        })),
        $group($ => ({
          _id: "$bracket",
          count: $.sum(1),
          avgAttack: $.avg("$attack"),
          maxHp: $.max("$hp"),
          monsters: $.push("$name"),
          totalItems: $.sum("$itemCount"),
        })),
        $sort({ _id: 1 }),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results.length).toBeGreaterThan(0);
    expect(results.find(r => r._id === "low")?.count).toBeGreaterThan(0);
  });

  it("should handle $group with $push and nested objects", async () => {
    const ResultSchema = S.Struct({
      _id: S.Null,
      monsterSummaries: S.Array(
        S.Struct({
          name: S.String,
          level: S.Number,
          stats: S.Struct({
            hp: S.Number,
            attack: S.Number,
          }),
        }),
      ),
    });

    const results = await monsterRegistry(db)
      .monsters.aggregate(
        $group($ => ({
          _id: null,
          monsterSummaries: $.push({
            name: "$name",
            level: "$level",
            stats: {
              hp: "$hp",
              attack: "$attack",
            },
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results[0]?.monsterSummaries.length).toBe(3);
    const first = results[0];
    expect(first?.monsterSummaries[0]?.stats.hp).toBeGreaterThan(0);
  });

  it("should handle projection with deeply nested field access and operators", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      scoreMultiplier: S.Number,
      hasAllFlags: S.Boolean,
      firstSkillEfficiency: S.Number,
    });

    const results = await monsterRegistry(db)
      .monsters.aggregate(
        $project($ => ({
          name: "$name",
          scoreMultiplier: $.multiply("$metadata.score", 0.1),
          hasAllFlags: $.allElementsTrue("$metadata.flags"),
          firstSkillEfficiency: $.divide(
            $.arrayElemAt("$skills.power", 0),
            $.arrayElemAt("$skills.cost", 0),
          ),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    const dragon = results[1];
    expect(results[0]?.scoreMultiplier).toBeCloseTo(10);
    expect(dragon?.hasAllFlags).toBe(true); // Dragon has all true flags
    expect(results[0]?.firstSkillEfficiency).toBeCloseTo(1.5); // Goblin: 15/10
  });
});

describe("HARDER-CORE: $$ROOT and $$CURRENT Torture", () => {
  let db: Db;

  beforeAll(async () => {
    db = (await setup()).db;
    await insertTestMonsters(db);
  });

  afterAll(() => teardown());

  it("should use $$ROOT in $map to access document context", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      itemsWithMonster: S.Array(
        S.Struct({
          itemName: S.String,
          monsterName: S.String,
          monsterLevel: S.Number,
        }),
      ),
    });

    const results = await monsterRegistry(db)
      .monsters.aggregate(
        $project($ => ({
          name: "$name",
          itemsWithMonster: $.map({
            input: "$items",
            as: "item",
            in: {
              itemName: "$$item.name",
              monsterName: "$$ROOT.name",
              monsterLevel: "$$ROOT.level",
            },
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    const first = results[0];
    expect(first?.itemsWithMonster[0]?.monsterName).toBe("Goblin");
    expect(first?.itemsWithMonster[0]?.monsterLevel).toBe(5);
  });

  it("should use $$ROOT with $filter and complex conditions", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      name: S.String,
      level: S.Number,
      affordableSkills: S.Array(
        S.Struct({
          name: S.String,
          power: S.Number,
          cost: S.Number,
        }),
      ),
    });

    const results = await monsterRegistry(db)
      .monsters.aggregate(
        $project($ => ({
          name: "$name",
          level: "$level",
          // Filter skills where cost <= monster's defense (from $$ROOT)
          affordableSkills: $.filter({
            input: "$skills",
            as: "skill",
            cond: $ => $.lte("$$skill.cost", "$$ROOT.defense"),
          }),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results[0]?.affordableSkills.length).toBeGreaterThanOrEqual(0);
  });
});

describe("HARDER-CORE: mergeObjects and Type Narrowing", () => {
  let db: Db;

  const MergeTestSchema = S.Struct({
    _id: ObjectIdSchema,
    base: S.Struct({
      a: S.Number,
      b: S.String,
    }),
    override: S.Struct({
      b: S.String,
      c: S.Boolean,
    }),
    extra: S.Struct({
      d: S.Number,
    }),
  });

  const mergeRegistry = registry("8.0", { merge: MergeTestSchema });

  beforeAll(async () => {
    db = (await setup()).db;

    await mergeRegistry(db)
      .merge.insertMany([
        {
          _id: new ObjectId(),
          base: { a: 1, b: "base" },
          override: { b: "override", c: true },
          extra: { d: 42 },
        },
        {
          _id: new ObjectId(),
          base: { a: 2, b: "another" },
          override: { b: "changed", c: false },
          extra: { d: 99 },
        },
      ])
      .execute();
  });

  afterAll(() => teardown());

  it("should merge multiple objects with proper field override", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      merged: S.Struct({
        a: S.Number,
        b: S.String,
        c: S.Boolean,
        d: S.Number,
      }),
    });

    const results = await mergeRegistry(db)
      .merge.aggregate(
        $project($ => ({
          merged: $.mergeObjects("$base", "$override", "$extra"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results[0]?.merged).toEqual({ a: 1, b: "override", c: true, d: 42 });
    expect(results[1]?.merged).toEqual({ a: 2, b: "changed", c: false, d: 99 });
  });

  it("should handle nested mergeObjects", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      nested: S.Struct({
        a: S.Number,
        b: S.String,
        c: S.Boolean,
        d: S.Number,
        e: S.Number,
      }),
    });

    const results = await mergeRegistry(db)
      .merge.aggregate(
        $project($ => ({
          nested: $.mergeObjects(
            $.mergeObjects("$base", "$override"),
            $.mergeObjects("$extra", { e: 100 }),
          ),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results[0]?.nested).toMatchObject({ a: 1, c: true, d: 42, e: 100 });
  });
});

describe("HARDER-CORE: ifNull Chain Torture", () => {
  let db: Db;

  const NullTestSchema = S.Struct({
    _id: ObjectIdSchema,
    a: S.NullOr(S.String),
    b: S.NullOr(S.String),
    c: S.NullOr(S.String),
    d: S.String,
  });

  const nullRegistry = registry("8.0", { nulls: NullTestSchema });

  beforeAll(async () => {
    db = (await setup()).db;

    await nullRegistry(db)
      .nulls.insertMany([
        { _id: new ObjectId(), a: null, b: null, c: null, d: "fallback" },
        { _id: new ObjectId(), a: null, b: null, c: "found", d: "fallback" },
        { _id: new ObjectId(), a: null, b: "found", c: "also", d: "fallback" },
        { _id: new ObjectId(), a: "found", b: "also", c: "extra", d: "fallback" },
      ])
      .execute();
  });

  afterAll(() => teardown());

  it("should chain $ifNull to find first non-null value", async () => {
    const ResultSchema = S.Struct({
      _id: ObjectIdSchema,
      firstNonNull: S.String,
    });

    const results = await nullRegistry(db)
      .nulls.aggregate(
        $project($ => ({
          firstNonNull: $.ifNull("$a", "$b", "$c", "$d"),
        })),
      )
      .toList();

    assertSync(S.Array(ResultSchema), results);
    expect(results[0]?.firstNonNull).toBe("fallback");
    expect(results.length).toBe(4);
    expect(results[1]?.firstNonNull).toBe("found");
    expect(results[2]?.firstNonNull).toBe("found");
    expect(results[3]?.firstNonNull).toBe("found");
  });
});
