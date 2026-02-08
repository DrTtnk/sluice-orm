/**
 * Shared setup for hardcore tests
 * Defines collections, schemas, and common test data
 */
import { Schema as S } from "@effect/schema";
import { Db, ObjectId } from "mongodb";

import { registry } from "../../../src/sluice.js";
import type { SimplifyWritable } from "../../../src/type-utils.js";
import { ObjectIdSchema } from "../../utils/common-schemas.js";

// ============================================
// MONSTER SCHEMA: Deeply nested, union types, optionals, nullables
// ============================================

export const MonsterSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  score: S.Number,
  active: S.Boolean,
  createdAt: S.Date,
  deletedAt: S.NullOr(S.Date),
  legacyScore: S.NullOr(S.Number),
  description: S.optional(S.String),
  priority: S.optional(S.Number),
  status: S.Literal("draft", "published", "archived"),
  level: S.Literal(1, 2, 3, 4, 5),
  metadata: S.Struct({
    version: S.String,
    flags: S.Array(S.Boolean),
    counts: S.Struct({
      views: S.Number,
      likes: S.Number,
      shares: S.Number,
    }),
    audit: S.NullOr(
      S.Struct({
        lastModifiedBy: S.String,
        lastModifiedAt: S.Date,
      }),
    ),
  }),
  tags: S.Array(S.String),
  scores: S.Array(S.Number),
  items: S.Array(
    S.Struct({
      id: S.String,
      name: S.String,
      price: S.Number,
      quantity: S.Number,
      discounts: S.Array(
        S.Struct({
          code: S.String,
          percent: S.Number,
          validUntil: S.Date,
        }),
      ),
    }),
  ),
  coords: S.Tuple(S.Number, S.Number),
});

export type Monster = typeof MonsterSchema.Type;

export const monster = registry("8.0", { monsters: MonsterSchema });

// Predefined ObjectIds for consistent testing
export const m1Id = new ObjectId("507f1f77bcf86cd799439011");
export const m2Id = new ObjectId("507f1f77bcf86cd799439012");
export const m3Id = new ObjectId("507f1f77bcf86cd799439013");

export const monsterTestData: SimplifyWritable<Monster>[] = [
  {
    _id: m1Id,
    name: "Blob",
    score: 10,
    active: true,
    createdAt: new Date("2023-01-01"),
    deletedAt: null,
    legacyScore: 50,
    status: "draft",
    level: 1,
    metadata: {
      version: "v1",
      flags: [true],
      counts: {
        views: 100,
        likes: 10,
        shares: 0,
      },
      audit: null,
    },
    tags: ["slime", "green"],
    scores: [10, 20, 15],
    items: [
      {
        id: "i1",
        name: "goo",
        price: 10,
        quantity: 5,
        discounts: [],
      },
    ],
    coords: [0, 0] as [number, number],
  },
  {
    _id: m2Id,
    name: "Dragon",
    score: 90,
    active: true,
    createdAt: new Date("2023-01-01"),
    deletedAt: null,
    legacyScore: null,
    status: "published",
    level: 5,
    metadata: {
      version: "v1",
      flags: [true, false],
      counts: {
        views: 5000,
        likes: 1000,
        shares: 500,
      },
      audit: null,
    },
    tags: ["fire", "flying"],
    scores: [90, 95, 88],
    items: [
      {
        id: "i2",
        name: "gold",
        price: 1000,
        quantity: 10,
        discounts: [
          {
            code: "SAVE10",
            percent: 10,
            validUntil: new Date(),
          },
        ],
      },
    ],
    coords: [100, 100] as [number, number],
  },
  {
    _id: m3Id,
    name: "Ghost",
    score: 40,
    active: false,
    createdAt: new Date("2022-01-01"),
    deletedAt: new Date("2023-01-01"),
    legacyScore: 20,
    status: "archived",
    level: 3,
    metadata: {
      version: "v2",
      flags: [],
      counts: {
        views: 50,
        likes: 5,
        shares: 1,
      },
      audit: null,
    },
    tags: ["undead"],
    scores: [40, 35, 42],
    items: [],
    coords: [10, 10] as [number, number],
  },
];

export async function seedMonsters(db: Db) {
  await monster(db).monsters.insertMany(monsterTestData).execute();
}
