import { Schema as S } from "@effect/schema";
import { ObjectId } from "mongodb";

/** Effect Schema for ObjectId — test-only, not part of the public API */
export const ObjectIdSchema = S.instanceOf(ObjectId);

// ============================================
// COMPLEX MONSTER SCHEMA: Deeply nested, union types, optionals, nullables
// Used in hardcore.test.ts
// ============================================

export const ComplexMonsterSchema = S.Struct({
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

// ============================================
// RPG MONSTER SCHEMA: Stats, Inventory
// Used in harder-core.test.ts
// ============================================

export const RpgMonsterSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  level: S.Number,
  hp: S.Number,
  attack: S.Number,
  defense: S.Number,
  items: S.Array(
    S.Struct({
      name: S.String,
      quantity: S.Number,
      rarity: S.Literal("common", "rare", "epic", "legendary"),
    }),
  ),
  skills: S.Array(
    S.Struct({
      name: S.String,
      power: S.Number,
      cost: S.Number,
    }),
  ),
  tags: S.Array(S.String),
  metadata: S.Struct({
    createdAt: S.Date,
    score: S.Number,
    flags: S.Array(S.Boolean),
  }),
});

// ============================================
// BASIC SCHEMAS
// Used in basic/basic.test.ts and general usage
// ============================================

export const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
  scores: S.Array(S.Number),
  tags: S.Array(S.String),
  active: S.Boolean,
  addresses: S.Array(
    S.Struct({
      city: S.String,
      zip: S.Number,
    }),
  ),
});

export const OrderSchema = S.Struct({
  _id: ObjectIdSchema,
  userId: ObjectIdSchema,
  items: S.Array(
    S.Struct({
      productId: ObjectIdSchema,
      quantity: S.Number,
      price: S.Number,
    }),
  ),
  orderDate: S.Date,
  status: S.String,
  // Removed unused fields: total, date
});

export const ProductSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  sku: S.String,
  categoryId: ObjectIdSchema,
  price: S.Number,
  cost: S.Number,
  stock: S.Number,
  tags: S.Array(S.String),
  attributes: S.Array(
    S.Struct({
      key: S.String,
      value: S.String,
    }),
  ),
  reviews: S.Array(
    S.Struct({
      userId: ObjectIdSchema,
      rating: S.Number,
      comment: S.String,
      date: S.Date,
    }),
  ),
});

export const CategorySchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  parentId: S.optional(S.NullOr(ObjectIdSchema)),
  level: S.Number,
  path: S.Array(S.String),
});
