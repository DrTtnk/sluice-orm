/**
 * JSON Schema Type Validation Tests
 *
 * Tests for $jsonSchema query operator type checking.
 *
 * CURRENT STATUS:
 * - Basic JSON schema structure is validated (type must be string/array, properties must be object)
 * - Schema properties are NOT validated against the collection schema
 *
 * KNOWN GAP (Bonus Feature):
 * - $jsonSchema should validate that its properties intersect with the collection schema
 * - Extra properties not in collection schema should error
 * - Schema evolution should follow the matched schema, or never/OpaqueError if can't match
 *
 * WHY THIS IS HARD:
 * - JSON Schema and TypeScript types don't map 1:1 (JSON Schema supports union, allOf, anyOf, etc.)
 * - Would need to convert Effect Schema type to JSON Schema type at the type level
 * - Complex recursive type mapping between different type systems
 */
import { Schema as S } from "@effect/schema";
import { ObjectId as _ObjectId } from "bson";
import type { Db } from "mongodb";

import { $match, collection } from "../../../src/sluice.js";

const ObjectIdSchema = S.instanceOf(_ObjectId);

const UserSchema = S.Struct({
  _id: ObjectIdSchema,
  name: S.String,
  age: S.Number,
});

const mockDb = {} as Db;
const users = collection("users", UserSchema, mockDb.collection("users"));

// Valid $jsonSchema - properties match collection schema
{
  const res = users
    .aggregate(
      $match(() => ({
        $jsonSchema: {
          type: "object",
          properties: {
            age: { type: "number" },
            name: { type: "string" },
          },
          required: ["age"],
        },
      })),
    )
    .toList();
}

// KNOWN GAP: Invalid $jsonSchema with extra properties
// This SHOULD error but currently DOESN'T validate against collection schema
// TODO: Implement intersection validation for schema safety
{
  const res = users
    .aggregate(
      $match(() => ({
        // This passes but should fail - extraProp doesn't exist on UserSchema
        $jsonSchema: {
          type: "object",
          properties: {
            age: { type: "number" },
            name: { type: "string" },
            extraProp: { type: "string" }, // Should error: not in collection
          },
          required: ["age"],
        },
      })),
    )
    .toList();
}

// KNOWN GAP: JSON Schema with incompatible types
// This SHOULD error but currently DOESN'T validate types
{
  const res = users
    .aggregate(
      $match(() => ({
        // This passes but should fail - age is number, not string
        $jsonSchema: {
          type: "object",
          properties: {
            age: { type: "string" }, // Should error: age is number
          },
        },
      })),
    )
    .toList();
}

// Invalid $jsonSchema - type must be string or array
void users
  .aggregate(
    $match(() => ({
      $jsonSchema: {
        // @ts-expect-error invalid json schema type
        type: 123,
      },
    })),
  )
  .toList();

// Invalid $jsonSchema - properties must be an object
void users
  .aggregate(
    $match(() => ({
      $jsonSchema: {
        type: "object",
        // @ts-expect-error invalid json schema properties
        properties: 123,
      },
    })),
  )
  .toList();
