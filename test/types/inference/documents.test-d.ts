/**
 * Type tests for $documents stage
 *
 * MongoDB Documentation: https://www.mongodb.com/docs/manual/reference/operator/aggregation/documents/
 */

import { $documents, type Agg } from "../../../src/sluice.js";

// ============================================
// $documents Tests
// ============================================

// Create pipeline from literal documents
const literalDocs = $documents([{ x: 10 }, { x: 2 }, { x: 5 }]);
// $documents infers type from the array elements
literalDocs satisfies Agg<unknown, { readonly x: number }>;

// Complex document structure
const complexDocs = $documents([
  {
    name: "Alice",
    age: 30,
    active: true,
  },
  {
    name: "Bob",
    age: 25,
    active: false,
  },
  {
    name: "Charlie",
    age: 35,
    active: true,
  },
]);
complexDocs satisfies Agg<
  unknown,
  { readonly name: string; readonly age: number; readonly active: boolean }
>;

// Lookup table pattern
const currencies = $documents([
  {
    code: "USD",
    symbol: "$",
    name: "US Dollar",
  },
  {
    code: "EUR",
    symbol: "€",
    name: "Euro",
  },
  {
    code: "GBP",
    symbol: "£",
    name: "Pound Sterling",
  },
]);
currencies satisfies Agg<
  unknown,
  { readonly code: string; readonly symbol: string; readonly name: string }
>;
