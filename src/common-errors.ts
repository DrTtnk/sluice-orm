/**
 * Common error types and utilities for consistent error handling
 * across the MongoDB aggregation and CRUD operations.
 */

// Error types for type-level enforcement
export type CallbackOnlyError<Op extends string> = {
  __error: true;
  __tag: "CallbackOnlyError";
  __op: Op;
  message: `Operation '${Op}' requires a callback function, not a raw expression`;
};
