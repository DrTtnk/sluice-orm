declare const opaqueErrorBrand: unique symbol;

/**
 * Opaque error type that carries a message but hides implementation details
 */

export type OpaqueError<Msg extends string> = {
  readonly __error: Msg;
  readonly __tag: "OpaqueError";
  readonly [opaqueErrorBrand]: never;
};

/**
 * Type error with structured details: what went wrong, expected type, got type
 */
export type TypeError<Msg extends string, Expected = unknown, Got = unknown> = OpaqueError<Msg> & {
  readonly __expected: Expected;
  readonly __got: Got;
  readonly __tag: "TypeError";
};

/**
 * Invalid field reference error - field does not exist in context
 */
export type InvalidFieldError<Field extends string, Context = unknown> = TypeError<
  `Invalid field: "${Field}" does not exist`,
  keyof Context,
  Field
>;

/**
 * Type mismatch error - got wrong type for an operator
 */
export type TypeMismatchError<
  Operator extends string,
  Expected extends string,
  Got extends string,
> = TypeError<`${Operator}: expected ${Expected}, got ${Got}`, Expected, Got>;

/**
 * Numeric type error - expected number but got something else
 */
export type NumericTypeError<Got extends string> = TypeMismatchError<
  "$numeric",
  "number | numeric field ref",
  Got
>;

/**
 * String type error - expected string but got something else
 */
export type StringTypeError<Got extends string> = TypeMismatchError<
  "$string",
  "string | string field ref",
  Got
>;

/**
 * Array type error - expected array but got something else
 */
export type ArrayTypeError<Got extends string> = TypeMismatchError<
  "$array",
  "array | array field ref",
  Got
>;

/**
 * Boolean type error - expected boolean but got something else
 */
export type BooleanTypeError<Got extends string> = TypeMismatchError<
  "$boolean",
  "boolean | boolean field ref",
  Got
>;

/**
 * Date type error - expected Date but got something else
 */
export type DateTypeError<Got extends string> = TypeMismatchError<
  "$date",
  "Date | date field ref",
  Got
>;

// ==========================================
// Update Operation Errors
// ==========================================

/**
 * Invalid update path error - path does not exist in document
 */
export type InvalidUpdatePathError<Path extends string> =
  OpaqueError<`Invalid update path: "${Path}" does not exist in document`> & {
    readonly __path: Path;
    readonly __tag: "InvalidUpdatePathError";
  };

/**
 * Update type mismatch error - value doesn't match path type
 */
export type UpdateTypeMismatchError<Path extends string, Expected, Got> = TypeError<
  `$set: path "${Path}" expects type ${Expected & string}, got ${Got & string}`,
  Expected,
  Got
> & {
  readonly __path: Path;
  readonly __tag: "UpdateTypeMismatchError";
};

/**
 * Non-numeric path error for $inc/$mul operators
 */
export type NonNumericPathError<Path extends string, ActualType> = TypeError<
  `$inc/$mul: path "${Path}" must be numeric, got ${ActualType & string}`,
  number,
  ActualType
> & {
  readonly __path: Path;
  readonly __tag: "NonNumericPathError";
};

/**
 * Non-array path error for array operators
 */
export type NonArrayPathError<Operator extends string, Path extends string, ActualType> = TypeError<
  `${Operator}: path "${Path}" must be an array, got ${ActualType & string}`,
  "Array<T>",
  ActualType
> & {
  readonly __path: Path;
  readonly __op: Operator;
  readonly __tag: "NonArrayPathError";
};

/**
 * Array element type mismatch for $push/$addToSet
 */
export type ArrayElementTypeMismatchError<Path extends string, Expected, Got> = TypeError<
  `$push/$addToSet: array at "${Path}" expects elements of type ${Expected & string}, got ${Got & string}`,
  Expected,
  Got
> & {
  readonly __path: Path;
  readonly __tag: "ArrayElementTypeMismatchError";
};
