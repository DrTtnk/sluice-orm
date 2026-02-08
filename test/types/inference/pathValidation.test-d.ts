/* eslint-disable @typescript-eslint/no-empty-object-type */
/**
 * Path validation tests - ensure paths are MongoDB-compliant
 * Tests that PathType<T> generates ONLY valid MongoDB paths
 * and specifically excludes paths into ObjectId and other BSON types
 */

import type { ObjectId } from "mongodb";
import { expectType } from "tsd";
import * as tf from "type-fest";

import type { PathType, ResolveUpdatePath, UpdatePathType } from "../../../src/paths.js";
import type { OpaqueError } from "../../../src/type-errors.js";

// Test type
type User = {
  _id: ObjectId;
  createdAt: Date;
  name: string;
  email: string;
  age: number;
  tags: string[];
  matrixMetadata: { cell: number }[][][];
  profile: {
    bio: string;
    avatar: string;
  };
  orders: {
    orderId: ObjectId;
    total: number;
    items: string[];
  }[];
};

// Valid paths that SHOULD be included
type ValidPaths = PathType<User>;

type ExpectedValidPaths =
  | "_id"
  | "createdAt"
  | "name"
  | "email"
  | "age"
  | "matrixMetadata"
  | "profile"
  | "tags"
  | `tags.${number}`
  | "orders"
  | `orders.${number}`
  | `orders.${number}.orderId`
  | `orders.${number}.total`
  | `orders.${number}.items`
  | `orders.${number}.items.${number}`
  | "orders.orderId"
  | "orders.total"
  | "orders.items"
  | `orders.items.${number}`
  | `matrixMetadata.${number}`
  | `matrixMetadata.${number}.${number}`
  | `matrixMetadata.${number}.${number}.${number}`
  | `matrixMetadata.${number}.${number}.${number}.cell`
  | `matrixMetadata.${number}.${number}.cell`
  | `matrixMetadata.${number}.cell`
  | "matrixMetadata.cell"
  | "profile.bio"
  | "profile.avatar";

// Test that direct fields are accessible
expectType<ValidPaths>({} as ExpectedValidPaths);

type ValidUpdatePaths = UpdatePathType<User>;

type Pos = number | "$" | "$[]" | `$[${string}]`;

type ExpectedValidUpdatePaths =
  // --- Top Level Primitives ---
  | "_id"
  | "createdAt"
  | "name"
  | "email"
  | "age"

  // --- Profile (Nested Object) ---
  | "profile"
  | "profile.bio"
  | "profile.avatar"

  // --- Tags (Simple Array) ---
  | "tags"
  | `tags.${Pos}`

  // --- Orders (Array of Objects) ---
  | "orders"
  | `orders.${Pos}`
  | `orders.${Pos}.orderId`
  | `orders.${Pos}.total`
  | `orders.${Pos}.items`
  | `orders.${Pos}.items.${Pos}`

  // --- Matrix Metadata (3D Nested Array) ---
  | "matrixMetadata"
  | `matrixMetadata.${Pos}`
  | `matrixMetadata.${Pos}.${Pos}`
  | `matrixMetadata.${Pos}.${Pos}.${Pos}`
  | `matrixMetadata.${Pos}.${Pos}.${Pos}.cell`;

// Strict equality check replaced with diff checks below.

type MissingUpdatePaths = Exclude<ExpectedValidUpdatePaths, ValidUpdatePaths>;
type ExtraUpdatePaths = Exclude<ValidUpdatePaths, ExpectedValidUpdatePaths>;

expectType<never>({} as MissingUpdatePaths);
expectType<never>({} as ExtraUpdatePaths);

{
  type ResolveUser<S extends ExpectedValidUpdatePaths> = ResolveUpdatePath<User, S>;

  expectType<User["_id"]>({} as ResolveUser<"_id">);

  // Top Level Primitives
  expectType<string>({} as ResolveUser<"name">);
  expectType<string>({} as ResolveUser<"email">);
  expectType<number>({} as ResolveUser<"age">);
  expectType<Date>({} as ResolveUser<"createdAt">);

  // Profile
  expectType<User["profile"]>({} as ResolveUser<"profile">);
  expectType<string>({} as ResolveUser<"profile.bio">);
  expectType<string>({} as ResolveUser<"profile.avatar">);

  // Tags
  expectType<string>({} as ResolveUser<"tags.0">);
  expectType<string>({} as ResolveUser<"tags.$">);
  expectType<string>({} as ResolveUser<"tags.$[elem]">);

  // Orders
  expectType<User["orders"]>({} as ResolveUser<"orders">);
  expectType<User["orders"][0]>({} as ResolveUser<"orders.0">);
  expectType<User["orders"][0]>({} as ResolveUser<"orders.$">);
  expectType<User["orders"][0]>({} as ResolveUser<"orders.$[elem]">);
  expectType<ObjectId>({} as ResolveUser<"orders.0.orderId">);
  expectType<ObjectId>({} as ResolveUser<"orders.$.orderId">);
  expectType<ObjectId>({} as ResolveUser<"orders.$[elem].orderId">);
  expectType<number>({} as ResolveUser<"orders.0.total">);
  expectType<number>({} as ResolveUser<"orders.$.total">);
  expectType<number>({} as ResolveUser<"orders.$[elem].total">);
  expectType<string[]>({} as ResolveUser<"orders.0.items">);
  expectType<string[]>({} as ResolveUser<"orders.$.items">);
  expectType<string[]>({} as ResolveUser<"orders.$[elem].items">);
  expectType<string>({} as ResolveUser<"orders.0.items.0">);
  expectType<string>({} as ResolveUser<"orders.$.items.0">);
  expectType<string>({} as ResolveUser<"orders.$.items.$">);
  expectType<string>({} as ResolveUser<"orders.$.items.$[elem]">);
  expectType<string>({} as ResolveUser<"orders.$[elem].items.0">);
  expectType<string>({} as ResolveUser<"orders.$[elem].items.$">);
  expectType<string>({} as ResolveUser<"orders.$[elem].items.$[elem]">);

  // Matrix Metadata
  expectType<User["matrixMetadata"]>({} as ResolveUser<"matrixMetadata">);
  expectType<User["matrixMetadata"][0]>({} as ResolveUser<"matrixMetadata.0">);
  expectType<User["matrixMetadata"][0]>({} as ResolveUser<"matrixMetadata.$">);
  expectType<User["matrixMetadata"][0]>({} as ResolveUser<"matrixMetadata.$[elem]">);
  expectType<User["matrixMetadata"][0][0]>({} as ResolveUser<"matrixMetadata.0.0">);
  expectType<User["matrixMetadata"][0][0]>({} as ResolveUser<"matrixMetadata.$.0">);
  expectType<User["matrixMetadata"][0][0]>({} as ResolveUser<"matrixMetadata.$.$">);
  expectType<User["matrixMetadata"][0][0]>({} as ResolveUser<"matrixMetadata.$.$[elem]">);
  expectType<User["matrixMetadata"][0][0]>({} as ResolveUser<"matrixMetadata.$[elem].0">);
  expectType<User["matrixMetadata"][0][0]>({} as ResolveUser<"matrixMetadata.$[elem].$">);
  expectType<User["matrixMetadata"][0][0]>({} as ResolveUser<"matrixMetadata.$[elem].$[elem]">);
  expectType<User["matrixMetadata"][0][0][0]>({} as ResolveUser<"matrixMetadata.0.0.0">);
  expectType<User["matrixMetadata"][0][0][0]>({} as ResolveUser<"matrixMetadata.$.0.0">);
  expectType<User["matrixMetadata"][0][0][0]>({} as ResolveUser<"matrixMetadata.$.$.0">);
  expectType<User["matrixMetadata"][0][0][0]>({} as ResolveUser<"matrixMetadata.$.$.$">);
  expectType<User["matrixMetadata"][0][0][0]>({} as ResolveUser<"matrixMetadata.$.$.$[elem]">);
  expectType<User["matrixMetadata"][0][0][0]>({} as ResolveUser<"matrixMetadata.$[elem].0.0">);
  expectType<User["matrixMetadata"][0][0][0]>({} as ResolveUser<"matrixMetadata.$[elem].$.0">);
  expectType<User["matrixMetadata"][0][0][0]>({} as ResolveUser<"matrixMetadata.$[elem].$.$">);
  expectType<User["matrixMetadata"][0][0][0]>(
    {} as ResolveUser<"matrixMetadata.$[elem].$.$[elem]">,
  );
  expectType<User["matrixMetadata"][0][0][0]>(
    {} as ResolveUser<"matrixMetadata.$[elem].$[elem].0">,
  );
  expectType<User["matrixMetadata"][0][0][0]>(
    {} as ResolveUser<"matrixMetadata.$[elem].$[elem].$">,
  );
  expectType<User["matrixMetadata"][0][0][0]>(
    {} as ResolveUser<"matrixMetadata.$[elem].$[elem].$[elem]">,
  );
  expectType<number>({} as ResolveUser<"matrixMetadata.0.0.0.cell">);
  expectType<number>({} as ResolveUser<"matrixMetadata.$.0.0.cell">);
  expectType<number>({} as ResolveUser<"matrixMetadata.$.$.0.cell">);
  expectType<number>({} as ResolveUser<"matrixMetadata.$.$.$.cell">);
  expectType<number>({} as ResolveUser<"matrixMetadata.$.$.$[elem].cell">);
  expectType<number>({} as ResolveUser<"matrixMetadata.$[].0.0.cell">);
  expectType<number>({} as ResolveUser<"matrixMetadata.$[elem].$.0.cell">);
  expectType<number>({} as ResolveUser<"matrixMetadata.$[elem].$.$.cell">);
  expectType<number>({} as ResolveUser<"matrixMetadata.$[elem].$.$[elem].cell">);
  expectType<number>({} as ResolveUser<"matrixMetadata.$[].$[elem].0.cell">);
  expectType<number>({} as ResolveUser<"matrixMetadata.$[elem].$[elem].$.cell">);
  expectType<number>({} as ResolveUser<"matrixMetadata.$[elem].$[elem].$[elem].cell">);

  // @ts-expect-error - Invalid path into ObjectId
  expectType<never>({} as ResolveUser<"orders.orderId">);
}

type JoinPath<Prefix extends string, Segment extends string> =
  Prefix extends "" ? Segment : `${Prefix}.${Segment}`;

type Alpha<T extends string> = Lowercase<T> extends Uppercase<T> ? never : T;
type Digit<T extends string> = T extends `${0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9}` ? T : never;

type MatchAlphanumeric<T extends string> =
  T extends `${infer F}${infer R}` ?
    Alpha<F> extends never ?
      Digit<F> extends never ?
        never
      : MatchAlphanumeric<R>
    : MatchAlphanumeric<R>
  : T;

type ValidIdentifier<T extends string> = T extends "" ? T : MatchAlphanumeric<T>;

type InvalidIdentifier<P extends string> =
  P extends `${infer _Start}$[${infer Id}]${infer Rest}` ?
    Id extends "" ? InvalidIdentifier<Rest>
    : ValidIdentifier<Id> extends never ? Id
    : InvalidIdentifier<Rest>
  : never;

type AddArrayFilter<T, FullPath extends string, Segment extends string> =
  Segment extends `$[${infer Id}]` ?
    Id extends "" ? {}
    : ValidIdentifier<Id> extends never ? OpaqueError<`Invalid array filter identifier: ${Id}`>
    : FullPath extends UpdatePathType<T> ? { [K in Id]: ResolveUpdatePath<T, FullPath> }
    : {}
  : {};

type MergeFilters<A, B> =
  A extends OpaqueError<string> ? A
  : B extends OpaqueError<string> ? B
  : {
      [K in keyof A | keyof B]: K extends keyof B ?
        K extends keyof A ?
          A[K] & B[K]
        : B[K]
      : K extends keyof A ? A[K]
      : never;
    };

type ArrayFilterMap<T, P extends string, Prefix extends string = "", Acc = {}> =
  P extends `${infer Head}.${infer Rest}` ?
    ArrayFilterMap<
      T,
      Rest,
      JoinPath<Prefix, Head>,
      MergeFilters<Acc, AddArrayFilter<T, JoinPath<Prefix, Head>, Head>>
    >
  : MergeFilters<Acc, AddArrayFilter<T, JoinPath<Prefix, P>, P>>;

type InvalidPositional<P extends string> = P extends `${string}.$` ? P : never;

type UpdatePathOf<P extends string> =
  InvalidPositional<P> extends never ?
    InvalidIdentifier<P> extends never ?
      P extends ValidUpdatePaths ?
        ResolveUpdatePath<User, P & ValidUpdatePaths> extends never ?
          never
        : P
      : never
    : OpaqueError<`Invalid array filter identifier: ${InvalidIdentifier<P>}`>
  : never;

type UpdatePathFilters<P extends string> =
  UpdatePathOf<P> extends string ?
    ArrayFilterMap<User, UpdatePathOf<P>> extends OpaqueError<string> ?
      ArrayFilterMap<User, UpdatePathOf<P>>
    : tf.Simplify<ArrayFilterMap<User, UpdatePathOf<P>>>
  : UpdatePathOf<P>;

// --- 1. Simple Primitive Array (tags) ---
expectType<UpdatePathFilters<"tags.$[t]">>({} as { t: string });

// --- 2. Simple Array of Objects (orders) ---
expectType<UpdatePathFilters<"orders.$[ord]">>({} as { ord: User["orders"][number] });

// --- 3. Field Access inside Array of Objects ---
expectType<UpdatePathFilters<"orders.$[ord].total">>({} as { ord: User["orders"][number] });

// --- 4. Nested Array of Primitives (orders.items) ---
// Using mixed Index and Filtered Positional
expectType<UpdatePathFilters<"orders.0.items.$[item]">>({} as { item: string });

// Using double Filtered Positional
expectType<UpdatePathFilters<"orders.$[ord].items.$[item]">>(
  {} as { ord: User["orders"][number]; item: string },
);

// --- 5. The Matrix (Level 1 Filter) ---
expectType<UpdatePathFilters<"matrixMetadata.$[l1]">>({} as { l1: User["matrixMetadata"][number] });

// --- 6. The Matrix (Level 2 Filter) ---
expectType<UpdatePathFilters<"matrixMetadata.0.$[l2]">>(
  {} as { l2: User["matrixMetadata"][number][number] },
);

// --- 7. The Matrix (Level 3 Filter) ---
expectType<UpdatePathFilters<"matrixMetadata.0.0.$[l3]">>({} as { l3: { cell: number } });

// --- 8. The Matrix (Leaf Property with Multi-Filters) ---
expectType<UpdatePathFilters<"matrixMetadata.$[l1].$[l2].$[l3].cell">>(
  {} as {
    l1: User["matrixMetadata"][number];
    l2: User["matrixMetadata"][number][number];
    l3: User["matrixMetadata"][number][number][number];
  },
);

// --- 9. Mixed $[] and $[id] ---
// $[] does not require an entry in arrayFilters, but $[id] does.
expectType<UpdatePathFilters<"matrixMetadata.$[].$[l2].$[l3]">>(
  {} as {
    l2: User["matrixMetadata"][number][number];
    l3: User["matrixMetadata"][number][number][number];
  },
);

// --- 10. Reusing Identifiers (Valid MQL, testing inference consistency) ---
expectType<UpdatePathFilters<"matrixMetadata.$[a].$[a].$[a]">>(
  {} as {
    // Usually, it should resolve to the union or the most specific element.
    // Here we assume the INTERSECTION (most specific).
    a: User["matrixMetadata"][number] &
      User["matrixMetadata"][number][number] &
      User["matrixMetadata"][number][number][number];
  },
);

// --- 11. Updating a specific item in all orders ---
expectType<UpdatePathFilters<"orders.$[].items.$[targetItem]">>({} as { targetItem: string });

// --- 12. Using '$' (Positional) with $[id] ---
// The query-positional '$' doesn't go in arrayFilters.
expectType<UpdatePathFilters<"orders.$.items.$[specificItem]">>({} as { specificItem: string });

// --- 13. $[id] on a non-array field ---
expectType<never>({} as UpdatePathOf<"age.$[val]">);

// --- 14. $[id] on an object (not an array) ---
expectType<never>({} as UpdatePathOf<"profile.$[val]">);

// --- 15. Too many dots for the schema (Matrix is only 3 levels deep) ---
expectType<never>({} as UpdatePathOf<"matrixMetadata.$[l1].$[l2].$[l3].$[l4]">);

// --- 16. Implicit array traversal in Update (Illegal in MQL) ---
expectType<never>({} as UpdatePathOf<"orders.total">);

// --- 17. Accessing property on array instead of element ---
expectType<never>({} as UpdatePathOf<"matrixMetadata.cell">);

// --- 18. Malformed Identifier ---
expectType<OpaqueError<"Invalid array filter identifier: my-tag.invalid">>(
  {} as UpdatePathFilters<"tags.$[my-tag.invalid]">,
);

// --- 19. Full Matrix Filters (no leaf access) ---
expectType<UpdatePathFilters<"matrixMetadata.$[l1].$[l2].$[l3]">>(
  {} as {
    l1: User["matrixMetadata"][number];
    l2: User["matrixMetadata"][number][number];
    l3: User["matrixMetadata"][number][number][number];
  },
);

// --- 20. Mixed filter + numeric index (orders.items) ---
expectType<UpdatePathFilters<"orders.$[ord].items.0">>({} as { ord: User["orders"][number] });

// --- 21. Too deep into primitive array element ---
expectType<never>({} as UpdatePathOf<"orders.$[ord].items.$[item].$[sub]">);

// --- 22. Filter + numeric indices on matrix with leaf access ---
expectType<UpdatePathFilters<"matrixMetadata.$[l1].0.0.cell">>(
  {} as { l1: User["matrixMetadata"][number] },
);

// --- 23. Filter on primitive array element (invalid) ---
expectType<never>({} as UpdatePathOf<"tags.$[t].$[u]">);

// --- 24. Unnamed positional without $[id] (invalid for Foo) ---
expectType<never>({} as UpdatePathOf<"orders.$[ord].items.$">);
