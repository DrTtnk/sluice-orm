# MongoDB 8.0 gap analysis for sluice-orm

> Generated on 2026-10-13 by a two-pass audit. Every row in sections 4 to 7 comes from a type probe (tsc 7), a code reading with file:line, or a v8.0 docs page. The probe files are not in the repository. Re-run a probe from the row text if you need to confirm a row.

## 1. Purpose and method

This document lists the places where sluice-orm differs from the MongoDB 8.0 API. It helps the maintainers choose what to fix. Ten first-pass agents compared the library with the MongoDB docs. A second pass checked every claim again. The second pass wrote new TypeScript probe files and ran them with tsc 7 (`strict`, `exactOptionalPropertyTypes`, same flags as `tsconfig.test.json`). It also read the code and fetched the `https://www.mongodb.com/docs/v8.0/...` pages again. The first pass had used 9.0 pages for some items. No real MongoDB 8.0 server run was possible, so rows about server behaviour come from code reading and docs text only. When the docs text is unclear, the row is marked UNSURE (section 7). A verdict CONFIRMED means: the probe compiled or failed as the row says, or the code shows the fact. Severity: S1 = wrong result type or wrong runtime behaviour. S2 = invalid MQL compiles, or valid MQL is rejected. S3 = missing feature. S4 = missing test only. "(design?)" means the library is strict on purpose and the maintainers must decide.
Counting note: totals in section 2 are counted from the verifier tables, one row per claim. Several rows describe the same root cause (for example, `$limit` accepts `0`). A fix at one place can close many rows.

### Maintainer decisions (2026-10-13)

These decisions settle most "(design?)" rows. A row that agrees with a decision is "by design" and is not a gap.

| Topic | Decision |
|---|---|
| Boolean operands (`$and`, `$or`, `$cond.if`, `$switch.case`) | Accept any expression, as MongoDB does. |
| Comparison operators (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$cmp`) | Operand types must overlap. `string \| number` against `string \| null` is allowed, because the runtime value can match. `null` against a field whose type has no `null` is rejected. `string` against `number` is rejected. |
| Numeric argument checks (`$limit`, `$skip`, `$sample`, `$range`, `n`, `$bit`, `$bucketAuto` buckets) | Use literal-type refinement. Reject bad number literals at compile time. A variable of type `number` still passes. No branded types. |
| Scope | Core MongoDB 8.0 data APIs only. Atlas search stages, features newer than 8.0, and admin or management APIs (section 6) are parked. |
| `_id` on `insertOne`, `insertMany`, `replaceOne`, `findOneAndReplace` | Keep `_id` always required. |
| Typo guards | Keep strict: `$unset` of a missing field, unknown key in a find projection, unknown key in a find `sort`. Dotted paths stay allowed in sort and projection. |
| Pipeline-form updates | Fix. Accept options, `findOneAndUpdate` and bulk updates. Write the tests first. |

### Fixed since this audit

| Gap | Fix | Tests |
|---|---|---|
| Top gap 3: four stages were `export declare const` with no runtime value | `$listLocalSessions`, `$listSessions`, `$planCacheStats` and `$shardedDataDistribution` build their stage. The first, second and fourth run only at database level, and sluice has no database-level aggregate yet. Their JSDoc says so. | `test/stages/adminStages.test.ts`, `test/utils/no-declare-only-exports.test.ts` |
| Top gap 4: `bulkWrite().execute({ ordered: false })` was ignored | `execute` merges its options over the options given to `bulkWrite`. | `test/integration/bulk-write.test.ts` (ordered and unordered) |
| Top gap 13, part: `$indexOfBytes` and `$indexOfCP` with `end` and no `start` sent `end` as `start` | A lone `end` now sends `start: 0`. | `test/operators/stringOperators.test.ts` |

## 2. Totals (verified rows)

Rows are counted per verifier-table row. "Out of scope" means a CONFIRMED gap that is newer than 8.0, Atlas-only, or a driver or admin API (section 6).

| Area | Confirmed S1 | Confirmed S2 | Confirmed S3 | Confirmed S4 | Confirmed, out of scope | Refuted | Unsure | Unverified | OK rows that were false |
|---|---|---|---|---|---|---|---|---|---|
| Aggregation stages | 20 | 67 | 13 | 21 | 7 | 5 | 9 | 9 | 0 |
| Expression operators (math, logic, convert) | 4 | 28 | 5 | 13 | 2 | 0 | 6 | 0 | 0 |
| Expression operators (string, date) | 17 | 29 | 3 | 11 | 1 | 0 | 6 | 3 | 0 |
| Expression operators (array, set, object, misc) | 6 | 24 | 4 | 23 | 10 | 7 | 6 | 0 | 2 |
| Accumulators and window operators | 17 | 30 | 2 | 9 | 0 | 3 | 5 | 1 | 0 |
| Query and projection operators | 0 | 73 | 5 | 1 | 0 | 2 | 2 | 1 | 0 |
| Update operators, update pipelines and bulkWrite | 1 | 35 | 7 | 12 | 0 | 2 | 3 | 0 | 2 |
| CRUD collection API | 11 | 36 | 60 | 10 | 16 | 3 | 7 | 0 | 0 |
| Cross-cutting hardening (stage position, terminal stages, version gating, test debt) | 8 | 19 | 11 | 7 | 1 | 7 | 1 | 2 | 0 |
| **All areas** | **84** | **341** | **110** | **107** | **37** | **29** | **45** | **16** | **4** |

Other rows: 71 OK rows held or were not re-checked, 4 confirmed notes with no severity. The "Cross-cutting" row counts only rows that are not repeats of the stage rows (the cross-cutting verifier repeated about 60 stage rows; those repeats are left out).

## 3. Top 20 gaps

Ranked by severity and by how many users meet the gap. Details and probe evidence are in section 4.

| # | Gap | Sev. | Area | Fix idea |
|---|---|---|---|---|
| 1 | Null results are typed as non-null. Numeric operators (`$add`, `$abs`, `$sqrt`, `$round`, `$bitAnd`, ...) return `number` for a nullable field. String and date operators (`$concat`, `$trim`, `$replaceOne`, `$indexOfBytes`, `$dateTrunc`, `$dateToString`, `$dateFromString`) never return `null` in the type. `$arrayElemAt` is non-null. `$isNumber` adds `null` but MongoDB never returns it. | S1 | 4.2 | Reuse the nullish helper that `$toInt` and `$toDate` already use (`src/builder.ts:98-105`) for every operator that returns null on null input. Test with an exact `Equal` check. |
| 2 | Accumulators and window operators typed `T \| undefined` where the server returns `null`: `$first`, `$last`, `$min`, `$max`, `$top`, `$bottom`, `$linearFill`, `$locf`, builder `$shift`, `$derivative`, `$integral`, `$expMovingAvg`. `$stdDevSamp` is `number` but returns null for one document. | S1 | 4.3 | Add `\| null` in `src/builder.ts:455-489, 1170-1270` and in `WindowOperatorResult` (`src/sluice-stages.ts:139-170`). |
| 3 | Four stages are exported with no runtime value: `$listLocalSessions`, `$listSessions`, `$planCacheStats`, `$shardedDataDistribution` (`export declare const`, `src/sluice-stages.ts:962,984,1006,1029`). The import compiles. `dist/index.js` has no such export, so the call fails at runtime. | S1 | 4.1 | Implement with `pushStage`, or remove from `src/index.ts`. Add a package check that every value export of `index.d.ts` exists in `index.js`. |
| 4 | `BulkWriteBuilder.execute({ ordered: false })` compiles and is ignored (`src/crud.ts:174`, `src/registry.ts:279-282`, Effect flavour `src/registryEffect.ts:44-45`). The existing test passes `ordered: true`, which is the default, so it cannot catch this. | S1 | 4.6 | Forward the argument or remove it from the type. Add a runtime test with `ordered: false` and a failing operation. |
| 5 | Dotted field names are wrong in result types. `$set` and `$addFields` keep `"a.b"` as a flat key. The object form `$set({ x: 1 })` loses all fields of the document. `$project` with `"a.b": 1` or `0` gives a wrong type. A missing field in `$project` compiles with an `OpaqueError` field type. `$$REMOVE` gives `never`. `$unset("a.b")` is rejected. | S1/S2 | 4.1 | Nest dotted keys in `ResolveSpec` (`src/sluice-stages.ts:315-319`, `:86-99`, `:846-852`). Type `$$REMOVE` as key removal. |
| 6 | The query filter type rejects valid MQL, and it hits common queries. Rejected: `{ field: null }` on a non-nullable or optional field; `$eq` and `$ne` on embedded documents, arrays and ObjectId; `$in`, `$nin`, `$gt`, `$lt` on Date, boolean, ObjectId (also `_id: { $in: [...] }`), nullable and optional fields; `$regex` on nullable fields; `$type: "number"`, numeric codes and arrays. | S2 | 4.4 | Fix the union fall-through in `src/sluice.ts:369-459` (`ComparisonOps<never>`). Distribute over `null` and `undefined`. Add ObjectId, Date and boolean. Replace `BSONTypeAlias` with a sluice union. |
| 7 | Aggregation `$match` rejects inline array literals (TS4104): `$box`, `$center`, `$polygon`, `$geometry`, `$bitsAllClear: [0, 1]`, `tags: ["a"]`, an inline `$near` Point. The same literals compile in `find` and `countDocuments`. | S2 | 4.4 | Use `readonly` tuple and array types in `src/sluice.ts:423-441, 414-415`. |
| 8 | `find` projection accepts only top-level `0` and `1`. Valid MQL is rejected: dotted paths, `true` and `false`, `$slice`, `$elemMatch`, `$meta`, positional `$`. | S2/S3 | 4.4 | Extend `ProjectionSpec` (`src/crud.ts:26`, `src/projection-types.ts:5-27`) and the result type. |
| 9 | CRUD result types and inputs are wrong for a `_id` that is not an ObjectId. `insertedId`, `insertedIds` and `upsertedId` are typed `ObjectId` for `_id: string`. `distinct("tags")` on `tags?: string[]` gives `(string[] \| undefined)[]`. `distinct("a.b")` is rejected. `insertOne`, `insertMany`, `replaceOne`, `findOneAndReplace` and bulk `insertOne` require `_id`. | S1/S2 | 4.6 | Pass the schema generic to the driver result types (`src/crud.ts:2-9`). Distribute `distinct` over `undefined`. Use `OptionalUnlessRequiredId` rules (decide: design?). |
| 10 | Options are missing or untyped on many CRUD methods. `updateOne` and `updateMany` lack `collation`, `let`, `writeConcern`, `maxTimeMS`, `bypassDocumentValidation`. `updateOne` and `replaceOne` lack `sort` (new in 8.0). `replaceOne` options are `unknown`. `deleteOne` and `deleteMany` take no options. `aggregate` takes no options. `insertMany` has no `ordered`. `find` lacks `let`, `allowDiskUse`, `readConcern`, `min`, `max` and more. Unknown option keys are silently ignored when one valid key is present. | S2/S3 | 4.6, 4.5 | Type each option set from the driver option interfaces. Remove the generic that disables the excess-property check (`src/crud.ts:185-261`). |
| 11 | Pipeline-style updates are rejected in some places. `findOneAndUpdate` and bulk `updateOne`/`updateMany` reject a pipeline. A pipeline `updateOne`/`updateMany` rejects every option, including `{ upsert: true }`. | S2 | 4.5, 4.6 | Widen `Update` in `src/crud.ts:139-156, 206-225, 258-268`. |
| 12 | Nested filtered positional paths on `$set` are rejected: `grades.$[t].questions.$[score]`, `grades.$[].questions.$[score]`, `a.$[].b.$[]` (the v8.0 docs use these). `$inc`, `$mul` and `$min` accept them. A negative test (`test/types/failing/updateFailing.test-d.ts:469-470`) locks the bug in. | S2 | 4.5, 4.7 | Fix overlapping template-literal keys in `UpdatePathType` (`src/paths.ts:44`). Flip the test. |
| 13 | Regex operators lose flags. The `bson` serializer drops the JS `s` flag and sends JS `g` as `s` (`node_modules/bson/src/parser/serializer.ts:175-177`). `$regexFind` captures are typed `string[]` but hold `null` for unmatched groups. `$indexOfBytes` and `$indexOfCP` with `end` and no `start` send `end` as `start`. | S1 | 4.2 | Reject `s` and `g` flags at type level. Type captures as `(string \| null)[]`. Emit `start: 0` when only `end` is set (`src/builder.ts:684-707`). |
| 14 | Variables and dynamic field names. `$getField` and `$setField` reject a dynamic `field` expression (valid since 7.2). `$setField` with `$$REMOVE` adds the key. `$$NOW` and `$$CLUSTER_TIME` are string literal types. Unknown `$$var` compiles and is `never`. `$$USER_ROLES` is `never`. | S1/S2 | 4.2 | Resolve system variables in `src/sluice.ts:139-176`. Accept `Ret<string>` for `field` (`src/builder.ts:1032-1072`). |
| 15 | Operator signatures reject valid input. `$.cmp("$num", 250)` fails even for equal types (shared `const T`, `src/builder.ts:621`). `$eq` family rejects mixed types, `null` and literal-first calls. `$and`, `$or`, `$cond.if` and `$switch.case` accept only booleans (design?). `$switch` needs `default`. `$cond` has no array form. `$convert` lacks `format`, numeric `to` and `binData`. Date-part operators reject the `{ date, timezone }` form. Date inputs reject ObjectId and Timestamp. | S2 | 4.2 | Use one type parameter per argument. Add the missing forms. |
| 16 | Window and accumulator rules are not enforced for the builder. `$.rank()`, `$.denseRank()`, `$.documentNumber()`, `$.shift()`, `$.first()`, `$.linearFill()` compile without `sortBy`. `$.add(...)` and `$.accumulator(...)` compile in a window. `$.add(...)` compiles in `$group` and `$bucket`. `documents` and `range` can be combined. `$expMovingAvg` accepts neither or both of `N` and `alpha`. Dotted `sortBy` is rejected. `$accumulator` requires `initArgs`. | S2 | 4.3 | Add an expression-or-accumulator mode to `Ret` (`src/builder.ts:270-281`). Check `sortBy` on builder results too (`src/sluice-stages.ts:202-210`). |
| 17 | `$bucketAuto` cannot express real accumulators in `output`, accepts a bare field reference, and always keeps `count` in the result type. `$bucket` rejects Date and string boundaries, and accepts `[]` and `[5]`. | S1/S2 | 4.1 | Give `$bucketAuto` the callback form of `$bucket` (`src/sluice-stages.ts:323-386`). Drop `count` when `output` is set. |
| 18 | Stage position and terminal rules are not enforced. After `$out` or `$merge`, `$match`, `$limit`, `$project` and `$count` compile (and `$count` revives the element type). `$geoNear`, `$changeStream`, `$collStats`, `$indexStats` and `$currentOp` compile in any position. `$facet`, `$lookup` and `$unionWith` accept forbidden stages (`$out`, `$merge`, `$geoNear`, nested `$facet`, ...). `$out` with `timeseries` is accepted and the key is dropped at runtime. `$merge` rejects `into: { db, coll }`. `registry(version)` has no effect (`src/registry.ts:363-364`). | S1/S2 | 4.1, 4.7 | New mechanism: a terminal marker in `Agg<TIn, TOut>`, a first-stage marker, and a stage filter for sub-pipelines. Needs maintainer approval. |
| 19 | Valid 8.0 stage forms can not be written. `$unionWith` without `coll` (with `$documents`), `$lookup` without `from`, `$unionWith("coll")`. `$documents` can not run (no database-level aggregate; `toList(collection?)` ignores its argument). `$fill` with values only and no `sortBy`. `$changeStream.startAtOperationTime` takes `Date` not `Timestamp`. `$geoNear` rejects a legacy coordinate pair and expression distances. `$sortByCount` rejects builder expressions. | S2/S3 | 4.1 | Make `coll` and `from` optional with a `$documents` rule. Add a database-level pipeline entry point. |
| 20 | Missing in 8.0: `$toUUID`, `$tsSecond`, `$tsIncrement`, `$toHashedIndexKey`, `$changeStreamSplitLargeEvent`, `$listSampledQueries`, `$querySettings`, expression forms of `$median` and `$percentile`, `$function` (deprecated in 8.0), `$planCacheStats({ allHosts })`, `$currentOp({ targetAllNodes })`, `$indexStats.building`. Also many numeric argument checks are missing (`$limit(0)`, `$skip(-1)`, `$sample({ size: 0 })`, `$count("")`, `n: 0` for `$firstN` and `$topN`, `$range` step `0`, `$sampleRate(2)`, `skip: -1`). | S3/S2 | 4.1-4.3 | Add the operators. For numeric checks, decide first whether to use literal-type refinements or a branded integer type. |

## 4. Confirmed gaps by area

Only CONFIRMED gaps of severity S1, S2 and S3 that apply to MongoDB 8.0 are listed. Missing tests (S4) are in section 5. Newer-than-8.0 and Atlas-only items are in section 6. `Impl` is the code that owns the behaviour. When the verifier gave only probe lines, the value is the line where the operator or option is defined. A dash means no single code line owns the gap (for example a missing feature).

### 4.1 Aggregation stages

| Item | Gap | Severity | Version scope | Impl (file:line) |
|---|---|---|---|---|
| $addFields ($$REMOVE typing) | $addFields ($$REMOVE typing): p3:30 (`{nick: "$$REMOVE"}`) infers `nick: never` (key stays, type never) instead of the key being removed/optional | S1 | 8.0 | src/sluice-stages.ts:315 |
| $addFields (dotted key type) | `"imdb.certified"` typed as flat key, runtime nests | S1 | 8.0 | src/sluice-stages.ts:319 |
| $bucketAuto (count in result with output) | result type always has `count: number` even with `output` | S1 | 8.0 | src/sluice-stages.ts:356 |
| $collStats (latencyStats type) | `reads/writes/commands` typed `Dict<number>` | S1 | 8.0 | src/sluice-stages.ts:390 |
| $collStats (queryExecStats type) | $collStats (queryExecStats type): src:415,440 `queryExecStats: Dict<number>` (not probed) | S1 | 8.0 | src/sluice-stages.ts:390 |
| $currentOp (secs_running, microsecs_running required) | present only when active | S1 | 8.0 | src/sluice-stages.ts:915 |
| $listLocalSessions (no runtime value) | `export declare const`, no value emitted | S1 | 8.0 | src/sluice-stages.ts:962; src/index.ts:93 |
| $listSessions (no runtime value) | $listSessions (no runtime value): src/sluice-stages.ts:984 `export declare const $listSessions`; src/index.ts:94; absent from dist/index.js (see above) | S1 | 8.0 | src/sluice-stages.ts:984; src/index.ts:94 |
| $listSessions/$listLocalSessions (output `_id` types) | `_id:{id:ObjectId; uid:string}` | S1 | 8.0 | src/sluice-stages.ts:984 |
| $out (timeseries) | `timeseries` missing and silently dropped | S1 | 8.0 | src/sluice-stages.ts:699-700; src/sluice-stages.ts:695 |
| $planCacheStats (output type) | Output has undocumented fields, lacks documented ones | S1 | 8.0 | src/sluice-stages.ts:1006-1025 |
| $planCacheStats (runtime absent) | `declare const` has no runtime value | S1 | 8.0 (stage exists since 4.0) | src/sluice-stages.ts:1006; src/index.ts:99 |
| $project (dotted exclusion) | $project (dotted exclusion): a.ts:81 result is the full document including `meta:{created,score}`; `Omit<C,"meta.score">` removes nothing (SS:87,99) | S1 | 8.0 | src/sluice-stages.ts:87 |
| $project (dotted inclusion) | `"a.b": 1` rejected | S1 | 8.0 | src/sluice-stages.ts:94 |
| $project ({} empty) result type | Empty spec gives `{[x:string]: unknown; _id}` | S1 | 8.0 | src/sluice-stages.ts:705 |
| $set (dotted key result) | Dotted key becomes flat literal key | S1 | 8.0 | src/sluice-stages.ts:774 |
| $set (object form loses document type) | `users.aggregate($set({x:1}))` result is `{[x:string]: unknown}` | S1 | 8.0 | src/sluice-stages.ts:772-774 |
| $shardedDataDistribution (output type) | `shards` typed Record, extra fields | S1 | 8.0 | src/sluice-stages.ts:1033; src/sluice-stages.ts:1042-1056 |
| $shardedDataDistribution (runtime absent) | `declare const` has no runtime value | S1 | 8.0 (6.0.3) | src/sluice-stages.ts:1029; src/index.ts:107 |
| $unwind (includeArrayIndex type) | Index typed `number`, docs say `null` possible | S1 | 8.0 | src/sluice-stages.ts:876 |
| $bucket (boundaries `[5]`, `[]`) | compile although docs need >= 2 boundaries | S2 | 8.0 | src/sluice-stages.ts:323 |
| $bucket (boundaries only numbers) | `boundaries: readonly number[]` rejects Date/string | S2 | 8.0 | src/sluice-stages.ts:323 |
| $bucket (default inside range) | `default: 5` with `[0,5,10]` compiles | S2 | 8.0 | src/sluice-stages.ts:323 |
| $bucket (default type) | `default` typed `string \| number` | S2 | 8.0 | src/sluice-stages.ts:338 |
| $bucketAuto (buckets 0/-1/2.5) | `buckets: number` accepts non-positive-int | S2 (design?) | 8.0 | src/sluice-stages.ts:356 |
| $bucketAuto (granularity with string groupBy) | `granularity:"R5"` with `groupBy:"$name"` compiles | S2 | 8.0 | src/sluice-stages.ts:356 |
| $bucketAuto (output accumulators cannot be written) | valid `output: {total:{$sum:"$price"}}` is not expressible | S2 | 8.0 | src/sluice-stages.ts:356 |
| $bucketAuto (output without accumulator) | `output:{x:"$price"}` compiles | S2 | 8.0 | src/sluice-stages.ts:356 |
| $changeStream (must be first) | not typed; `$match` before it compiles | S2 | 8.0 | src/sluice-stages.ts:896 |
| $changeStream (mutual exclusion) | resumeAfter/startAfter/startAtOperationTime together compile | S2 | 8.0 | src/sluice-stages.ts:896 |
| $changeStream (startAtOperationTime type) | $changeStream (startAtOperationTime type): p1:104 `new Timestamp({t:1,i:1})` rejected: "Type 'Timestamp' is missing the following properties from type 'Date'"; p1:105 `new Date()` accepted | S2 | 8.0 | src/sluice-stages.ts:896 |
| $collStats (must be first) | $collStats (must be first): p1:117 `users.aggregate($match(...), $collStats({count:{}}))` compiles | S2 | 8.0 | src/sluice-stages.ts:390 |
| $count (string restrictions) | `""`, `"$x"`, `"a.b"` compile | S2 | 8.0 | src/sluice-stages.ts:447 |
| $currentOp (admin-only, first stage) | $currentOp (admin-only, first stage): src:915-958 no position/db check | S2 | 8.0 | src/sluice-stages.ts:915 |
| $densify (bounds vs field type) | `[__,__]` not tied to field | S2 | 8.0 | src/sluice-stages.ts:453 |
| $densify (date without unit) | date field without unit compiles | S2 | 8.0 | src/sluice-stages.ts:453 |
| $densify (field type) | string field accepted | S2 | 8.0 | src/sluice-stages.ts:453 |
| $densify (step with unit) | `step:1.5` + unit compiles | S2 | 8.0 | src/sluice-stages.ts:453 |
| $densify (unit vs field type) | numeric field + unit compiles | S2 | 8.0 | src/sluice-stages.ts:453 |
| $documents (expression argument) | argument typed as literal array only | S2 | 8.0 (stage is 6.0) | src/sluice-stages.ts:470 |
| $facet (forbidden stages) | `$out`,`$merge`,`$facet`,`$collStats`,`$indexStats`,`$geoNear` compile in sub-pipeline | S2 | 8.0 | src/sluice-stages.ts:476 |
| $fill (linear with 2 sortBy keys) | $fill (linear with 2 sortBy keys): p1:78 compiles | S2 | 8.0 | src/sluice-stages.ts:500 |
| $fill (method + value) | `{method:"linear", value:0}` compiles | S2 | 8.0 | src/sluice-stages.ts:500 |
| $fill (partitionBy + partitionByFields) | $fill (partitionBy + partitionByFields): p1:76 compiles | S2 | 8.0 | src/sluice-stages.ts:500 |
| $fill (sortBy required) | value-only `$fill` without `sortBy` rejected | S2 | 8.0 | src/sluice-stages.ts:500 |
| $geoNear (legacy coordinate pair) | `near:[lng,lat]` rejected | S2 | 8.0 | src/sluice-stages.ts:514 |
| $geoNear (maxDistance/minDistance expressions) | $geoNear (maxDistance/minDistance expressions): src:522-523 `maxDistance?: number; minDistance?: number` | S2 | 8.0 (7.2) | src/sluice-stages.ts:514 |
| $geoNear (must be first) | $geoNear (must be first): p1:114 `$match` then `$geoNear` compiles | S2 | 8.0 | src/sluice-stages.ts:514 |
| $geoNear (query `$near`) | `$near` inside query compiles | S2 | 8.0 | src/sluice-stages.ts:514 |
| $graphLookup (maxDepth) | `-1`, `1.5` compile | S2 | 8.0 | src/sluice-stages.ts:542 |
| $group accumulators ahead of 8.0 | `concatArrays`, `setUnion` accumulators unconditionally available | S2 | 8.0 vs later | src/builder.ts:420 |
| $indexStats (position, transactions) | $indexStats (position, transactions): src:593 no check | S2 | 8.0 | src/sluice-stages.ts:593 |
| $limit (non-positive / fractional) | `0`, `-1`, `1.5` compile | S2 | 8.0 | src/sluice-stages.ts:599 |
| $listLocalSessions / $listSessions (collection-level) | `<C>` accepts collection pipeline | S2 | 8.0 | src/sluice-stages.ts:962 |
| $lookup (`$out`/`$merge` in callback) | $lookup (`$out`/`$merge` in callback): p1:87 (`$.pipe($out(orders))`), p1:88 (`$merge`) compile | S2 | 8.0 | src/sluice-stages.ts:617 |
| $lookup (`let` without pipeline) | $lookup (`let` without pipeline): p1:85 compiles | S2 | 8.0 | src/sluice-stages.ts:617 |
| $lookup (`{from, as}` alone) | $lookup (`{from, as}` alone): p1:82 `$lookup({from: orders, as:"o"})` compiles and infers `o: {_id; userId; amount}[]` (p1:89) | S2 | 8.0 | src/sluice-stages.ts:617 |
| $lookup (localField without foreignField) | $lookup (localField without foreignField): p1:83 compiles; also p1:84 (foreignField only) | S2 | 8.0 | src/sluice-stages.ts:617 |
| $match ($near/$nearSphere) | Geo `$near`/`$nearSphere` compile in `$match` | S2 | 8.0 | src/sluice-stages.ts:662 |
| $match ($where) | `$where` compiles in `$match` | S2 | 8.0 | src/sluice-stages.ts:662 |
| $merge (on array field) | `on` of array-typed field compiles | S2 | 8.0 | src/sluice-stages.ts:680 |
| $merge (whenMatched pipeline) | Any stage allowed in pipeline form | S2 | 8.0 | src/sluice-stages.ts:682 |
| $merge / $out (terminal position) | $merge / $out (terminal position): a.ts:52 `$merge,$match`, a.ts:54 `$out,$limit`, a.ts:55 `$merge,$limit` all compile clean | S2 | 8.0 | src/sluice-stages.ts:687; src/sluice-stages.ts:701 |
| $project ($slice negative n with position) | `$.slice("$a", -2, 1)` compiles | S2 | 8.0 | src/builder.ts:585-590 |
| $project (nested object form) | `{meta:{score:1}}` rejected | S2 | 8.0 | src/sluice-stages.ts:705 |
| $project ({} empty) | Empty `$project` compiles | S2 | 8.0 | src/sluice-stages.ts:705 |
| $replaceRoot (expression newRoot) | `{newRoot: $.add(1,2)}` compiles | S2 | 8.0 | src/sluice-stages.ts:291 |
| $replaceRoot (extra key) | `{newRoot, extra}` compiles | S2 | 8.0 | src/sluice-stages.ts:737; src/sluice-stages.ts:733-735 |
| $replaceRoot (scalar newRoot) | String field compiles | S2 | 8.0 | src/sluice-stages.ts:729 |
| $replaceWith (scalar / expression) | String field and `$.add` compile | S2 | 8.0 | src/sluice-stages.ts:754-757 |
| $sample (size) | 0, -1, 2.5 compile | S2 | 8.0 | src/sluice-stages.ts:764 |
| $setWindowFields ($documentNumber without sortBy) | $setWindowFields ($documentNumber without sortBy): b.ts:52 (builder) and b.ts:54 (raw `{$documentNumber:{}}`) both compile clean; SS:205 does not list it | S2 | 8.0 | src/sluice-stages.ts:205 |
| $setWindowFields (bounded window without sortBy) | $setWindowFields (bounded window without sortBy): d.ts:21 `window:{documents:[-1,0]}` with no sortBy compiles clean | S2 | 8.0 | src/sluice-stages.ts:782 |
| $setWindowFields (builder rank without sortBy) | `$.rank()` skips sortBy check | S2 | 8.0 | src/sluice-stages.ts:205 |
| $setWindowFields (implicit-window ops with window) | Rank/`$shift` with `window` compile | S2 | 8.0 | src/sluice-stages.ts:782 |
| $setWindowFields (order ops without sortBy) | `$shift`/`$first` without sortBy compile | S2 | 8.0 | src/sluice-stages.ts:782 |
| $setWindowFields (range+unit multi/desc sortBy) | $setWindowFields (range+unit multi/desc sortBy): d.ts:17 (two sortBy keys with range+unit) and d.ts:19 (descending single key with range) both compile clean (out_d.txt has errors only at d.ts:11 contr | S2 | 8.0 | src/sluice-stages.ts:782 |
| $setWindowFields (sortBy dotted) | Dotted `sortBy` rejected | S2 (design?) | 8.0 | src/sluice-stages.ts:165 |
| $setWindowFields (unit without range) | `unit` with documents window compiles | S2 | 8.0 | src/sluice-stages.ts:782 |
| $setWindowFields (window documents+range) | $setWindowFields (window documents+range): b.ts:56 compiles clean (SS:106-110 not XOR) | S2 | 8.0 | src/sluice-stages.ts:106-110 |
| $skip | `-1`, `2.5` compile | S2 | 8.0 | src/sluice-stages.ts:797 |
| $sort ($meta key name) | Arbitrary key with `$meta` rejected | S2 | 8.0 | src/sluice-stages.ts:167-172 |
| $sortByCount (builder expression) | Builder/`Ret` expressions rejected | S2 | 8.0 | src/sluice-stages.ts:811 |
| $sortByCount (unknown field) | `$sortByCount("$nope")` compiles | S2 | 8.0 | src/sluice-stages.ts:809 |
| $unset (dotted/embedded paths) | Dotted paths rejected | S2 | 8.0 | src/sluice-stages.ts:850 |
| $unwind (includeArrayIndex name) | `"$idx"` compiles | S2 | 8.0 | src/sluice-stages.ts:868 |
| $unwind (short string form on non-array) | Non-array path rejected | S2 (design?) | 8.0 | src/sluice-stages.ts:856 |
| $changeStreamSplitLargeEvent | $changeStreamSplitLargeEvent: `grep -rniE changeStreamSplitLargeEvent src test docs README.md` returns nothing | S3 | 8.0 | - |
| $collStats (host/shard/transactions, storageStats) | output type incomplete | S3 | 8.0 | src/sluice-stages.ts:390 |
| $currentOp (targetAllNodes) | option missing | S3 | 8.0 | src/sluice-stages.ts:918-925 |
| $indexStats (building) | `building?: boolean` missing | S3 | 8.0 | src/sluice-stages.ts:233-240 |
| $listSampledQueries | $listSampledQueries: grep: only docs/MQL_COMPLIANCE.md:53 lists it under "Not implemented (Atlas-only)". Docs v8.0 listSampledQueries: syntax `{ namespace: <namespace> }`, limits: not Atlas multi-tena | S3 | 8.0 | - |
| $lookup (`from` omission with `$documents`) | $lookup (`from` omission with `$documents`): src:635 `TFrom extends Collection`, `from` required in both overloads (src:641, 652) | S3 | 8.0 | src/sluice-stages.ts:617 |
| $merge (into {db,coll}) | Cross-DB `into` missing | S3 (design?) | 8.0 | src/sluice-stages.ts:679; src/sluice-stages.ts:674 |
| $planCacheStats (allHosts) | `allHosts` option absent | S3 | 8.0 (7.1) | src/sluice-stages.ts:1006 |
| $project ($meta keywords) | Missing score, scoreDetails, searchScoreDetails, searchSequenceToken, vectorSearchScore | S3 | textScore/indexKey: 8.0; searchSequenceToken, sear | src/builder.ts:1089 |
| $querySettings | Whole stage missing | S3 | 8.0 (core, not Atlas-only) | - |
| $setWindowFields (object form) | Only callback form typed | S3 | 8.0 | src/sluice-stages.ts:792; src/sluice-stages.ts:783 |
| $unionWith (coll required) | `$documents` pipeline without `coll` rejected | S3 | 8.0 | src/sluice-stages.ts:835 |
| $unionWith (string shorthand) | `$unionWith("coll")` missing | S3 | 8.0 | src/sluice-stages.ts:817 |

### 4.2 Expression operators: arithmetic, comparison, boolean, conditional, type conversion, trigonometry, bitwise, data size

| Item | Gap | Severity | Version scope | Impl (file:line) |
|---|---|---|---|---|
| $add / $subtract / $multiply / $divide / $mod ... (NULL flag as row property) | $add / $subtract / $multiply / $divide / $mod ... (NULL flag as row property): same as the "Numeric ops" row above; probed on add, subtract, multiply, divide, abs, sqrt, round, bitAnd | S1 | 8.0 | src/builder.ts:346 |
| $isNumber | Type adds null for nullable input; docs: never null | S1 | 8.0 | src/builder.ts:975 |
| $isNumber | Optional field also gives `boolean \| null` | S1 | 8.0 | src/builder.ts:975 |
| Numeric ops (abs, add, divide, multiply, subtract, mod, sqrt, ln, log, log10, pow, round,  | Result typed `number`, never `number\|null`, for nullable input | S1 | 8.0 | src/builder.ts:78 |
| $abs (array arg) | `$.abs("$scores")` (number[]) compiles | S2 | 8.0 | src/sluice.ts:93 |
| $and / $or | Operands must be boolean; docs: any expression by truthiness | S2 (design?) | 8.0 | src/builder.ts:644 |
| $asin / $acos / $acosh | Domain not checked: `$.asin(2)` | S2 | 8.0 | src/builder.ts:1132 |
| $binarySize | `$.binarySize(5)` and number field compile | S2 | 8.0 | src/builder.ts:1104 |
| $bitAnd / $bitOr / $bitXor / $bitNot | Doubles accepted: `$.bitAnd("$num", 1.5)`, `$.bitNot(0.5)` | S2 (design?) | 8.0 | src/builder.ts:1111 |
| $bsonSize | `$.bsonSize(5)` and string field compile | S2 | 8.0 | src/builder.ts:1107 |
| $cmp | Valid `$.cmp("$num", 250)` and `$.cmp("$d", date)` rejected (even same BSON type) | S2 | 8.0 | src/builder.ts:621 |
| $cond | `if` typed boolean only | S2 (design?) | 8.0 | src/builder.ts:658 |
| $eq | Literal first, field second rejected | S2 | 8.0 | src/builder.ts:625 |
| $eq family | Literal on the left and field on the right rejected: `$.eq(1,"$num")` | S2 | 8.0 | src/builder.ts:625 |
| $eq family | Nullable vs non-null field rejected: `$.eq("$num","$nnum")`, `$.eq("$num", null)` | S2 (design?) | 8.0 | src/builder.ts:625 |
| $eq/$gt/$gte/$lt/$lte/$ne | Mixed BSON types rejected; docs allow | S2 (design?) | 8.0 | src/builder.ts:625 |
| $eq/$ne etc. | Number vs null rejected | S2 (design?) | 8.0 | src/builder.ts:625 |
| $ifNull | 0 or 1 args compile | S2 | 8.0 | src/builder.ts:666-668 |
| $ln | `$.ln(-1)` compiles | S2 | 8.0 | src/builder.ts:370 |
| $log | `$.log(100, 1)` compiles (base must be >1) | S2 | 8.0 | src/builder.ts:373 |
| $log10 | `$.log10(-1)` compiles | S2 | 8.0 | src/builder.ts:376 |
| $multiply | $multiply: a.ts A50: no error | S2 | 8.0 | src/builder.ts:352 |
| $nor | Not an aggregation expression yet sluice accepts it | S2 | 8.0 | src/builder.ts:650 |
| $round | place not range-checked (`$.round("$num",100)`, `1.5`) | S2 | 8.0 | src/builder.ts:392 |
| $sqrt | `$.sqrt(-4)` compiles | S2 | 8.0 | src/builder.ts:382 |
| $switch | `default` required; docs optional | S2 | 8.0 | src/builder.ts:672-675 |
| $switch | `branches: []` compiles; docs 1+ | S2 | 8.0 | src/builder.ts:672 |
| $switch | Extra branch keys compile | S2 | 8.0 | src/builder.ts:672 |
| $switch | `case` typed BooleanArg; docs: any expression coerced | S2 (design?) | 8.0 | src/builder.ts:672 |
| $toObjectId | `$.toObjectId(5)` compiles (docs: string) | S2 | 8.0 | src/builder.ts:996 |
| $trunc | place not range-checked | S2 | 8.0 | src/builder.ts:385 |
| $cond | Array form `[if, then, else]` absent | S3 | 8.0 | src/builder.ts:658 |
| $convert | `base`, `format`, `byteOrder` options missing | S3 | `format`: 8.0; `base`, `byteOrder`: newer than 8.0 | src/builder.ts:959 |
| $convert | Numeric `to` codes and `to: {type, subtype}` and `"binData"` | S3 | 8.0 | src/builder.ts:107 |
| $toUUID | $toUUID: grep -il toUUID in src, test, docs, README: no file; a.ts A52 error TS2339 "Property 'toUUID' does not exist". Docs https://www.mongodb.com/docs/v8.0/reference/operator/aggregation/toUUID/ ex | S3 | 8.0 (in scope) | - |
| Numeric ops (decimal) | Result always `number`; docs say decimal when any operand decimal; Decimal128 not a NumericArg | S3 | 8.0 | src/sluice.ts:98; src/sluice.ts:242-249 |

### 4.3 Expression operators: string and date

| Item | Gap | Severity | Version scope | Impl (file:line) |
|---|---|---|---|---|
| $concat (null typing) | `$.concat("$nick","x")` typed string; docs say null | S1 | 8.0 | src/builder.ts:222-226 |
| $concat (optional field) | $concat (optional field): a-infer.ts:33 `Type 'string'` for `$.concat("$opt","x")` | S1 | 8.0 | src/builder.ts:679 |
| $dateFromString (return type) | $dateFromString (return type): a-infer.ts:63 `Type 'Date'` for nullable string; :65 with `onNull: 5, onError:"x"` still `Date` | S1 | 8.0 | src/builder.ts:857 |
| $dateToString (null typing) | string; docs null if date null and no onNull | S1 | 8.0 | src/builder.ts:902 |
| $dateTrunc (null typing) | `Date`; docs null if date null/missing | S1 | 8.0 | src/builder.ts:910 |
| $indexOfBytes (lone end) | `end` without `start` is sent as 3rd arg and read as start | S1 | 8.0 | src/builder.ts:689-693 |
| $indexOfBytes (null typing) | returns number, docs null if string null | S1 | 8.0 | src/builder.ts:684 |
| $indexOfCP (lone end) | $indexOfCP (lone end): builder.ts:702-706 (same code, quoted above for Bytes: `if (end !== undefined) args.push(end)` without start) | S1 | 8.0 | src/builder.ts:702-706 |
| $indexOfCP (null typing) | returns number, docs null | S1 | 8.0 | src/builder.ts:697 |
| $ltrim (null typing) | string, docs null->null | S1 | 8.0 | src/builder.ts:710 |
| $regexFind (captures null) | `captures: string[]` but non-participating group is null | S1 | 8.0 | src/builder.ts:716 |
| $regexFind (regex flags) | `/a/s` compiles; docs: regex literal only i,m | S1 | 8.0 | src/builder.ts:716 |
| $regexFindAll (captures null) | $regexFindAll (captures null): a-infer.ts:79 `captures: string[]` in array element; docs regexFindAll share capture semantics | S1 | 8.0 | src/builder.ts:723 |
| $replaceAll (null typing) | string; docs null | S1 | 8.0 | src/builder.ts:737 |
| $replaceOne (null typing) | string; docs any null -> null | S1 | 8.0 | src/builder.ts:744 |
| $rtrim (null typing) | $rtrim (null typing): a-infer.ts:41 `Type 'string'` (same example pattern in docs) | S1 | 8.0 | src/builder.ts:751 |
| $trim (null typing) | $trim (null typing): a-infer.ts:37 `Type 'string'` | S1 | 8.0 | src/builder.ts:799 |
| $dateAdd (ObjectId/Timestamp rejected) | docs accept Date, Timestamp, ObjectID | S2 | 8.0 | src/sluice.ts:104 |
| $dateAdd (timezone ref) | `timezone` typed `StringArg` not validated | S2 | 8.0 | src/builder.ts:841 |
| $dateAdd (unit string) | `unit:"fortnight"` compiles | S2 | 8.0 | src/builder.ts:839 |
| $dateDiff (ObjectId/Timestamp) | $dateDiff (ObjectId/Timestamp): b-accept.ts:73 `"$ts"` rejected | S2 | 8.0 | src/builder.ts:845 |
| $dateDiff (unit/startOfWeek) | `unit:"fortnight"`, `startOfWeek:"funday"` compile | S2 | 8.0 | src/builder.ts:848 |
| $dateFromParts (opts untyped) | `{month:1}`, mix year+isoWeek, string year compile | S2 | 8.0 | src/builder.ts:854 |
| $dateFromString (dateString unvalidated) | `D extends StringArg` not wrapped | S2 | 8.0 | src/builder.ts:857-858 |
| $dateFromString (timezone ref) | timezone `StringArg` unvalidated | S2 | 8.0 | src/builder.ts:857 |
| $dateSubtract (same as dateAdd) | ObjectId/Timestamp, unit, amount | S2 | 8.0 | src/builder.ts:866-871 |
| $dateToParts (ObjectId/Timestamp) | $dateToParts (ObjectId/Timestamp): b-accept.ts:82 `"$oid"` TS2820. Docs dateToParts: "Date, a Timestamp, or an ObjectID" | S2 | 8.0 | src/builder.ts:874 |
| $dateToString (ObjectId/Timestamp) | $dateToString (ObjectId/Timestamp): b-accept.ts:72 `"$oid"` rejected | S2 | 8.0 | src/builder.ts:902 |
| $dateToString (format any string) | specifiers unchecked | S2 (design?) | 8.0 | src/builder.ts:903 |
| $dateTrunc (ObjectId/Timestamp) | $dateTrunc (ObjectId/Timestamp): b-accept.ts:74 `"$oid"` rejected | S2 | 8.0 | src/builder.ts:910 |
| $dateTrunc (binSize) | `binSize` 0 / -1 compile | S2 | 8.0 | src/builder.ts:910 |
| $dateTrunc (startOfWeek) | $dateTrunc (startOfWeek): b-accept.ts:42 `"funday"` compiles | S2 | 8.0 | src/builder.ts:910 |
| $dateTrunc (unit millisecond) | `unit:"millisecond"` compiles | S2 | 8.0 | src/builder.ts:910 |
| $regexFind (options string) | `options:"q"` compiles; docs i,m,x,s | S2 | 8.0 | src/builder.ts:716 |
| $regexFind (regex + options) | `regex:/a/i, options:"i"` compiles; docs forbid | S2 | 8.0 | src/builder.ts:716 |
| $regexFindAll (flags/options holes) | $regexFindAll (flags/options holes): b-accept.ts:33 `/a/s` compiles; options typed `StringArgInput` (builder.ts:726); same bson flag loss | S2 | 8.0 | src/builder.ts:726 |
| $regexMatch (holes) | `/a/s`, options both compile | S2 | 8.0 | src/builder.ts:730 |
| $replaceOne (find as regex) | find regex rejected by type | S2 | 8.0 | src/builder.ts:744 |
| $strLenBytes (nullable accepted) | nullable field accepted; docs error | S2 | 8.0 | src/builder.ts:766 |
| $strLenCP (nullable accepted) | $strLenCP (nullable accepted): b-accept.ts:55 compiles | S2 | 8.0 | src/builder.ts:769 |
| $substrBytes (negatives) | `-1` start/length compile; docs non-negative | S2 | 8.0 | src/builder.ts:779 |
| $substrCP (negatives) | negatives compile | S2 | 8.0 | src/builder.ts:786 |
| $toDate (input unchecked) | `$.toDate(true)` compiles | S2 | 8.0 | src/builder.ts:98-100 |
| $trim/$ltrim/$rtrim (chars > 4096 bytes) | error on 8.0.22+, not typed | S2 (design?) | 8.0 (8.0.22+) | src/builder.ts:799 |
| date-part ops (ObjectId/Timestamp) | date-part ops (ObjectId/Timestamp): b-accept.ts:69 `$.year("$oid")`, :70 `"$ts"`, :76 ObjectId literal, :77 Timestamp literal all rejected | S2 | 8.0 | - |
| date-part ops object form | `{date, timezone}` rejected for all 12 ops | S2 | 8.0 | src/builder.ts:919-955 |
| $tsIncrement | $tsIncrement: c-missing.ts:8 TS2339; same grep | S3 | 8.0 | - |
| $tsSecond | $tsSecond: c-missing.ts:7 TS2551 "Property 'tsSecond' does not exist on type 'ExprBuilder<Doc>'"; grep -rniE "tsSecond\|tsIncrement\|encStr" src test docs README.md -> no hits | S3 | 8.0 | - |
| date-part ops (nullable field) | nullable date field accepted, returns number | S3 | 8.0 | - |

### 4.4 Expression operators: array, set, object, variables, miscellaneous

| Item | Gap | Severity | Version scope | Impl (file:line) |
|---|---|---|---|---|
| $$CLUSTER_TIME (typed literal) | $$CLUSTER_TIME (typed literal): p1.ts:118-119 gives `"$$CLUSTER_TIME"`; also p2.ts:35 `$.eq("$$CLUSTER_TIME", 1)` rejected because type is a string literal | S1 | 8.0 | src/sluice.ts:139-176 |
| $$NOW (typed literal) | $$NOW (typed literal): p1.ts:116-117: `$addFields({now:"$$NOW"})` gives `"$$NOW"`; src/sluice.ts:145. Consequence: `$.dateToString({date:"$$NOW"})` compiles (p1.ts:121) but output `now` is not Date | S1 | 8.0 | src/sluice.ts:145 |
| $$USER_ROLES (dotted/undotted) | dotted rejected, undotted unknown | S1 | 8.0 (7.0+) | src/sluice.ts:139-176 |
| $arrayElemAt (result nullability) | Result typed as non-null element; docs: out-of-bounds gives no value, null array gives null | S1 | 8.0 | src/builder.ts:409-412 |
| $setField ($$REMOVE value) | $setField ($$REMOVE value): p1.ts:89-90: `value:"$$REMOVE"` gives `{ a: number; b: string; z: never }` (adds `z: never` instead of removing) | S1 | 8.0 | src/builder.ts:1062 |
| Unknown `$$var` accepted anywhere, typed never | `$$NOPE` compiles in $addFields and gives `never` | S1 | 8.0 | src/sluice.ts:157-164 |
| $$DESCEND / $$PRUNE / $$KEEP | allowed anywhere | S2 | 8.0 | src/sluice.ts:139-176 |
| $$REMOVE (anywhere) | allowed outside $addFields/$project | S2 | 8.0 | src/sluice.ts:139-176 |
| $arrayToObject (invalid element shape) | elements that are neither {k,v} nor [k,v] accepted | S2 | 8.0 | src/builder.ts:415-417 |
| $filter (limit 0) | `limit: 0` compiles; docs need >= 1 | S2 | 8.0 | src/builder.ts:426 |
| $first (non-array input, expression) | `$.first(5)`, `$.first("$name")` compile | S2 | 8.0 | src/builder.ts:455 |
| $firstN (n: 0) | n 0 / negative compiles | S2 | 8.0 | src/builder.ts:531 |
| $getField ($-prefixed name) | `"$price"` needs $literal, not enforced | S2 | 8.0 | src/builder.ts:1032 |
| $getField (dynamic field typing) | expression `field` typed F string, result never when not in C | S2 | 8.0 (7.2+) | src/builder.ts:1032-1041 |
| $indexOfArray (end without start) | `{array, search, end}` compiles | S2 | 8.0 | src/builder.ts:471 |
| $last (non-array input, expression) | `$.last(5)` compiles | S2 | 8.0 | src/builder.ts:482 |
| $lastN (n: 0) | $lastN (n: 0): p1.ts:41 compiles | S2 | 8.0 | src/builder.ts:537 |
| $let (var names) | `Total` / `$x` names compile | S2 | 8.0 | src/builder.ts:1077 |
| $maxN (n: 0 / -1) | n 0 / negative compiles | S2 | 8.0 | src/builder.ts:519 |
| $mergeObjects (mergeObjects(5)) | number literal accepted | S2 | 8.0 | src/builder.ts:1045 |
| $minN (n: 0) | $minN (n: 0): p1.ts:43 compiles | S2 | 8.0 | src/builder.ts:525 |
| $range (non-integer) | `range(0.5,3)` compiles | S2 (design?) | 8.0 | src/builder.ts:548 |
| $range (step 0) | `range(0,10,0)` compiles | S2 | 8.0 | src/builder.ts:548 |
| $sampleRate (range) | `sampleRate(2)` compiles | S2 | 8.0 | src/builder.ts:1100 |
| $setEquals (arity) | zero/one args compile | S2 | 8.0 | src/builder.ts:818 |
| $setField ($-prefixed field) | `"$price"` accepted | S2 | 8.0 | src/builder.ts:1062 |
| $setIntersection (arity) | zero/one args compile | S2 | 8.0 | src/builder.ts:823 |
| $setUnion (arity, expression) | zero/one args compile | S2 | 8.0 | src/builder.ts:831 |
| $slice (real hole: position with negative n) | `slice(arr, 1, -2)` compiles; docs need positive n with position | S2 | 8.0 | src/builder.ts:585 |
| $zip (wrong-length defaults) | `defaults` with fewer entries than inputs compiles | S2 | 8.0 | src/builder.ts:604-608 |
| $function (MISSING) | not implemented | S3 | 8.0 (deprecated) | src/index.ts:5 |
| $toHashedIndexKey (MISSING) | not implemented | S3 | 8.0 | - |
| $tsIncrement (MISSING) | not implemented | S3 | 8.0 | - |
| $tsSecond (MISSING) | not implemented | S3 | 8.0 | - |

### 4.5 Accumulators and window operators

| Item | Gap | Severity | Version scope | Impl (file:line) |
|---|---|---|---|---|
| $avg | `$.avg("$name")` (string field) compiles | S1 | 8.0 | src/builder.ts:1176 |
| $bottom | $bottom: a.ts:40: `Type 'number \| undefined'` for `$.bottom({output:"$opt",...})`; no null | S1 | 8.0 | src/builder.ts:1239 |
| $bucketAuto | result type always has `count` even with `output` | S1 | 8.0 | - |
| $derivative / $integral / $expMovingAvg (builder) | builder result typed `number` | S1 | 8.0 | src/builder.ts:1347 |
| $first | non-null on optional field in `$group` | S1 | 8.0 | src/builder.ts:455 |
| $first (raw window) | raw `{ $first }` typed non-null | S1 | 8.0 | src/sluice-stages.ts:147 |
| $last | non-null on optional | S1 | 8.0 | src/builder.ts:482 |
| $linearFill | $linearFill: b.ts:43 `number \| undefined` (optional source), b.ts:44 `number` (required source); never null | S1 | 8.0 (5.3+) | src/builder.ts:1393 |
| $locf | $locf: b.ts:45 `number \| undefined`, b.ts:46 `number` | S1 | 8.0 (5.2+) | src/builder.ts:1397 |
| $max | non-null on optional | S1 | 8.0 | src/builder.ts:1184 |
| $min | non-null on optional | S1 | 8.0 | src/builder.ts:1190 |
| $shift (builder) | $shift (builder): b.ts:47 `number` (no default); raw b.ts:87 `number \| null` | S1 | 8.0 | src/builder.ts:1406 |
| $stdDevSamp | $stdDevSamp: a.ts:44 (S09) `Type 'number'` (`eq<number\|null>` also fails) | S1 | 8.0 | src/builder.ts:1204 |
| $top | non-null on optional | S1 | 8.0 | src/builder.ts:1256 |
| Window result typing | empty window should add `\| null` for all but count/sum/push/addToSet | S1 | 8.0 | src/sluice-stages.ts:105-210 |
| builder window ops vs raw | builder results lack the `\| null` that raw ones have | S1 | 8.0 | src/sluice-stages.ts:105-210 |
| first/last/min/max/top/bottom/linearFill/locf on optional | first/last/min/max/top/bottom/linearFill/locf on optional: a.ts:35-40, b.ts:37-45 `number \| undefined`; raw b.ts:79-82 | S1 | 8.0 | src/builder.ts:455-489, 1184-1194 |
| $accumulator | `initArgs` required, docs optional | S2 | 8.0 | src/accumulator-utils.ts:21 |
| $accumulator (closures) | functions via `toString()`, closure vars lost silently | S2 | n/a | src/accumulator-utils.ts:2-4; src/accumulator-utils.ts:31-37 |
| $accumulator (window) | accepted inside `$setWindowFields` | S2 | 8.0 | src/builder.ts:1269 |
| $accumulator in window / `$.add` etc. | Ret-only gate (any `Ret`) | S2 | 8.0 | src/sluice-stages.ts:123-135; src/sluice-stages.ts:575-580 |
| $bottomN | no type test; n:0 compiles | S2 | 8.0 | src/builder.ts:1245 |
| $bucketAuto | `output: { x: "$price" }` (plain field ref) compiles | S2 | 8.0 | - |
| $bucketAuto output | real accumulators rejected: `output: { n: { $sum: 1 } }` | S2 | 8.0 | src/sluice-stages.ts:356-388 |
| $denseRank (builder) | builder call without sortBy compiles | S2 | 8.0 | src/sluice-stages.ts:203-211 |
| $derivative | builder without `window` compiles | S2 | 8.0 | src/builder.ts:1347 |
| $derivative | `unit: "month"` and unit with numeric sortBy compile | S2 | 8.0 | src/builder.ts:1347 |
| $documentNumber | not enforced, builder and raw | S2 | 8.0 | src/builder.ts:1361 |
| $expMovingAvg | neither N nor alpha / both compile | S2 | 8.0 | src/builder.ts:1364 |
| $firstN / $lastN / $maxN / $minN | no type test; n constraints | S2 | 8.0 | src/builder.ts:531 |
| $group allowed accumulators | `$.add(1,2)` compiles in `$group` | S2 | 8.0 | src/sluice-stages.ts:575-580 |
| $integral | `unit: "month"`; unit with numeric sortBy | S2 | 8.0 | src/builder.ts:1379 |
| $linearFill / $first / $last / bounded window | sortBy not required by types | S2 | 8.0 | src/builder.ts:1393 |
| $max / $min / $sum (multi-arg) | multi-arg compiles in `$group` | S2 (design?) | 8.0 | src/builder.ts:316-321 |
| $median | `method` optional, docs required | S2 | 8.0 | src/builder.ts:1317 |
| $mergeObjects | `$.mergeObjects("$price")` (number) compiles | S2 | 8.0 | src/builder.ts:1045 |
| $minN / $topN / $bottomN | n:0, -1, 1.5 compile | S2 | 8.0 | src/builder.ts:525 |
| $percentRank | exists in 8.0? | S2 | none | src/builder.ts:1400; src/sluice-stages.ts:129 |
| $percentile | `method` optional | S2 | 8.0 | src/builder.ts:1321 |
| $percentile | p out of [0,1] accepted | S2 | 8.0 | src/builder.ts:1321 |
| $rank (builder) | $rank (builder): c.ts:7 (d01) no error | S2 | 8.0 | src/builder.ts:1403 |
| $shift | `default: "$price"` (field ref) compiles; docs: constant | S2 | 8.0 | src/builder.ts:1406 |
| Allowed window operators | `$.add(1,2)` compiles in window | S2 | 8.0 | src/sluice-stages.ts:105-210 |
| Window `range` | 2-key / descending / string sortBy accepted | S2 | 8.0 | src/sluice-stages.ts:105-210 |
| Window bounds | `documents` + `range` both compile | S2 | 8.0 | src/sluice.ts:490-492 |
| Window bounds | fractional bound / reversed bound compile | S2 | 8.0 | src/sluice-stages.ts:105-210 |
| sortBy dotted path | sortBy dotted path: e.ts:11 TS2353 "'\"meta.at\"' does not exist in type 'Partial<Record<\"_id\" \| \"meta\" \| \"v\", ...>>'" on a real nested schema; failureCases.test-d.ts:341-345 asserts rejection | S2 | 8.0 | src/sluice-stages.ts:105-210 |
| $median / $percentile | no expression form | S3 | 8.0 (7.0+) | src/builder.ts:1315 |
| $setWindowFields | builder `$.sum` etc cannot take `window` | S3 | 8.0 | - |

### 4.6 Query and projection operators

| Item | Gap | Severity | Version scope | Impl (file:line) |
|---|---|---|---|---|
| $all ($elemMatch) | `$all:[{$elemMatch}]` rejected | S2 | 8.0 | src/sluice.ts:415 |
| $all (regex elements) | $all (regex elements): F:862 `$all:[/a/,/b/]` `Type 'RegExp' is not assignable to type 'string'` | S2 | 8.0 | src/sluice.ts:415 |
| $all (scalar field) | `$all` on non-array rejected | S2 | 8.0 | src/sluice.ts:415 |
| $and (empty) | `{ $and: [] }` compiles | S2 | 8.0 | src/sluice.ts:468 |
| $and (two $text) | two `$text` in one `$and` compile | S2 | 8.0 | src/sluice.ts:468 |
| $bitsAllClear | BinData operand not typed | S2 | 8.0 | src/sluice.ts:395 |
| $bitsAllSet | BinData operand not typed | S2 | 8.0 | src/sluice.ts:396 |
| $bitsAnyClear | BinData operand not typed | S2 | 8.0 | src/sluice.ts:397 |
| $bitsAnySet | BinData operand not typed | S2 | 8.0 | src/sluice.ts:398 |
| $box | inline literal rejected in aggregation `$match` | S2 | 8.0 | src/sluice.ts:427 |
| $center | inline in `$match` | S2 | 8.0 | src/sluice.ts:429 |
| $centerSphere | $centerSphere: F:796 TS4104; F:798/800 clean | S2 | 8.0 | src/sluice.ts:430 |
| $elemMatch ($text) | `$text` inside `$elemMatch` compiles | S2 | 8.0 | src/sluice.ts:416 |
| $eq (embedded doc / array / ObjectId) | `$eq` with embedded doc rejected | S2 | 8.0 | src/sluice.ts:370 |
| $eq (null) | `{age:{$eq:null}}`, `{nick:{$eq:null}}` rejected | S2 | 8.0 | src/sluice.ts:369-374 |
| $expr (raw MQL) | raw object rejected, builder works | S2 (design?) | 8.0 | src/sluice.ts:466 |
| $geoIntersects | $geoIntersects: no test mentions `$geoIntersects` anywhere in test/ (grep: none runtime, none type) | S2 | 8.0 | src/sluice.ts:424 |
| $geoWithin (empty) | `{ $geoWithin: {} }` compiles | S2 | 8.0 | src/sluice.ts:425-431 |
| $geoWithin (multiple shapes) | `$box`+`$polygon` compile; "exactly one" rule | S2 | 8.0 | src/sluice.ts:425 |
| $geometry | $geometry: untested: no `$geometry` in test/. Docs: `$geoWithin` `$geometry` type "Polygon" or "MultiPolygon"; `$near`/`$nearSphere` accept Point only ("Starting in MongoDB 8.0, $near, $nearSphere, an | S2 | 8.0 | src/sluice.ts:424-441 |
| $gt (ObjectId) | ObjectId `$gt` rejected | S2 | 8.0 | src/sluice.ts:388 |
| $gt (boolean) | boolean `$gt` rejected | S2 | 8.0 | src/sluice.ts:455 |
| $gt (nullable/optional) | nullable and optional fields lose `$gt` | S2 | 8.0 | src/sluice.ts:452-459 |
| $gte | $gte: F:226 `'$gte' does not exist in type 'ComparisonOps<never> \| Ret<..., number \| null>'` | S2 | 8.0 | src/sluice.ts:389 |
| $in (Date) | Date `$in` rejected | S2 | 8.0 | src/sluice.ts:444-449 |
| $in (ObjectId) | ObjectId `$in` rejected | S2 | 8.0 | src/sluice.ts:392 |
| $in (boolean) | boolean `$in` rejected | S2 | 8.0 | src/sluice.ts:392 |
| $in (optional / nullable) | $in (optional / nullable): F:292 `'$in' does not exist in type 'ComparisonOps<never> \| Ret<..., string \| undefined>'`; F:298 nullable `["a", null]`; F:304 nullable `["a"]`; F:310 `number\|null` | S2 | 8.0 | src/sluice.ts:392 |
| $in (regex element) | $in (regex element): F:376 `$in:[/^a/]` on string `Type 'RegExp' is not assignable to type 'MatchStringIn'` | S2 | 8.0 | src/sluice.ts:392 |
| $jsonSchema | accepts any object, wrong vocabulary | S2 | 8.0 | src/sluice.ts:478 |
| $lt | $lt: F:232 `'$lt' does not exist in type ... number \| null` | S2 | 8.0 | src/sluice.ts:390 |
| $lte | $lte: F:238 `'$lte' does not exist in type ... number \| undefined` | S2 | 8.0 | src/sluice.ts:391 |
| $mod (array field) | `$mod` on number-array field rejected | S2 | 8.0 | src/sluice.ts:394 |
| $mod (divisor 0) | divisor 0 accepted by type | S2 | 8.0 | src/sluice.ts:394 |
| $natural | `hint:{$natural:0}` compiles | S2 | 8.0 | src/crud.ts:33 |
| $ne (embedded doc) | `$ne` with embedded doc rejected | S2 | 8.0 | src/sluice.ts:371 |
| $ne (null) | $ne (null): F:94 (non-null number) `null not assignable to MatchNumericIn \| undefined`; F:100 (nullable) `null not assignable to undefined` | S2 | 8.0 | src/sluice.ts:371 |
| $near (countDocuments) | $near (countDocuments): E:72 clean | S2 | 8.0 | src/sluice.ts:432 |
| $near (in $match) | accepted in aggregation `$match` | S2 | 8.0 | src/sluice.ts:432 |
| $near (legacy form) | `{ $near: [0,0] }` rejected | S2 | 8.0 | src/sluice.ts:432 |
| $nearSphere (in $match) | accepted in aggregation `$match` | S2 | 8.0 | src/sluice.ts:437 |
| $nearSphere (legacy form) | $nearSphere (legacy form): F:658/660/662 `TS2741 Property '$geometry' is missing` | S2 | 8.0 | src/sluice.ts:437 |
| $nin | $nin: F:346 Date, F:352 boolean, F:358 nullable, F:364 ObjectId, all `'$nin' does not exist in type ...` | S2 | 8.0 | src/sluice.ts:393 |
| $nor ($text) | `$text` in `$nor` compiles | S2 | 8.0 | src/sluice.ts:469 |
| $nor (empty) | $nor (empty): F:694-698 clean; docs "array of one or more query predicates" (D query/nor/) | S2 | 8.0 | src/sluice.ts:469 |
| $not (per-field coverage) | per-field `$not` missing on nullable/object/ObjectId | S2 | 8.0 | src/sluice.ts:377 |
| $not (top-level) | top-level `$not` compiles | S2 | 8.0 | src/sluice.ts:377 |
| $options | `"z"`, `"g"` accepted | S2 | 8.0 | src/sluice.ts:402 |
| $or (empty) | `{ $or: [] }` compiles | S2 | 8.0 | src/sluice.ts:467 |
| $polygon | inline in `$match` | S2 | 8.0 | src/sluice.ts:428 |
| $regex (array field) | `$regex` on `string[]` rejected | S2 | 8.0 | src/sluice.ts:402 |
| $regex (optional/nullable) | $regex (optional/nullable): F:382 optional `'$regex' does not exist in type 'ComparisonOps<never> \| Ret<..., string \| undefined>'`; F:388 nullable `... string \| null` | S2 | 8.0 | src/sluice.ts:402 |
| $size | negative / fractional compile | S2 | 8.0 | src/sluice.ts:414 |
| $text (first stage) | `$match` position unchecked | S2 (design?) | 8.0 | src/sluice-stages.ts:661-668 |
| $text (in $or) | $text (in $or): F:616 `{ $or: [{ $text }, {age:1}] }` clean | S2 | 8.0 | src/sluice.ts:470 |
| $text (nor/elemMatch/and) | accepted in `$nor`, `$elemMatch`, two in `$and` | S2 | 8.0 | src/sluice.ts:470 |
| $type ("number" alias) | `$type:"number"` rejected | S2 | 8.0 | src/sluice.ts:373 |
| $type (array form) | array of types rejected | S2 | 8.0 | src/sluice.ts:373 |
| $type (numeric code) | `$type:1` rejected | S2 | 8.0 | src/sluice.ts:373 |
| $where (countDocuments) | accepted in countDocuments | S2 | 8.0 | src/sluice.ts:477 |
| $where (in $match) | accepted in aggregation `$match` | S2 | 8.0 | src/sluice.ts:477 |
| `$bitsAllSet` etc on BinData field | field typed Binary rejects every bit operator | S2 | 8.0 | src/sluice.ts:396 |
| `$eq`/`$in`/`$gt` on ObjectId | falls to `ComparisonOps<never>` | S2 | 8.0 | src/sluice.ts:370 |
| `$near`/`$nearSphere` accept any Geometry | Polygon etc compile in `$near` | S2 | 8.0 | src/sluice.ts:432-441 |
| arrayFilters `$type` (hardening list) | `string \| number` accepts any alias | S2 | 8.0 | src/crud/updates/types.ts:135 |
| dotted path typo `"profile.nope"` | dotted path typo `"profile.nope"`: F:736/738/740 clean (a/f/c) in the schema with arrays; E:60 clean in find; `"items.0.nope"` F:748 clean; `"items.nope"` F:754 clean | S2 | 8.0 | src/sluice.ts:423-480 |
| equality embedded / array | direct works, `$eq` does not | S2 | 8.0 | src/sluice.ts:423-480 |
| inline mutable-array literals in aggregation `$match` | any filter type containing a mutable tuple/array (`$box`, `$center`, `$centerSphere`, `$polygon`, `$geometry.coordinates`, `$bitsAllClear:[0,1]`, bare `tags:["a"]`, inline `$near` Point) is rejected in `$match` but accepted in fin | S2 | 8.0 | src/sluice.ts:423-480 |
| null vs missing (bare null) | `{opt:null}`, `{age:null}` rejected | S2 | 8.0 | src/sluice.ts:423-480 |
| projection boolean values | projection boolean values: O:64 `{name:true}` `TS2322 Type 'true' is not assignable to type '0 \| 1 \| undefined'`; O:66 `false` same | S2 | 8.0 | src/crud.ts:26; src/projection-types.ts:5-27 |
| projection unknown key | `{nope:1}` rejected | S2 (design?) | 8.0 | src/crud.ts:26; src/projection-types.ts:5-27 |
| top-level operators | `$gt`, `$regex`, `$elemMatch`, `$bogusOperator` at top level compile | S2 | 8.0 | src/sluice.ts:423-480 |
| unknown top-level field | unknown top-level field: Main (arrays): F:730 `{nonexistentField:1}` clean | S2 | 8.0 | src/sluice.ts:463-480 |
| $ (positional projection) | `"items.$":1` rejected | S3 | 8.0 | src/crud.ts:26 |
| $elemMatch (projection) | $elemMatch (projection): O:50 `TS2322 Type '{ $elemMatch: ... }' is not assignable to type '0 \| 1 \| undefined'` | S3 | 8.0 | src/sluice.ts:416 |
| $meta (projection) | $meta (projection): O:52 `Type '{ $meta: string; }' is not assignable to type '0 \| 1 \| undefined'`; computed key `score:{$meta}` O:54 `TS2561 'score' does not exist` | S3 | 8.0 | src/crud.ts:26; src/projection-types.ts:5-27 |
| $slice (projection) | $slice (projection): O:56 `{ $slice: number }` and O:58 `{ $slice: number[] }` `not assignable to type '0 \| 1 \| undefined'` | S3 | 8.0 | src/crud.ts:26; src/projection-types.ts:5-27 |
| projection dotted path | `"profile.bio":1` rejected | S3 | 8.0 | src/crud.ts:26; src/projection-types.ts:5-27 |

### 4.7 Update operators, update pipelines and bulkWrite

| Item | Gap | Severity | Version scope | Impl (file:line) |
|---|---|---|---|---|
| BulkWriteBuilder.execute(options) | BulkWriteBuilder.execute(options): crud.ts:174 `execute(options?: { ordered?: boolean })`; registry.ts:279-282 quote: `execute: () => mongoCol.bulkWrite(operations as AnyBulkWriteOperation<T>[], optio | S1 | 8.0 | src/crud.ts:174; src/registry.ts:279-282 |
| $ (positional) filter must contain the array | $ (positional) filter must contain the array: p5.ts:5 (`updateOne(F, {$set:{"items.$.qty":1}})` with filter `{_id:"x"}`) clean | S2 (design?) | 8.0 | - |
| $ (positional) with upsert | docs: upsert fails; TS cannot know | S2 | 8.0 | - |
| $[<identifier>] (identifier syntax) | `$[Bad]`, `$[my_id]` compile | S2 | 8.0 | src/crud/updates/types.ts:103-112 |
| $addToSet (with $position) | `{$each:[..],$position:0}` compiles | S2 | 8.0 | src/crud/updates/operators.ts:70-74 |
| $bit (fractional operand) | `{and:1.5}` compiles | S2 (design?) | 8.0 | src/crud/updates/operators.ts:113-117 |
| $currentDate (timestamp into Date field) | `{$type:"timestamp"}` compiles on a `Date` schema field | S2 (design?) | 8.0 | src/crud/updates/operators.ts:53 |
| $inc on nullable number | `{$inc:{nul:1}}` compiles; server errors on null | S2 | 8.0 | src/crud/updates/operators.ts:41 |
| $inc/$min/$max/$mul on `number\|null` | `$min` rejects nullable numbers | S2 | 8.0 | src/crud/updates/operators.ts:41 |
| $max (boolean field) | $max (boolean field): p1.ts:31 TS2322 "'false' not assignable to 'never'" | S2 | 8.0 | src/crud/updates/operators.ts:44 |
| $min (boolean field) | `{$min:{active:true}}` rejected, docs: any BSON type | S2 | 8.0 | src/crud/updates/operators.ts:44-49 |
| $min/$max (other rejected shapes, not in haiku) | nullable number, ObjectId, arrays, nested object also rejected | S2 | 8.0 | src/crud/updates/operators.ts:44 |
| $mul (operand/field types) | operand is JS number only; docs examples use Decimal128/Int32 | S2 (design?) | 8.0 | src/crud/updates/operators.ts:42 |
| $mul on nullable number | `{$mul:{nul:2}}` compiles | S2 | 8.0 | src/crud/updates/operators.ts:42 |
| $position (without `$each`) | $position (without `$each`): same evidence as $push row: p1.ts:73 | S2 | 8.0 | src/crud/updates/operators.ts:55-74 |
| $pull ($elemMatch nested in element field) | docs-example `$elemMatch` rejected | S2 | 8.0 | src/crud/updates/operators.ts:76-87 |
| $push (`$position`/`$slice`/`$sort` without `$each`) | compile; docs: need `$each` | S2 | 8.0 | src/crud/updates/operators.ts:66 |
| $push (empty modifier object) | `$push:{tags:{}}` compiles | S2 | 8.0 | src/crud/updates/operators.ts:55-74 |
| $rename (positional path accepted) | `{$rename:{"items.$.id":"x"}}` compiles | S2 | 8.0 | src/crud/updates/operators.ts:51 |
| $rename (same name) | `{$rename:{name:"name"}}` compiles | S2 | 8.0 | src/crud/updates/operators.ts:51 |
| $rename (target path not in conflict check) | `$rename name->count` + `$set count` compiles | S2 | 8.0 | src/crud/updates/types.ts:64-87 |
| $slice (without `$each`) | $slice (without `$each`): p1.ts:75 clean; `$each:[]` + `$slice:-3` accepted p1.ts:81 | S2 | 8.0 | src/crud/updates/operators.ts:55-74 |
| $sort (push, without `$each`) | $sort (push, without `$each`): p1.ts:77 clean (now by compile, not by reading) | S2 | 8.0 | src/crud/updates/operators.ts:55-74 |
| Conflicting paths (bulkWrite `update`) | Conflicting paths (bulkWrite `update`): p2.ts:60 clean (`$set tags` + `$push tags` in bulkWrite updateOne); p6.ts:32 clean (updateMany) | S2 | 8.0 | src/crud.ts:141; src/crud.ts:205 |
| Conflicting paths (rename target vs other operators) | Conflicting paths (rename target vs other operators): p1.ts:47 clean (see $rename target row) | S2 | 8.0 | src/crud/updates/validation.ts:56-79 |
| _id immutability (`$set _id`) | `{$set:{_id:"other"}}` compiles; rule unknown | S2 (design?) | 8.0 | src/crud/updates/operators.ts:33 |
| `$set` is the only operator rejected on nested identifiers | root cause = overlapping template keys | S2 | 8.0 | src/crud/updates/operators.ts:33-35; src/paths.ts:44 |
| arrayFilters (nested identifiers, `$set` on primitive arrays) | arrayFilters (nested identifiers, `$set` on primitive arrays): p1.ts:99 (`grades.$[t].questions.$[s]`), p1.ts:101, p1.ts:105 (`grades.$[].questions.$[s]`), p1.ts:107 (`matrix.$[r].$[e]`), p2.ts:114/11 | S2 | 8.0 | src/paths.ts:44 |
| arrayFilters (two filter objects for the same identifier) | extra filters not checked | S2 | 8.0 | src/crud/updates/types.ts:124-219 |
| arrayFilters option (identifier syntax / unused validator) | arrayFilters option (identifier syntax / unused validator): Identifier: see `$[<identifier>]` row | S2 | 8.0 | src/crud/updates/validation.ts:87 |
| bulkWrite updateOne/updateMany: pipeline update | MISSING (pipeline arrays rejected) | S2 | 8.0 | src/crud.ts:141 |
| bulkWrite: updateOne (empty update) | bulkWrite: updateOne (empty update): p2.ts:70 clean; also `updateOne(F, {})` p3.ts:22 clean | S2 | 8.0 | src/crud.ts:285 |
| bulkWrite: updateOne/updateMany (empty / wrong arrayFilters) | bulkWrite: updateOne/updateMany (empty / wrong arrayFilters): p2.ts:52 (`[]`), p2.ts:54 (wrong identifier `j.qty`), p2.ts:56 (wrong value type `"i.qty":"str"`), p6.ts:30 (updateMany `[]`) all clean | S2 | 8.0 | src/crud/updates/types.ts:124-219 |
| bulkWrite: updateOne/updateMany (positional w/o arrayFilters) | bulkWrite: updateOne/updateMany (positional w/o arrayFilters): p2.ts:50 (updateOne), p6.ts:28 (updateMany) clean | S2 | 8.0 | src/crud.ts:143 |
| replaceOne (options typed `unknown`) | any options compile | S2 | 8.0 | src/crud.ts:230; src/registry.ts:182 |
| updateOne/updateMany pipeline form + options | every option rejected with a pipeline callback | S2 (design?) | 8.0 | src/crud.ts:207; src/registry.ts:136-153 |
| bulkWrite options: writeConcern | bulkWrite options: writeConcern: p2.ts:88 TS2353 "'writeConcern' does not exist in type { ordered?: boolean }". crud.ts:285. Docs v8.0 bulkWrite: options `writeConcern` and `ordered` | S3 | 8.0 | src/crud.ts:285 |
| deleteOne / deleteMany: options | MISSING hint/collation | S3 | 8.0 | src/crud.ts:233-239; src/registry.ts:185-193 |
| findOneAndUpdate: pipeline form | findOneAndUpdate: pipeline form: p2.ts:108 (array) and p2.ts:110 (callback form) TS2554 "Expected 3 arguments, but got 2" (arg rejected by `Update extends UpdateSpec<C>`, crud.ts:260) | S3 | 8.0 | src/crud.ts:260 |
| updateOne/updateMany: collation | MISSING in options | S3 | 8.0 | src/crud/updates/types.ts:216-219; src/registry.ts:153 |
| updateOne/updateMany: let | updateOne/updateMany: let: p2.ts:8 (updateOne), p2.ts:24 (updateMany) TS2353. Docs v8.0 updateOne lists `let`. | S3 | 8.0 | src/crud.ts:200 |
| updateOne/updateMany: maxTimeMS, writeConcern, bypassDocumentValidation | updateOne/updateMany: maxTimeMS, writeConcern, bypassDocumentValidation: p2.ts:12,14,16 (updateOne) and p2.ts:26,28 (updateMany) TS2353 each | S3 | 8.0 | src/registry.ts:153 |
| updateOne: sort (New in 8.0) | updateOne: sort (New in 8.0): p2.ts:10 TS2353 "'sort' does not exist". Docs v8.0 updateOne: `sort`: "New in version 8.0." updateMany correctly has no sort (p2.ts:30 rejected, docs list no sort for upd | S3 | 8.0 | src/crud.ts:200 |

### 4.8 CRUD collection API

| Item | Gap | Severity | Version scope | Impl (file:line) |
|---|---|---|---|---|
| B6 bulkWrite: ordered (execute arg) | B6 bulkWrite: ordered (execute arg): Key facts; p2:40 | S1 | n/a | src/crud.ts:285 |
| C1 InsertOneResult/InsertManyResult generics | C1 InsertOneResult/InsertManyResult generics: p2:20,23 (ObjectId for `_id: string`); p1:32,38 eq fails; crud.ts:2-9,48,53 | S1 | 8.0 | src/crud.ts:2-9 |
| C2 UpdateResult upsertedId | C2 UpdateResult upsertedId: p2:26 `ObjectId \| null` for `_id: string`; p1:41 eq fails; crud.ts:64 | S1 | 8.0 | src/crud.ts:64 |
| C9 distinct return | C9 distinct return: same as distinct element rows (p1:45, p2:75) | S1 | 8.0 | src/crud.ts:280 |
| E3 bulkWrite.execute options in Effect | E3 bulkWrite.execute options in Effect: registryEffect.ts:44-45 forwards args to `invoke(...args)`; registry.ts:281 execute ignores them | S1 | n/a | src/registryEffect.ts:44-45; src/registry.ts:281 |
| bulkWrite (execute args ignored) | `.execute({ ordered:false })` compiles and is ignored | S1 | n/a | src/registry.ts:279-282 |
| distinct (nullable array) | `nums?: number[] \| null` | S1 | 8.0 | src/crud.ts:280 |
| distinct (optional array element type) | `tags?: string[]` yields `(string[] \| undefined)[]` | S1 | 8.0 | src/crud.ts:280 |
| find toList vs toOne inconsistent options | toOne forwards all options to driver findOne, toList applies only 8 | S1 | n/a | src/registry.ts:93-104 |
| insertMany (InsertManyResult generic) | insertedIds typed ObjectId for string `_id` | S1 | 8.0 | src/crud.ts:53 |
| insertOne (InsertOneResult generic) | insertedId typed ObjectId for string `_id` | S1 | 8.0 | src/crud.ts:2-9 |
| B1 find: limit | `limit: 1.5` compiles | S2 | 8.0 | src/crud.ts:183 |
| B1 find: maxTimeMS | B1 find: maxTimeMS: p1:64 no error | S2 | 8.0 | src/crud.ts:183 |
| B1 find: projection | true/false and dot-path rejected | S2 | 8.0 | src/crud.ts:26 |
| B1 find: skip | `skip: -1` compiles | S2 | 8.0 | src/registry.ts:97 |
| B1 find: sort | B1 find: sort: see find rows | S2 | 8.0 | src/crud.ts:183 |
| B10 distinct: dot-path key | B10 distinct: dot-path key: p1:47; docs `distinct("item.sku")` | S2 | 8.0 | src/crud.ts:280 |
| B4 updateOne/Many: pipeline update options | `[]` tuple for pipeline form | S2 | 8.0 | src/crud.ts:206-210 |
| B5 replaceOne: options (all) | B5 replaceOne: options (all): crud.ts:230; p1:52,54,p2:65 | S2 | 8.0 | src/crud.ts:230 |
| B6 bulkWrite op insertOne.document | `_id` required | S2 | 8.0 | src/crud.ts:196 |
| B6 bulkWrite op replaceOne.replacement | `_id` required | S2 | 8.0 | src/crud.ts:227 |
| B6 bulkWrite op update: `$[t]` without arrayFilters | B6 bulkWrite op update: `$[t]` without arrayFilters: p1:114 no error; standalone updateOne rejects it (p2:56 `Source has 0 element(s) but target requires 1`) | S2 | 8.0 | src/crud.ts:285 |
| B6 bulkWrite op update: conflicting paths | B6 bulkWrite op update: conflicting paths: p2:42 `{ $set: { address: {..}, "address.city": ".." } }` and p2:46 `$set`+`$unset` same field compile in bulk; standalone p2:44,48 error `Conflicting update | S2 | 8.0 | src/crud.ts:285 |
| B6 bulkWrite op update: pipeline | B6 bulkWrite op update: pipeline: p1:128 | S2 | 8.0 | src/crud.ts:285 |
| B6 bulkWrite op: arrayFilters `Document[]` | `[{}]` compiles | S2 | 8.0 | src/crud.ts:143 |
| B7 findOneAndX: returnDocument | OK (replace/update) / LENIENT (delete) | S2 | 8.0 | - |
| B7 findOneAndX: upsert | OK/LENIENT (delete) | S2 | 8.0 | src/crud.ts:115 |
| Hole: `updateOne(f, {})` server rejects | Hole: `updateOne(f, {})` server rejects: driver rejects client-side (update.js:58-60, utils.js:420) | S2 | 8.0 | src/crud.ts:200 |
| Hole: bulk `$[t]`, conflicts, `arrayFilters:[{}]` | Hole: bulk `$[t]`, conflicts, `arrayFilters:[{}]`: see B6 rows | S2 | 8.0 | - |
| Hole: bulkWrite([]) / insertMany([]) / updateOne(f,{}) | Hole: bulkWrite([]) / insertMany([]) / updateOne(f,{}): p1:94,98,100 | S2 | 8.0 | src/crud.ts:197 |
| Hole: findOneAndDelete returnDocument/upsert | Hole: findOneAndDelete returnDocument/upsert: p1:58 | S2 | 8.0 | src/crud.ts:241 |
| Hole: maxTimeMS -5 / skip -1 / limit 1.5 compile | type hardening | S2 | 8.0 | - |
| bulkWrite (pipeline update in op) | updateOne.update pipeline rejected | S2 | 8.0 | src/crud.ts:285 |
| bulkWrite([]) | empty compiles | S2 | 8.0 | src/crud.ts:285 |
| distinct (dot-path key) | `distinct("address.city")` rejected | S2 | 8.0 | src/crud.ts:280 |
| find (LENIENT sort key) | `sort: { nmae: 1 }` compiles | S2 (design?) | 8.0 | src/crud.ts:25 |
| find/findOne/findOneAnd* unknown option keys | any unknown key compiles when mixed with at least one valid key | S2 | n/a | src/crud.ts:185; src/registry.ts:93-104 |
| findOneAndDelete (returnDocument/upsert) | compile but dropped | S2 | 8.0 | src/registry.ts:195-210 |
| findOneAndReplace (_id required) | replacement without `_id` rejected | S2 | 8.0 | src/crud.ts:249 |
| findOneAndUpdate (pipeline) | pipeline form not accepted | S2 | 8.0 | src/crud.ts:258-268 |
| insertMany (_id required) | docs without `_id` rejected | S2 | 8.0 | src/crud.ts:197 |
| insertMany([]) | empty array compiles | S2 | 8.0 | src/crud.ts:197 |
| insertOne (_id required) | doc without `_id` rejected when schema has `_id` | S2 | 8.0 | src/crud.ts:196 |
| replaceOne (_id required) | replacement without `_id` rejected | S2 | 8.0 | src/crud.ts:227 |
| replaceOne (options unknown) | any options object compiles | S2 | 8.0 | src/crud.ts:230 |
| updateOne(f, {}) | empty update compiles | S2 | 8.0 | src/crud.ts:200 |
| updateOne/updateMany (pipeline + options) | pipeline form takes no options | S2 | 8.0 | src/crud.ts:214 |
| B1 find: allowDiskUse | B1 find: allowDiskUse: p1:72 compiles when mixed, ignored at runtime | S3 | 8.0 | src/crud.ts:183 |
| B1 find: batchSize | B1 find: batchSize: p1:68 rejected alone; p1:70 `{ limit: 1, batchSize: 1 }` compiles and is silently ignored (registry.ts:90-110 applies only sort/skip/limit/projection/hint/maxTimeMS/collation/comme | S3 | 8.0 | src/registry.ts:90-110 |
| B1 find: explain | B1 find: explain: crud.ts:28-44; docs lists explain | S3 | 8.0 | src/crud.ts:28-44 |
| B1 find: let | B1 find: let: p1:74 compiles when mixed (ignored at runtime in toList; passed through in toOne via registry.ts:108) | S3 | 8.0 | src/registry.ts:108 |
| B1 find: min / max | B1 find: min / max: not in crud.ts:28-37; docs options table lists min, max | S3 | 8.0 | src/crud.ts:28-37 |
| B1 find: noCursorTimeout | B1 find: noCursorTimeout: p1:157 rejected alone | S3 | 8.0 | src/crud.ts:183 |
| B1 find: readConcern | B1 find: readConcern: p1:76 compiles when mixed (ignored); docs lists readConcern | S3 | 8.0 | src/crud.ts:183 |
| B1 find: readPreference | B1 find: readPreference: crud.ts:28-37; docs lists readPreference | S3 | 8.0 | src/crud.ts:28-37 |
| B1 find: returnKey | B1 find: returnKey: crud.ts:28-37; docs lists returnKey | S3 | 8.0 | src/crud.ts:28-37 |
| B1 find: showRecordId | B1 find: showRecordId: p1:78 compiles when mixed, ignored | S3 | 8.0 | src/crud.ts:183 |
| B10 distinct: options | B10 distinct: options: p1:141 | S3 | 8.0 | src/crud.ts:280 |
| B11 aggregate: options | B11 aggregate: options: p1:102,104 | S3 | 8.0 | src/pipeline-types.ts:36-40; src/registry.ts:315-330 |
| B12 cursor-level execution options | B12 cursor-level execution options: crud.ts:39-44 FindBuilder only toList/toOne | S3 | 8.0 | src/crud.ts:39-44 |
| B13 deleteOne/deleteMany: hint/collation/let/writeConcern/maxTimeMS | B13 deleteOne/deleteMany: hint/collation/let/writeConcern/maxTimeMS: p1:133,135; docs deleteOne syntax lists all five | S3 | 8.0 | src/crud.ts:237 |
| B2 findOne: all find options | B2 findOne: all find options: p1:157; crud.ts:191-194; p2:36 mixed unknown `batchSize` compiles | S3 | 8.0 | src/crud.ts:191-194 |
| B3 insertMany: ordered | B3 insertMany: ordered: p1:139; docs insertMany: ordered default true | S3 | 8.0 | src/crud.ts:197 |
| B3 insertMany: writeConcern/bypass/let/comment | B3 insertMany: writeConcern/bypass/let/comment: p1:139; d.ts:1093-1111 | S3 | 8.0 | src/crud.ts:197 |
| B3 insertOne: bypassDocumentValidation/forceServerObjectId/comment | B3 insertOne: bypassDocumentValidation/forceServerObjectId/comment: p1:137; d.ts:5325-5330 only has bypassDocumentValidation, forceServerObjectId (comment via CommandOperationOptions); docs insertOne  | S3 | 8.0 (driver-level) | src/crud.ts:196 |
| B3 insertOne: writeConcern | B3 insertOne: writeConcern: p1:137; docs insertOne options: writeConcern | S3 | 8.0 | src/crud.ts:196 |
| B4 updateOne/Many: bypassDocumentValidation, writeConcern, comment | B4 updateOne/Many: bypassDocumentValidation, writeConcern, comment: p1:90 comment, p1:92 writeConcern rejected; docs updateOne lists writeConcern, bypassDocumentValidation | S3 | 8.0 | src/crud.ts:200 |
| B4 updateOne/Many: collation | B4 updateOne/Many: collation: p1:84 | S3 | 8.0 | src/crud.ts:200 |
| B4 updateOne/Many: let | B4 updateOne/Many: let: p1:88; docs updateOne: let "can only filter results inside $expr" | S3 | 8.0 | src/crud.ts:200 |
| B4 updateOne: sort | missing (New in 8.0) | S3 | 8.0 | src/crud.ts:200 |
| B6 bulkWrite op replaceOne | B6 bulkWrite op replaceOne: p1:126 rejected; docs replaceOne sort "New in version 8.0"; driver ReplaceOneModel via update.js:158 | S3 | 8.0 | src/crud.ts:227 |
| B6 bulkWrite op update: sort | B6 bulkWrite op update: sort: p1:124 rejected | S3 | 8.0 | src/crud.ts:285 |
| B6 bulkWrite: writeConcern/bypass/let/comment | B6 bulkWrite: writeConcern/bypass/let/comment: p1:120,122 | S3 | 8.0 | src/crud.ts:285 |
| B7 findOneAndX: let/writeConcern/bypass/includeResultMetadata | B7 findOneAndX: let/writeConcern/bypass/includeResultMetadata: p1:145,147,161; includeResultMetadata would change return type (d.ts:4689) | S3 | 8.0 | - |
| B8 countDocuments: readConcern/readPreference | B8 countDocuments: readConcern/readPreference: p1:149 | S3 | 8.0 | src/crud.ts:270 |
| B9 estimatedDocumentCount: maxTimeMS, comment | B9 estimatedDocumentCount: maxTimeMS, comment: p1:143 | S3 | 8.0 | src/crud.ts:278 |
| D3 async iteration | D3 async iteration: crud.ts:39-44 FindBuilder has only `_filter`, `_options`, `toList`, `toOne`; grep asyncIterator in src: none | S3 | n/a | src/crud.ts:39-44 |
| D4 stream() | D4 stream(): same; grep `.stream(` in src: none | S3 | n/a | src/crud.ts:39-44 |
| D5 explain() | D5 explain(): grep explain in src: none (only in docs comments for stages); docs find lists `explain` option | S3 | 8.0 | src/crud.ts:39-44 |
| D6 batchSize | D6 batchSize: crud.ts:28-44 | S3 | 8.0 | src/crud.ts:28-44 |
| D7 allowDiskUse cursor method | D7 allowDiskUse cursor method: crud.ts:28-44; docs find options list allowDiskUse | S3 | 8.0 | src/crud.ts:28-44 |
| D8 hasNext/next/forEach/map/close/count/min/max/returnKey/showRecordId/noCursorTimeout | D8 hasNext/next/forEach/map/close/count/min/max/returnKey/showRecordId/noCursorTimeout: crud.ts:39-44 | S3 | 8.0 | src/crud.ts:39-44 |
| E4 rawCollection absent in Effect | E4 rawCollection absent in Effect: Key facts; p1:131 | S3 | n/a | src/registryEffect.ts:28-65 |
| F11 writeConcern/readConcern per op | MISSING (core) | S3 | 8.0 | - |
| Hole: findOne `limit` ignored by driver | Hole: findOne `limit` ignored by driver: d.ts:4739-4745 FindOneOptions marks batchSize/limit/noCursorTimeout deprecated, "User provided value will be ignored" | S3 | n/a | src/crud.ts:191 |
| aggregate (no options) | no options at all | S3 | 8.0 | src/pipeline-types.ts:36-40; src/registry.ts:315-330 |
| aggregate (terminal) | aggregate (terminal): registry.ts:326; crud.ts has no cursor | S3 | n/a | src/registry.ts:326 |
| aggregate (toList collection arg ignored) | aggregate (toList collection arg ignored): see Key facts | S3 | n/a | src/pipeline-types.ts:38; src/registry.ts:326 |
| bulkWrite (writeConcern/comment/let/bypass) | method options missing | S3 | 8.0 | src/crud.ts:285 |
| countDocuments (options) | readConcern/readPreference missing | S3 | 8.0 | src/crud.ts:270 |
| deleteMany (no options) | deleteMany (no options): p1:135 `Expected 1 arguments, but got 2`; crud.ts:237-239 | S3 | 8.0 | src/crud.ts:237-239 |
| deleteOne (no options) | no hint/collation/let/writeConcern/maxTimeMS | S3 | 8.0 | src/crud.ts:233 |
| distinct (no options) | collation/maxTimeMS/hint/comment/readConcern missing | S3 | 8.0 | src/crud.ts:280 |
| estimatedDocumentCount (options) | no options (maxTimeMS) | S3 | 8.0 | src/crud.ts:278 |
| find (option subset) | FindOptions is a subset of the driver/docs options | S3 | 8.0 | src/crud.ts:28-37 |
| find (sort value forms) | only 1/-1; no $meta/"asc"/Map | S3 | 8.0 | src/crud.ts:25; src/registry.ts:96 |
| findOne (option subset) | findOne (option subset): crud.ts:191-194 uses FindOptions<C>; p1:157 `noCursorTimeout` rejected when alone | S3 | 8.0 | src/crud.ts:191-194 |
| findOneAndDelete (missing options) | let, includeResultMetadata missing | S3 | 8.0 | src/crud.ts:241 |
| findOneAndReplace (missing options) | let, writeConcern, bypassDocumentValidation, includeResultMetadata | S3 | 8.0 | src/crud.ts:112-121 |
| findOneAndUpdate (missing options) | let, writeConcern, bypassDocumentValidation, includeResultMetadata | S3 | 8.0 | src/crud.ts:258 |
| insertMany (no options) | no `ordered`/writeConcern | S3 | 8.0 | src/crud.ts:197 |
| insertOne (no options) | no options param (writeConcern etc.) | S3 | 8.0 | src/crud.ts:196 |
| registryEffect ignores prefix/resolveCollectionName | Effect registry has no options param | S3 | n/a | src/registryEffect.ts:68-77; src/registry.ts:366-369 |
| replaceOne (typed options missing) | upsert, hint, collation, sort (New in 8.0), let, bypassDocumentValidation, writeConcern, comment | S3 | 8.0 | src/crud.ts:227 |
| updateMany (options subset) | updateMany (options subset): crud.ts:214-225 same option type; p2:52 | S3 | 8.0 | src/crud.ts:214-225 |
| updateOne (options subset) | only upsert, hint, arrayFilters | S3 | 8.0 | src/crud/updates/types.ts:216-219 |
| updateOne (sort) | sort option missing, New in 8.0 | S3 | 8.0 | src/crud.ts:200 |

### 4.9 Cross-cutting hardening (stage position, terminal stages, version gating, test debt)

| Item | Gap | Severity | Version scope | Impl (file:line) |
|---|---|---|---|---|
| $documents: db-level only (new) | $documents: db-level only (new): Docs (documents, 8.0) "only ... database-level aggregation pipeline". sluice only offers `collection.aggregate` and `$documents(...).toList(collection?)` (pipeline-typ | S1 | 8.0 | src/pipeline-types.ts:36; src/registry.ts:326 |
| `$documents` cannot run through the library at all (db-level only) | `$documents` cannot run through the library at all (db-level only): see row above | S1 | 8.0 | src/sluice-stages.ts:470 |
| `$out` followed by `$count("n")` revives the result type to `{ n: number }` | `$out` followed by `$count("n")` revives the result type to `{ n: number }`: b2:30 error text "Type '{ n: number; }' is not assignable to type 'never'" | S1 | 8.0 | src/sluice-stages.ts:691 |
| `$project` missing-field poison type is accessible | `r[0]?.nope.__error` compiles | S1 | 8.0 | src/sluice-stages.ts:705 |
| `$unionWith` array-form pipeline is sent to the server as JS functions | runtime bug candidate | S1 | n/a | src/sluice-stages.ts:817-831 |
| `$unionWith` array-form pipeline runtime | `$unionWith` array-form pipeline runtime: sluice-stages.ts:817-831: only `typeof spec.pipeline === "function"` and `"stages" in spec.pipeline` are resolved | S1 | n/a | src/sluice-stages.ts:817-831; src/sluice-stages.ts:837-839 |
| `declare const` stages have no runtime value | src/sluice-stages.ts:962,984,1006,1029; exported src/index.ts:93-107 | S1 | n/a | src/sluice-stages.ts:962; src/index.ts:93 |
| dist/index.js lacks 4 runtime exports declared in dist/index.d.ts | package broken for those names | S1 | n/a | - |
| $changeStream must be first | `$match` then `$changeStream` compiles; two compile | S2 | 8.0 | src/sluice-stages.ts:896 |
| $collStats first stage | $collStats first stage: b1:36 (`$match, $collStats({count:{}})`) compiles | S2 | 8.0 | src/sluice-stages.ts:390 |
| $geoNear must be first | `$match` then `$geoNear` compiles; two $geoNear compile | S2 | 8.0 | src/sluice-stages.ts:514 |
| $indexStats first stage | $indexStats first stage: b1:37 `$match, $indexStats()` compiles | S2 | 8.0 | src/sluice-stages.ts:593 |
| $out/$merge must be last: stage after them | $out/$merge must be last: stage after them: Probes b2 (D rows): `$out(archive), $match(...)` b2:25 compiles; `$out, $limit(1)` b2:27 compiles; `$out, $count("n")` b2:29 compiles AND revives the type:  | S2 | 8.0 | src/sluice-stages.ts:691 |
| $unset: fields must exist | "WRONG over-strict: MongoDB accepts missing field" | S2 (design?) | 8.0 | src/sluice-stages.ts:850-852 |
| Stage ordering general: two $geoNear, two $out, $changeStream mid | Stage ordering general: two $geoNear, two $out, $changeStream mid: Two $geoNear compile (b1:35); `$changeStream` mid compiles (b1:31); two `$out` is REJECTED (b1:29, b2:33, via `C = never`, "Argument  | S2 | 8.0 | src/pipeline-types.ts:121-287 |
| Version: `registry("4.2")` accepts $setWindowFields | Version: `registry("4.2")` accepts $setWindowFields: b1:60-62 `registry("4.2", { events: EventSchema })(mockDb).events.aggregate($setWindowFields(...))` compiles (no error) | S2 | 5.0 | src/sluice-stages.ts:782 |
| `$currentOp`/`$listLocalSessions`/`$shardedDataDistribution` db-level only | `$currentOp`/`$listLocalSessions`/`$shardedDataDistribution` db-level only: b1:39 `users.aggregate($match(...), $currentOp({}))` and b1:40 `users.aggregate($currentOp({}))` compile | S2 | 8.0 | src/sluice-stages.ts:915 |
| `$where` in `ValidMatchFilterWithBuilder` | src/sluice.ts:477 | S2 | 8.0 | src/sluice.ts:476-477 |
| `declare const` stages typed callable on a collection pipeline | `declare const` stages typed callable on a collection pipeline: b1:41 `users.aggregate($match(...), $listSessions({}))` compiles | S2 | n/a | - |
| `pushStage(...) as unknown as <sig>` (about 33 sites) | cast is the only type check | S2 | n/a | src/sluice-stages.ts:599; src/runtime-utils.ts:28-30 |
| aggregate.test-d.ts: typo inside nested path / $and | aggregate.test-d.ts: typo inside nested path / $and: b2:81 `{ "profile.nope": true }` compiles; b2:86 `$and: [{ activ: true }]` compiles; b2:82 `{ "profile.bio": 5 }` is rejected (control: known path  | S2 | n/a | - |
| aggregate.test-d.ts:505-509 typo key | `$match($ => ({ activ: true }))` compiles | S2 | n/a (any) | - |
| registry `aggregate` runtime builder cast | registry.ts:318-330 | S2 | n/a | src/registry.ts:315-330; src/pipeline-types.ts:36 |
| type-safety-fixes.test-d.ts:82-88 top-level $not | `$match(() => ({ $not: {...} }))` compiles | S2 | 8.0 (rule from find semantics, docs page silent) | - |
| updateFailing.test-d.ts:421-425 duplicate identifier | live directive at 425 passes for another reason | S2 | n/a | - |
| updateFailing.test-d.ts:469-470 inverted test | asserts REJECTION of valid MQL "nested filtered positional on primitive arrays" | S2 | 8.0 | - |
| updateFailing: real duplicate-identifier shapes | updateFailing: real duplicate-identifier shapes: a6_upd_extra.ts:485 `arrayFilters: [{ i: {$type:"string"} }, { i: {$regex:/a/} }]` compiles; :493 `[{ "id.total": {...} }, { "id.orderId": {...} }]` (t | S2 | 8.0 | - |
| Version: `__minVersion` unused | Version: `__minVersion` unused: src/builder.ts:277 `readonly __minVersion = ""` constructor parameter; `grep -rn __minVersion src test` returns only that line | S3 | n/a | src/builder.ts:277 |
| Version: registry `version` argument unused | Version: registry `version` argument unused: src/registry.ts:363-364 `version: TVersion, // For later on when we have to deal with version-specific features`; nothing in lines 363-379 reads `version` | S3 | n/a | src/registry.ts:363-364; src/registry.ts:56 |
| `$accumulator` init/accumulate `(...args: any[]) => any` | builder.ts:1270-1307, accumulator-utils.ts:22,35 | S3 | 8.0 | src/builder.ts:1271; src/accumulator-utils.ts:22 |
| `$geoNear.query?: AnyDict` | sluice-stages.ts:524 | S3 | n/a | src/sluice-stages.ts:524 |
| `toList(collection?)` argument is ignored at runtime | type promises an override | S3 | n/a | src/pipeline-types.ts:36; src/registry.ts:326 |
| jsonschema.test-d.ts:56-75 extra property | KNOWN GAP: `$jsonSchema.properties.extraProp` compiles | S3 | n/a | - |
| jsonschema.test-d.ts:57 TODO "Implement intersection validation" | jsonschema.test-d.ts:57 TODO "Implement intersection validation": jsonschema.test-d.ts:57 TODO text present; 2 live directives are at :100 and :113 only (`type: 123`, `properties: 123`) | S3 | n/a | - |
| jsonschema.test-d.ts:77-92 type mismatch | `properties: { age: { type: "string" } }` compiles | S3 | n/a | - |
| registry `as unknown as Promise<T[]>` read casts | registry.ts:104,108,123 | S3 | n/a | src/registry.ts:104 |
| registry `bulkWrite` cast | registry.ts:282 | S3 | n/a | - |
| update brand type-only; `pipe` cast | stages/index.ts:49,62; migrate.ts:68,75 | S3 | n/a | - |

## 5. Tests to add

Grouped by target file. Format: what the test asserts, then the kind: runtime test (needs MongoDB), positive type test (`expectType` or an exact `Equal` check), or negative type test (`@ts-expect-error`). Use an exact `Equal` check for result types: `expectType` only checks assignability, so it can not catch a type that is too narrow or too wide. Where a file name ends with "(file proposed)", the verifier proposed a default file.

#### test/basic/basic.test.ts

- `users.rawCollection` is the driver collection — runtime test. (5.3: `rawCollection` has 0 tests)

#### test/operators/accumulators.test.ts

- closure var in `init` fails loudly (decide behaviour) — runtime test. ($accumulator (closures))
- leading nulls stay null — runtime test. ($locf)

#### test/operators/arrayOperators.test.ts

- -1 not found, null array gives null, start>end gives -1 — runtime test. ($indexOfArray (UNTESTED))

#### test/operators/convertOperator.test.ts

- `format` option with binData — runtime test. ($convert)
- `to: "binData"` with `format`; type test for `to: 16` — runtime test. ($convert)
- `$toUUID` string -> binData subtype 4; expression-operators.test-d.ts positive — positive type test. ($toUUID)

#### test/operators/dateOperators.test.ts

- month/week + startOfWeek — runtime test. ($dateDiff (no runtime test))

#### test/operators/expressions.test.ts

- `$log:[100,10]` = 2 — runtime test. ($log)
- array form; expression-operators.test-d.ts positive — positive type test. ($cond)
- concat with null field returns null — runtime test. ($concat (runtime null test))

#### test/operators/fieldMiscOperators.test.ts

- $match with $sampleRate — runtime test. ($sampleRate (UNTESTED runtime))

#### test/operators/stringOperators.test.ts

- `indexOfBytes("vanilla","ll",undefined,2)` must not compile or emit `[..,0,2]` — negative type test. ($indexOfBytes (lone end))
- `undefined,end` case — runtime test. ($indexOfCP (lone end))
- chars of 4097 bytes errors — runtime test. ($trim/$ltrim/$rtrim (chars > 4096 bytes))
- ASCII, "cafétéria",0,5 -> "café", mid-char error — runtime test. ($substrBytes (no tests))

#### test/stages/addFields.test.ts

- `$addFields({"a.b": ...})` nests — runtime test. ($addFields (existing runtime tests))

#### test/stages/changeStream.test.ts

- insert then read event — runtime test. ($changeStream (untested runtime))

#### test/stages/collStats.test.ts

- `$collStats:{storageStats:{}}` returns `ns` — runtime test. ($collStats (untested))

#### test/stages/documents.test.ts

- `db.aggregate([$documents...])` — runtime test. ($documents (untested runtime))

#### test/stages/lookup.test.ts

- `let` + `$expr` pipeline join — runtime test. ($lookup (`let` untested runtime))

#### test/stages/match.test.ts

- `$lte` filter — runtime test. ($lte)

#### test/stages/mergeOut.test.ts

- `$merge({into:{db,coll}})` emits `{db,coll}`; positive type test — positive type test. ($merge (into {db,coll}))
- `$out({db,coll,timeseries})` emits `timeseries` (runtime, needs 7.0.3+); negative type: unknown key `garbage` must error — negative type test. ($out (timeseries))

#### test/stages/sessions.test.ts

- import `$listLocalSessions` from the built package and run on db — runtime test. ($listLocalSessions (no runtime value))

#### test/stages/sort.test.ts

- `$match($text)` then `$sort({score:{$meta:"textScore"}})` — runtime test. ($sort (textScore runtime test))

#### test/stages/unionWith.test.ts

- `$unionWith({ coll, pipeline: [$match(...)] })` returns filtered docs — runtime test. (`$unionWith` array-form pipeline runtime)

#### test/types/failing/failureCases.test-d.ts

- `@ts-expect-error` on `boundaries: [5]` and `[]` — negative type test. ($bucket (boundaries `[5]`, `[]`))
- `@ts-expect-error` `default: 5` with `[0,5,10]` — negative type test. ($bucket (default inside range))
- only if a branded/literal check is adopted; else document — positive type test. ($bucketAuto (buckets 0/-1/2.5))
- `@ts-expect-error` bare field ref in `$bucketAuto.output` — negative type test. ($bucketAuto (output without accumulator))
- `@ts-expect-error` granularity + string groupBy — negative type test. ($bucketAuto (granularity with string gro)
- `@ts-expect-error` for each pair — negative type test. ($changeStream (mutual exclusion))
- `@ts-expect-error` `$changeStream` after `$match` — negative type test. ($changeStream (must be first))
- `@ts-expect-error` for the three — negative type test. ($count (string restrictions))
- `@ts-expect-error` — negative type test. ($densify (unit vs field type))
- `@ts-expect-error` string field — negative type test. ($densify (field type))
- `@ts-expect-error` for each — negative type test. ($facet (forbidden stages))
- `@ts-expect-error` `{from, as}` — negative type test. ($lookup (`{from, as}` alone))
- `$merge({into:c, on:"tags"})` must error — negative type test. ($merge (on array field))
- `whenMatched:[{$sort:..}]` must error — negative type test. ($merge (whenMatched pipeline))
- `aggregate($merge(...), $match(...))` must error — negative type test. ($merge / $out (terminal position))
- `$project(()=>({}))` must error — negative type test. ($project ({} empty))
- `$.slice("$tags",-2,1)` must error — negative type test. ($project ($slice negative n with positio)
- `$project({a:{$elemMatch:..}})` must error — negative type test. ($project ($elemMatch))
- `$redact("$age")` must error — negative type test. ($redact)
- `$.rank()` without sortBy must error — negative type test. ($setWindowFields (builder rank without s)
- `$sortByCount({a:"$x"})` must error — negative type test. ($sortByCount (literal document))
- `$.abs("$scores")` @ts-expect-error — negative type test. ($abs (array arg))
- `$.ln(-1)` @ts-expect-error; needs a literal-sign check in NumericArg — negative type test. ($ln)
- `$.log(100, 1)` @ts-expect-error — negative type test. ($log)
- place 100 and 1.5 @ts-expect-error (needs literal range type) — negative type test. ($round)
- `$.nor` does not exist (negative) after removal — negative type test. ($nor)
- `$.ifNull("$nnum")` and `$.ifNull()` @ts-expect-error (negative) — negative type test. ($ifNull)
- `branches: []` @ts-expect-error (needs tuple type `[B, ...B[]]`) — negative type test. ($switch)
- `$.toObjectId(5)` and `$.toObjectId("$num")` @ts-expect-error; positive: string and ObjectId field — negative type test. ($toObjectId)
- both @ts-expect-error — negative type test. ($binarySize)
- `// @ts-expect-error $.concat("$age"," years")` — negative type test. ($concat (existing neg test mis-targeted))
- `regex: /a/s`, `/a/g` rejected — negative type test. ($regexFind (regex flags))
- `$.split("$name", 7)` — negative type test. ($split (type-negative untested))
- `unit:"fortnight"` — positive type test. ($dateAdd (unit string))
- those cases — negative type test. ($dateFromParts (opts untyped))
- `$.toDate(true)` — negative type test. ($toDate (input unchecked))
- `// @ts-expect-error` filter limit 0 (needs literal check on NumericArg) — negative type test. ($filter (limit 0))
- `@ts-expect-error` `$.first("$name")` in $addFields — negative type test. ($first (non-array input, expression))
- `@ts-expect-error` `$.last(5)` — negative type test. ($last (non-array input, expression))
- `@ts-expect-error` firstN n 0 — negative type test. ($firstN (n: 0))
- negative type n 0 — negative type test. ($lastN (n: 0))
- negative type — negative type test. ($maxN (n: 0 / -1))
- `@ts-expect-error` end without start (design?: could be allowed with a warning) — negative type test. ($indexOfArray (end without start))
- `@ts-expect-error` step 0 (literal) — negative type test. ($range (step 0))
- `@ts-expect-error` setDifference 1 arg — negative type test. ($setDifference (UNTESTED type))
- `@ts-expect-error` setEquals("$tags") and setEquals() — negative type test. ($setEquals (arity))
- negative types (low priority, design?) — negative type test. ($setUnion (arity, expression))
- `@ts-expect-error` mergeObjects(5) and ("$name") — negative type test. ($mergeObjects (mergeObjects(5)))
- negative type (design? string "$x" is the only dynamic path today) — negative type test. ($getField ($-prefixed name))
- `@ts-expect-error` uppercase var name (key-template check) — negative type test. ($let (var names))
- `@ts-expect-error` sampleRate(2) — negative type test. ($sampleRate (range))
- `@ts-expect-error` `$.slice("$tags", 1, -2)` (needs a literal-sign check) — negative type test. ($slice (real hole: position with negativ)
- `@ts-expect-error` zip defaults of wrong length — negative type test. ($zip (wrong-length defaults))
- `@ts-expect-error` arrayToObject on array of non k/v docs — negative type test. ($arrayToObject (invalid element shape))
- negative types — negative type test. ($setIntersection (arity))
- `@ts-expect-error` unknown variable in $addFields — negative type test. (Unknown `$$var` accepted anywhere, typed)

#### test/types/failing/match-sort-safety.test-d.ts

- `$match(() => ({loc:{$near:...}}))` must error — negative type test. ($match ($near/$nearSphere))

#### test/types/failing/type-safety-fixes.test-d.ts

- `$replaceRoot("$name")` (string) must error — negative type test. ($replaceRoot (scalar newRoot))

#### test/types/failing/updateFailing.test-d.ts

- neg `@ts-expect-error` for `$currentDate:{stamp:{$type:"timestamp"}}` on a Date field — negative type test. ($currentDate (timestamp into Date field))
- neg `$inc:{nul:1}` for `number|null` field; runtime: `$inc` on null throws — negative type test. ($inc on nullable number)
- neg `$mul` on nullable; settle server behaviour with an integration test — negative type test. ($mul on nullable number)
- neg `$rename:{"items.$.id":"x"}` — negative type test. ($rename (positional path accepted))
- neg same-name rename — negative type test. ($rename (same name))
- neg rename-target vs `$set` target; integration test of server behaviour — negative type test. ($rename (target path not in conflict che)
- neg `$`-path + `{upsert:true}` — negative type test. ($ (positional) with upsert)
- neg `$[Bad]`, `$[my_id]`, `$[1x]` with matching arrayFilters — negative type test. ($[<identifier>] (identifier syntax))
- neg `$addToSet` with `$position` — negative type test. ($addToSet (with $position))
- neg `{$position:0}`, `{$slice:3}`, `{$sort:1}` without `$each` — negative type test. ($push (`$position`/`$slice`/`$sort` with)
- neg `$push:{tags:{}}` — negative type test. ($push (empty modifier object))
- neg `{$slice:3}` alone — negative type test. ($slice (without `$each`))
- neg `{$sort:1}` alone; integration `$each:[]` + `$sort:-1` — negative type test. ($sort (push, without `$each`))
- neg `{and:1.5}` only if an integer brand exists; runtime test — negative type test. ($bit (fractional operand))
- neg rename-target conflict — negative type test. (Conflicting paths (rename target vs othe)
- neg conflicting paths inside bulkWrite — negative type test. (Conflicting paths (bulkWrite `update`))
- neg `$set/$unset/$rename _id`; integration: server error on changing `_id` — negative type test. (_id immutability (`$set _id`))
- neg unknown option; positive `upsert/collation/hint/sort` — negative type test. (replaceOne (options typed `unknown`))
- neg bulk `$[i]` without arrayFilters — negative type test. (bulkWrite: updateOne/updateMany (positio)
- neg bulk empty/mismatched arrayFilters — negative type test. (bulkWrite: updateOne/updateMany (empty /)
- neg `update:{}` — negative type test. (bulkWrite: updateOne (empty update))

#### test/types/inference/aggregate.test-d.ts

- `$out({db:"d",coll:users2})` compiles — positive type test. ($out (positive type test))
- result type is `C | ForeignType` — positive type test. ($unionWith (type test))
- `$.eq(1, "$num")` positive — positive type test. ($eq)
- `$.switch({branches:[...]})` without default compiles + RT — positive type test. ($switch)

#### test/types/inference/documents.test-d.ts

- expression form if supported — positive type test. ($documents (expression argument))

#### test/types/inference/expression-operators.test-d.ts

- `expectType<number | null>` for `$.add("$nnum",1)`; fix = use NullishResultFromArgs — positive type test. (Numeric ops (abs, add, divide, multiply,)
- `$.add("$dec", 1)` on a Decimal128 field compiles — positive type test. (Numeric ops (decimal))
- expectType<number> per op — positive type test. ($ceil / $floor / $exp / $pow / $mod)
- `$.cmp("$score", 250)` compiles, result `-1|0|1` — positive type test. ($cmp)
- result types of convert (positive); b.ts S/B15,B16 show string|null and string today — positive type test. ($convert)
- `expectType<boolean>($.isNumber("$maybeNum"))` — positive type test. ($isNumber)
- `eq<$.concat("$nick","x") result, string|null>` — positive type test. ($concat (null typing))
- `number|null` for nullable field — positive type test. ($indexOfBytes (null typing))
- `expectType<-1|0|1>` — positive type test. ($strcasecmp (untested type))
- arrayElemAt result is `T | null` (or missing); positive type — positive type test. ($arrayElemAt (result nullability))
- concatArrays elem type of union; positive type — positive type test. ($concatArrays (no type test))
- firstN result is array type — positive type test. ($firstN (UNTESTED type))
- positive type — positive type test. ($lastN (UNTESTED type))
- result elem type — positive type test. ($maxN (UNTESTED type))
- boolean — positive type test. ($isArray (UNTESTED type))
- `{k:string; v: V}[]` — positive type test. ($objectToArray (UNTESTED type))
- same array type — positive type test. ($reverseArray (UNTESTED type))
- result type = input type — positive type test. ($sortArray (UNTESTED type))
- element type — positive type test. ($setIntersection (UNTESTED type))
- getField with `$.literal("price")` and `$.concat` — positive type test. ($getField (dynamic field typing))
- `$$REMOVE` gives `Omit<I, F>` — positive type test. ($setField ($$REMOVE value))
- Omit result — positive type test. ($unsetField (UNTESTED type))
- number — positive type test. ($rand (UNTESTED type))
- `$$NOW` field is `Date` — positive type test. ($$NOW (typed literal))
- `$$USER_ROLES` is `{_id; role; db}[]`; runtime on auth-enabled server — runtime test. ($$USER_ROLES (dotted/undotted))
- nested array path — positive type test. (Field path: dotted through arrays)
- document intended behaviour — positive type test. ($getField (unknown field vanishes))

#### test/types/inference/extended-inference.test-d.ts

- `$addFields` with `"a.b"` yields `{a:{b:number}}` (positive type, Equal); test/stages/addFields.test.ts: runtime nested doc — positive type test. ($addFields (dotted key type))
- `$addFields({nick:"$$REMOVE"})` result has no `nick` + runtime in addFields.test.ts — positive type test. ($addFields ($$REMOVE typing))
- `$project({"meta.score":1})` result is `{_id; meta:{score:number}}` — positive type test. ($project (dotted inclusion))
- `$unset("meta.score")` result `meta:{created}`; runtime dotted case in unset.test.ts — runtime test. ($unset (dotted/embedded paths))

#### test/types/inference/review-regressions.test-d.ts

- `$set(()=>({"meta.score":1}))` gives `meta:{created:Date;score:number}`; runtime dotted case in a new test/stages/set.test.ts — positive type test. ($set (dotted key result))

#### test/types/inference/stage-improvements.test-d.ts

- `$bucket({groupBy:"$when", boundaries:[Date,Date]})` compiles — positive type test. ($bucket (boundaries only numbers))
- `default: null` / Date compiles — positive type test. ($bucket (default type))
- with `output`, `"count"` not in keys (Equal, positive type) — positive type test. ($bucketAuto (count in result with output)
- `$bucketAuto` result type with and without output — positive type test. ($bucketAuto (no type test))
- `$changeStream({startAtOperationTime: new Timestamp(...)})` compiles; `@ts-expect-error` Date — negative type test. ($changeStream (startAtOperationTime type)
- `latencyStats.reads` is `{latency:number; ops:number; histogram?: {micros:number;count:number}[]}` (Equal, positive type) — positive type test. ($collStats (latencyStats type))
- `queryExecStats.collectionScans.total` is number — positive type test. ($collStats (queryExecStats type))
- `host: string` present — positive type test. ($collStats (host/shard/transactions, sto)
- `$currentOp({targetAllNodes:true})` compiles — positive type test. ($currentOp (targetAllNodes))
- both optional (Equal, positive type) — positive type test. ($currentOp (secs_running, microsecs_runn)
- legacy pair compiles + runtime on 2d index — positive type test. ($geoNear (legacy coordinate pair))
- expression form — positive type test. ($geoNear (maxDistance/minDistance expres)
- `building?: boolean` — positive type test. ($indexStats (building))
- `$bucketAuto({groupBy:"$price",buckets:4,output:$=>({total:$.sum("$price")})})` compiles and result has `total:number`, no `count` + runtime in bucket.test.ts — positive type test. ($bucketAuto (output accumulators cannot )
- with `preserveNullAndEmptyArrays:true` the index type is `number | null` (exact type, use `Equal`); runtime null index — positive type test. ($unwind (includeArrayIndex type))

#### test/types/inference/updateValidation.test-d.ts

- positive `$mul`/`$inc` on a Decimal128 field — positive type test. ($mul (operand/field types))
- positive `$min:{active:true}`; runtime boolean `$min` — positive type test. ($min (boolean field))
- positive `$min:{nul:5}`, ObjectId, Date — positive type test. ($min/$max (other rejected shapes, not in)
- positive docs nested `$elemMatch` example; runtime docs example — positive type test. ($pull ($elemMatch nested in element fiel)
- positive `$set` on `grades.$[t].questions.$[score]` and `.$[].questions.$[score]`; remove the TODO directive at updateFailing.test-d.ts:470 when fixed — positive type test. (arrayFilters (nested identifiers, `$set`)
- positive `collation`; integration with collation — positive type test. (updateOne/updateMany: collation)
- positive pipeline callback + `{upsert:true}`; integration pipeline upsert — positive type test. (updateOne/updateMany pipeline form + opt)
- positive `$min:{nul:5}` — positive type test. ($inc/$min/$max/$mul on `number|null`)

#### test/integration/ (new file) (file proposed)

- see next row — runtime test. (updateFailing.test-d.ts:421-425 duplicat)
- as above — runtime test. (jsonschema.test-d.ts:77-92 type mismatch)
- test/package check (scripts/check-package.mjs): every runtime-value export in index.d.ts must exist in dist/index.js; or implement the four with `pushStage` — runtime test. (`declare const` stages have no runtime v)
- validators are NOT needed if the signature is tightened (see $limit rows) — runtime test. (`pushStage(...) as unknown as <sig>` (ab)
- decide; either way add `$unset("nope")` test in type-safety-fixes.test-d.ts — runtime test. ($unset: fields must exist)
- test (needs Mongo): `$documents([{x:1}]).toList(...)` — runtime test. ($documents: db-level only (new))
- test with `pipeline: [$match(...)]` — runtime test. (`$unionWith` array-form pipeline is sent)
- scripts/check-package.mjs: compare d.ts value exports with runtime exports — runtime test. (dist/index.js lacks 4 runtime exports de)

#### test/integration/crud-extended.test.ts (file proposed)

- as find — runtime test. (findOne (option subset))
- insertMany ordered:false past duplicate key — runtime test. (insertMany (no options))
- updateOne with `{ sort: { score: -1 } }` updates the highest score — runtime test. (updateOne (sort))
- updateMany with collation, let + $expr — runtime test. (updateMany (options subset))
- type per option — runtime test. (replaceOne (typed options missing))
- replaceOne upsert; sort picks doc — runtime test. (replaceOne (no standalone test))
- deleteOne with collation — runtime test. (deleteOne (no options))
- deleteMany with let + $expr — runtime test. (deleteMany (no options))
- findOneAndUpdate with sort — runtime test. (findOneAndUpdate (sort))
- distinct with collation — runtime test. (distinct (no options))
- `bulkWrite(ops).execute({ ordered: false })` with a failing op must continue (fails today); BW:118 uses `ordered: true` (the default) so it cannot catch it — runtime test. (bulkWrite (execute args ignored))
- see find rows — runtime test. (B1 find: sort)
- find with hint, missing index throws — runtime test. (B1 find: hint)
- runtime — runtime test. (B1 find: collation)
- + type — runtime test. (B1 find: batchSize)
- with hint — runtime test. (B1 find: min / max)
- type — runtime test. (B1 find: noCursorTimeout)
- type + runtime — runtime test. (B1 find: readConcern)
- find with let + $expr — runtime test. (B1 find: let)
- decision needed — runtime test. (B1 find: explain)
- findOne with sort + projection — runtime test. (B2 findOne: all find options)
- hint on missing index throws — runtime test. (B4 updateOne/Many: hint)
- see replaceOne — runtime test. (B5 replaceOne: options (all))
- ordered:false via execute — runtime test. (B6 bulkWrite: ordered (execute arg))
- bulk updateOne with sort — runtime test. (B6 bulkWrite op update: sort)
- upsert null/after — runtime test. (B7 findOneAndX: upsert)
- countDocuments with limit/skip — runtime test. (B8 countDocuments: limit/skip/hint/maxTi)
- see D — runtime test. (B12 cursor-level execution options)
- per option — runtime test. (B13 deleteOne/deleteMany: hint/collation)
- for-await — runtime test. (D3 async iteration)
- decision — runtime test. (D8 hasNext/next/forEach/map/close/count/)
- same as bulkWrite — runtime test. (E3 bulkWrite.execute options in Effect)
- per op — runtime test. (F11 writeConcern/readConcern per op)
- type: findOne should not accept limit — runtime test. (Hole: findOne `limit` ignored by driver)
- same options give consistent results in toList/toOne — runtime test. (find toList vs toOne inconsistent option)
- registryEffect with prefix — runtime test. (registryEffect ignores prefix/resolveCol)

#### test/integration/hardcore-updates.test.ts (file proposed)

- same as $min — runtime test. ($max (boolean field))
- test: server error when filter lacks array — runtime test. ($ (positional) filter must contain the a)
- `$pop` 1 and -1 — runtime test. ($pop (UNTESTED runtime))
- as $push row — runtime test. ($position (without `$each`))
- `$position` 0, 2, -2 (docs examples) — runtime test. ($position (UNTESTED))
- `$each:[]` + `$slice:-3` — runtime test. ($slice (UNTESTED))
- update `$bit` and/or/xor on Int32 — runtime test. ($bit (UNTESTED for update))
- see identifier row — runtime test. (arrayFilters option (identifier syntax /)
- test — runtime test. (arrayFilters (two filter objects for the)
- updateMany upsert inserts one document — runtime test. (upsert (updateMany) UNTESTED)
- `$.project` in an update pipeline — runtime test. (Pipeline stage $project/$unset)
- replaceOne incl. upsert — runtime test. (replaceOne (UNTESTED standalone))
- see nested-identifier row — runtime test. (`$set` is the only operator rejected on )
- see execute row — runtime test. (Existing test mis-tests an option)

#### test/operators/accumulators.test.ts (file proposed)

- as above — runtime test. ($bottom)
- test/types/inference: `$group` `$first` of optional typed `T | null` — runtime test. ($first)
- test/types/inference: raw window `$first` includes null — runtime test. ($first (raw window))
- test/types/inference: accumulator result `T[]` for each — runtime test. ($firstN / $lastN / $maxN / $minN)
- as $first — runtime test. ($last)
- `$group` `$max` typed `| null` for optional — runtime test. ($max)
- test/integration: `$group` `$sum: ["$a","$b"]` runtime result — runtime test. ($max / $min / $sum (multi-arg))
- test/operators: `$project` `$.median` / `$.percentile` on an array field — runtime test. ($median / $percentile)
- test/types/inference: `med: number` (a.ts:50 shows it is `number`) — runtime test. ($median)
- `$group` `$min` typed `| null` for optional — runtime test. ($min)
- test/types/failing: `$.mergeObjects("$price")`; or integration run — runtime test. ($mergeObjects)
- single-document group returns null; type: `number | null` — runtime test. ($stdDevSamp)
- `$top` typed `| null` for optional source — runtime test. ($top)
- window result `number | null` for builder ops — runtime test. ($derivative / $integral / $expMovingAvg )
- result `| null` — runtime test. ($linearFill)
- builder shift without default `| null` — runtime test. ($shift (builder))
- remove, or runtime check on 8.0 server (needed to settle fully) — runtime test. ($percentRank)
- window result tests for min/top/stdDev/median — runtime test. (Window result typing)
- with `output`, result has no `count` unless asked — runtime test. ($bucketAuto)
- introduce mode in `Ret` — runtime test. ($accumulator in window / `$.add` etc.)
- see window result typing — runtime test. (builder window ops vs raw)

#### test/operators/arrayOperators.test.ts (file proposed)

- test: error on 0.5; type test not feasible without branded Int — runtime test. ($range (non-integer))
- timestamp to long; null gives null — runtime test. ($tsSecond (MISSING))
- timestamp ordinal — runtime test. ($tsIncrement (MISSING))
- TD: Timestamp type; runtime on a replica set — runtime test. ($$CLUSTER_TIME (typed literal))
- rename params to `(array, positionOrN, n?)`; doc test: slice(arr, 5, 10) — runtime test. ($slice (param names swapped))

#### test/operators/expressions.test.ts (file proposed)

- see Numeric ops row — runtime test. ($add / $subtract / $multiply / $divide /)
- same as above — runtime test. ($cmp)
- decision with user (same as $and) — runtime test. ($switch)
- RT: null -> false — runtime test. ($isNumber)
- same as $isNumber row — runtime test. ($isNumber)

#### test/operators/stringOperators.test.ts / dateOperators.test.ts (file proposed)

- same file: optional field also `string|null` — runtime test. ($concat (optional field))
- as indexOfBytes — runtime test. ($indexOfCP (null typing))
- test/types/inference: `captures` is `(string|null)[]`; runtime test `/(C(ar)*)ol/` on "Colleen" — runtime test. ($regexFind (captures null))
- as regexFind — runtime test. ($regexFindAll (captures null))
- null -> "" — runtime test. ($substrBytes (null field))
- n/a (could be template-literal type) — runtime test. ($dateToString (format any string))
- live 8.0 needed for null result — runtime test. (date-part ops (nullable field))
- same — runtime test. ($tsIncrement)
- test with multibyte chars — runtime test. (Doc conflict: ltrim/trim/rtrim chars uni)

#### test/stages/ (new file per stage) (file proposed)

- fix docs/MQL_COMPLIANCE.md:53 wording — runtime test. ($listSampledQueries (docs label))
- needs position/db mechanism (approval) — runtime test. ($currentOp (admin-only, first stage))
- test/stages/currentOp.test.ts on admin db — runtime test. ($currentOp (untested))
- see rows above — runtime test. ($densify (no type test))
- needs position mechanism (approval) — runtime test. ($geoNear (must be first))
- test/stages/indexStats.test.ts — runtime test. ($indexStats (untested))
- same test file — runtime test. ($listSessions (no runtime value))
- type test `_id.id` is Binary/UUID, `uid` Binary — runtime test. ($listSessions/$listLocalSessions (output)
- remove or justify — runtime test. ($listLocalSessions/$listSessions (extra )
- see runtime row above — runtime test. ($listLocalSessions/$listSessions (untest)
- fix label to "types only, no runtime" — runtime test. (docs/MQL_COMPLIANCE.md status labels)
- test/stages/ new planCacheStats.test.ts: `$planCacheStats()` emits `{$planCacheStats:{}}` — runtime test. ($planCacheStats (runtime absent))
- type test: output has `planCacheShapeHash`, `host`; use a union by `version` — runtime test. ($planCacheStats (output type))
- + type test for `$querySettings({showDebugQueryShape})`; fix MQL_COMPLIANCE.md:53 — runtime test. ($querySettings)
- test/stages/set.test.ts — runtime test. ($set (no stage runtime test))
- flip failureCases.test-d.ts:339-347; runtime dotted sortBy in setWindowFields.test.ts; a real 8.0 run would settle server behaviour — runtime test. ($setWindowFields (sortBy dotted))
- test/stages new: `$shardedDataDistribution()` emits `{$shardedDataDistribution:{}}` — runtime test. ($shardedDataDistribution (runtime absent)
- type test: `shards` is `{shardName:string;numOrphanedDocs:number;...}[]` — runtime test. ($shardedDataDistribution (output type))
- `$unionWith(users2)` shorthand (design: Collection object instead of string) — runtime test. ($unionWith (string shorthand))

#### test/stages/match.test.ts (file proposed)

- `$jsonSchema` filter on a real collection (test/integration) — runtime test. ($jsonSchema)
- decide after server run — runtime test. ($mod (divisor 0))
- server run — runtime test. ($regex (array field))
- needs stage-order design — runtime test. ($text (first stage))
- `$near` with distances (needs 2dsphere index) — runtime test. ($minDistance / $maxDistance)
- decide — runtime test. ($all (regex elements))

#### test/types/failing/failureCases.test-d.ts (file proposed)

- failureCases.test-d.ts negative (needs position mechanism = approval) — negative type test. ($collStats (must be first))
- failureCases.test-d.ts negative — negative type test. ($densify (date without unit))
- failureCases.test-d.ts negative (needs typed query filter) — negative type test. ($geoNear (query `$near`))
- failureCases.test-d.ts negative (design?: number cannot express ints) — negative type test. ($graphLookup (maxDepth))
- failureCases.test-d.ts negative (design?: needs numeric-literal refinement) — negative type test. ($limit (non-positive / fractional))
- negative type needs db-level aggregate typing (approval) — negative type test. ($listLocalSessions / $listSessions (coll)
- failureCases.test-d.ts negative (union of `{pipeline}` / `{localField,foreignField}` forms) — negative type test. ($lookup (localField without foreignField)
- negative type — negative type test. ($lookup (`let` without pipeline))
- failureCases.test-d.ts negative (needs stage filter = approval) — negative type test. ($lookup (`$out`/`$merge` in callback))
- version gate negative type test — negative type test. ($group accumulators ahead of 8.0)
- same file: `$match(() => ({$where:"x"}))` must error — negative type test. ($match ($where))
- numeric builder expression must error — negative type test. ($replaceRoot (expression newRoot))
- extra key must error — negative type test. ($replaceRoot (extra key))
- `$replaceWith("$name")` must error — negative type test. ($replaceWith (scalar / expression))
- extended-inference / failureCases: `$sample({size:0})`, `{size:-1}`, `{size:2.5}` must error — negative type test. ($sample (size))
- `window:{documents,range}` must error — negative type test. ($setWindowFields (window documents+range)
- `window:{documents:[..],unit:"day"}` must error — negative type test. ($setWindowFields (unit without range))
- `{$documentNumber:{}}` alone must error — negative type test. ($setWindowFields ($documentNumber withou)
- negative type for `$shift`, `$first`, `$last` without sortBy — negative type test. ($setWindowFields (order ops without sort)
- bounded window without sortBy must error — negative type test. ($setWindowFields (bounded window without)
- `window` on `$rank`/`$shift` must error — negative type test. ($setWindowFields (implicit-window ops wi)
- range window with 2 sortBy keys or descending key must error — negative type test. ($setWindowFields (range+unit multi/desc )
- `$skip(-1)`, `$skip(2.5)` must error — negative type test. ($skip)
- `includeArrayIndex:"$i"` must error (template-literal check) — negative type test. ($unwind (includeArrayIndex name))
- included in the `$project({})` negative test — negative type test. ($project ({} empty) result type)
- `$sortByCount("$nope")` must error — negative type test. ($sortByCount (unknown field))
- test/types/failing: `$.multiply()` @ts-expect-error only if the server rejects it; decide after a real server run — negative type test. ($multiply)
- decide with user; if kept, a negative test already exists (aggregate.test-d.ts:96-104) — negative type test. ($eq/$gt/$gte/$lt/$lte/$ne)
- failureCases.test-d.ts negative; needs literal-range types — negative type test. ($asin / $acos / $acosh)
- failureCases.test-d.ts negative once an int brand exists; RT: double input errors — negative type test. ($bitAnd / $bitOr / $bitXor / $bitNot)
- covered by negative test in $nor row — negative type test. ($nor)
- `options: "q"` — negative type test. ($regexFind (options string))
- `$.strLenBytes("$nick")` — negative type test. ($strLenBytes (nullable accepted))
- negative start -> "" ; negative length -> rest — negative type test. ($substr (deprecated alias, negatives))
- $function sum; negative type: lang other than "js" — negative type test. ($function (MISSING))
- design? negative type — negative type test. ($$REMOVE (anywhere))
- test/types/failing: `$setWindowFields($ => ({..., output:{r:$.accumulator(...)}}))` with @ts-expect-error — negative type test. ($accumulator (window))
- test/types/failing: `$.avg("$name")` with @ts-expect-error (if made strict) — negative type test. ($avg)
- test/types/failing: `n: 0` with @ts-expect-error (needs literal constraint) — negative type test. ($bottomN)
- test/types/failing: `$.median({input})` @ts-expect-error — negative type test. ($median)
- test/types/failing: `n: 0` @ts-expect-error (needs literal constraint) — negative type test. ($minN / $topN / $bottomN)
- `$.percentile({input,p})` @ts-expect-error — negative type test. ($percentile)
- `p: [1.5]` @ts-expect-error (literal constraint) — negative type test. ($percentile)
- test/types/failing: builder `$.denseRank()` without sortBy @ts-expect-error — negative type test. ($denseRank (builder))
- `$.rank()` without sortBy @ts-expect-error — negative type test. ($rank (builder))
- `{ $documentNumber: {} }` alone without sortBy @ts-expect-error — negative type test. ($documentNumber)
- `$.derivative({input})` @ts-expect-error — negative type test. ($derivative)
- `unit: "month"` @ts-expect-error; numeric sortBy + unit — negative type test. ($derivative)
- both @ts-expect-error (union of `{N}`/`{alpha}`) — negative type test. ($expMovingAvg)
- both @ts-expect-error — negative type test. ($integral)
- each @ts-expect-error without sortBy — negative type test. ($linearFill / $first / $last / bounded w)
- `default: "$price"` @ts-expect-error — negative type test. ($shift)
- both keys @ts-expect-error — negative type test. (Window bounds)
- `[-1.5,0]` @ts-expect-error (hard in TS) — negative type test. (Window bounds)
- each @ts-expect-error — negative type test. (Window `range`)
- `$.add` in window @ts-expect-error — negative type test. (Allowed window operators)
- `$group` with `$.add` @ts-expect-error — negative type test. ($group allowed accumulators)
- `output: { x: "$price" }` @ts-expect-error — negative type test. ($bucketAuto)
- test/types/failing: negative for $unionWith missing coll (b2:48 errors TS2741) and positive result `C | Foreign` — negative type test. (5.3: `$unionWith` has 0 type tests)
- restore the `@ts-expect-error` once filter type rejects unknown keys (needs `PathType` index-signature fix, see TODO text) — negative type test. (aggregate.test-d.ts:505-509 typo key)
- negative type tests for both — negative type test. (aggregate.test-d.ts: typo inside nested )
- restore negative test — negative type test. (type-safety-fixes.test-d.ts:82-88 top-le)
- negative `@ts-expect-error` for two filter docs with same identifier, after a server run confirms — negative type test. (updateFailing: real duplicate-identifier)
- when implemented: negative test with directive in these two blocks — negative type test. (jsonschema.test-d.ts:56-75 extra propert)
- negative once db-level aggregate type exists — negative type test. (`declare const` stages typed callable on)
- negative test in match-sort-safety.test-d.ts: `$match(() => ({ $where: "1" }))` must error; remove `$where` from the aggregate filter type (keep in CRUD find) — negative type test. (`$where` in `ValidMatchFilterWithBuilder)
- negatives in failureCases.test-d.ts: `$out(x), $match(...)`, `$out(x), $limit(1)`, `$merge(...), $count("n")`; implement via `Agg<TIn, TerminalMarker>` that no StageFunction accepts — negative type test. ($out/$merge must be last: stage after th)
- negative tests — negative type test. ($geoNear must be first)
- negative — negative type test. ($collStats first stage)
- negative + runtime test — negative type test. ($indexStats first stage)
- negative once a db-level pipeline builder exists — negative type test. (`$currentOp`/`$listLocalSessions`/`$shar)
- negative once gating exists — negative type test. (Version: `registry("4.2")` accepts $setW)
- negatives for two $geoNear, mid $changeStream — negative type test. (Stage ordering general: two $geoNear, tw)

#### test/types/failing/match-sort-safety.test-d.ts (file proposed)

- negative `@ts-expect-error` empty `$and` (needs non-empty tuple type) — negative type test. ($and (empty))
- two `$text` in `$and` — negative type test. ($and (two $text))
- empty `$or` — negative type test. ($or (empty))
- `$text` in `$nor` — negative type test. ($nor ($text))
- empty `$nor` — negative type test. ($nor (empty))
- top-level `$not` — negative type test. ($not (top-level))
- negative test documenting that raw MQL in `$expr` is rejected — negative type test. ($expr (raw MQL))
- negative `$options:"z"`, `"g"`; use letter union — negative type test. ($options)
- negative for the three cases — negative type test. ($text (nor/elemMatch/and))
- negative (aggregation only) — negative type test. ($text (in $or))
- `$where` in `$match` — negative type test. ($where (in $match))
- `$where` in `countDocuments` — negative type test. ($where (countDocuments))
- empty `$geoWithin` — negative type test. ($geoWithin (empty))
- server run, then negative — negative type test. ($geoWithin (multiple shapes))
- `$near` in `$match` — negative type test. ($near (in $match))
- negative — negative type test. ($near (countDocuments))
- `hint:{$natural:0}` — negative type test. ($natural)
- negative `@ts-expect-error` if kept strict — negative type test. (projection unknown key)
- negative when index signature is fixed — negative type test. (dotted path typo `"profile.nope"`)
- negative test with a valid key + unknown key in a flat schema — negative type test. (unknown top-level field)
- negative tests — negative type test. (top-level operators)
- negative `$type:"bogus"` in arrayFilters — negative type test. (arrayFilters `$type` (hardening list))
- Polygon in `$near` — negative type test. (`$near`/`$nearSphere` accept any Geometr)

#### test/types/failing/migration-and-crud.test-d.ts (file proposed)

- negative @ts-expect-error type: sort key that is neither keyof C nor a valid dot-path (needs path type) — negative type test. (find (LENIENT sort key))
- `insertMany([])` (non-empty tuple) — negative type test. (insertMany([]))
- `updateOne(f, {})` — negative type test. (updateOne(f, {}))
- `{ upsert: "yes" }`; positive: `{ upsert: true, sort }` — negative type test. (replaceOne (options unknown))
- `findOneAndDelete(f, { returnDocument: "after" })` — negative type test. (findOneAndDelete (returnDocument/upsert))
- remove the param (negative type: `toList(x)`) — negative type test. (aggregate (toList collection arg ignored)
- `bulkWrite([])` — negative type test. (bulkWrite([]))
- negative type once branded — negative type test. (B1 find: limit)
- negative type `skip: -1` — negative type test. (B1 find: skip)
- negative type — negative type test. (B1 find: maxTimeMS)
- negative type in test/types/failing — negative type test. (B6 bulkWrite op update: `$[t]` without a)
- negative type for delete; runtime "after" — negative type test. (B7 findOneAndX: returnDocument)
- negative types — negative type test. (Hole: maxTimeMS -5 / skip -1 / limit 1.5)
- negative @ts-expect-error type: `find(f, { limit: 1, foo: 1 })` — negative type test. (find/findOne/findOneAnd* unknown option )

#### test/types/failing/updateFailing.test-d.ts (file proposed)

- `$rename` incl. existing target; updateFailing: negatives above — negative type test. ($rename (UNTESTED))
- hint with an existing index; neg: hint on missing index errors — negative type test. (updateOne/updateMany: hint)
- `bulkWrite(ops).execute({ordered:false})` must continue past an error, or remove the parameter from crud.ts:174 (type @ts-expect-error test) — negative type test. (BulkWriteBuilder.execute(options))

#### test/types/inference/aggregate.test-d.ts (file proposed)

- test/stages/indexStats.test.ts runtime; `*.test-d.ts` expectType on result shape for $currentOp/$indexStats — positive type test. (5.3: `$currentOp`, `$indexStats`, `$list)
- test/basic: `aggregate($match(...)).toMQL()` equals the JSON string — positive type test. (5.3: `toMQL` has 0 tests)
- flip updateFailing.test-d.ts:469 to a positive compile test (remove directive) after fixing the path-value resolver; add `"x.$[].y.$[]"` positive — positive type test. (updateFailing.test-d.ts:469-470 inverted)
- type test: `$documents([...]).pipe(...)` positive Equal; negative mid-pipeline — positive type test. ($documents first stage)

#### test/types/inference/expression-operators.test-d.ts (file proposed)

- expression-operators.test-d.ts positive — positive type test. ($ln (type test))
- `$.eq("$num","$nnum")` compiles — positive type test. ($eq/$ne etc.)
- decide with user; positive: `$.and("$num", true)` — positive type test. ($and / $or)
- positive after decision — positive type test. ($cond)
- expression-operators.test-d.ts positive for each — positive type test. ($toBool / $toDecimal / $toDouble / $toLo)
- expression-operators.test-d.ts positive `$.cmp("$score", 250)` — positive type test. ($cmp)
- aggregate.test-d.ts positive — positive type test. ($eq family)
- positive type `string|null` — positive type test. ($ltrim (null typing))
- positive type — positive type test. ($rtrim (null typing))
- `find: /\\bblue\\b/` + runtime — positive type test. ($replaceOne (find as regex))
- `startDate: "$oid"` (needs DateIn widening) — positive type test. ($dateAdd (ObjectId/Timestamp rejected))
- `Date|null`, and `Date|typeof onNull|typeof onError` — positive type test. ($dateFromString (return type))
- positive type for boolean-widened union — positive type test. ($dateToParts (output shapes))
- positive type `string|null` (and `string|typeof onNull`) — positive type test. ($dateToString (null typing))
- positive type `Date|null` — positive type test. ($dateTrunc (null typing))
- expression-operators.test-d.ts + dateOperators.test.ts: `$.hour({date:"$d",timezone:"America/Chicago"})` — positive type test. (date-part ops object form)
- add to builder.ts + sluice.ts Timestamp input type; positive+negative type, runtime — positive type test. ($tsSecond)
- $documents example gives a Long; positive type — positive type test. ($toHashedIndexKey (MISSING))
- test/types/inference: positive, `$.accumulator` with zero-arg init and no `initArgs` compiles (and fix) — positive type test. ($accumulator)
- test/types/inference: positive, result type of `$.accumulator` with finalize — positive type test. ($accumulator)
- test/types/inference: `eq<R["bottom"], number | null>` for optional source — positive type test. ($bottom)
- positive tuple result type — positive type test. ($percentile)
- positive `number` — positive type test. ($stdDevPop)
- positive `T[]` — positive type test. ($topN)
- positive `number | null` — positive type test. ($covariancePop / $covarianceSamp)
- positive result-type tests — positive type test. ($integral / $expMovingAvg / $shift / $lo)
- builder window overload — positive type test. ($setWindowFields)
- flip failureCases.test-d.ts:338-348 to a positive test — positive type test. (sortBy dotted path)
- `eq<R[k], number | null>` for each — positive type test. (first/last/min/max/top/bottom/linearFill)
- add callback form with AccumulatorBuilder; positive test — positive type test. ($bucketAuto output)

#### test/types/inference/query-filters.test-d.ts (new file) (file proposed)

- test/types/inference (new file query-filters.test-d.ts): positive `$eq: null` on nullable, optional, non-nullable fields — positive type test. ($eq (null))
- same file: positive `$eq` on embedded doc, array, ObjectId, `_id` — positive type test. ($eq (embedded doc / array / ObjectId))
- same file: positive `$ne` on embedded doc — positive type test. ($ne (embedded doc))
- same file: positive `$ne: null` — positive type test. ($ne (null))
- positive `$gt` on nullable/optional string, number, Date — positive type test. ($gt (nullable/optional))
- positive `$gt` on boolean — positive type test. ($gt (boolean))
- positive `$gt/$lt` on ObjectId and `_id` — positive type test. ($gt (ObjectId))
- positive `$gte` nullable — positive type test. ($gte)
- positive `$lt` nullable — positive type test. ($lt)
- positive `$in` nullable/optional — positive type test. ($in (optional / nullable))
- positive `$in`/`$nin` Date — positive type test. ($in (Date))
- positive `$in` boolean — positive type test. ($in (boolean))
- positive `_id: {$in:[ObjectId]}` — positive type test. ($in (ObjectId))
- positive `$in` with RegExp elements — positive type test. ($in (regex element))
- positive `$nin` Date, bool, nullable — positive type test. ($nin)
- positive `$not` nullable, ObjectId, embedded — positive type test. ($not (per-field coverage))
- positive `$type:"number"` — positive type test. ($type ("number" alias))
- positive `$type: 2` — positive type test. ($type (numeric code))
- positive `$type:["double","string"]` — positive type test. ($type (array form))
- server run, then positive type — positive type test. ($mod (array field))
- positive `$regex` on nullable/optional — positive type test. ($regex (optional/nullable))
- positive type in find and `$match` — positive type test. ($geoIntersects)
- positive/negative per operator — positive type test. ($geometry)
- inline `$box` in `$match` — positive type test. ($box)
- inline `$center` in `$match` — positive type test. ($center)
- positive — positive type test. ($centerSphere)
- positive legacy `$near` in find — positive type test. ($near (legacy form))
- `$all` with `$elemMatch` — positive type test. ($all ($elemMatch))
- positive/decide — positive type test. ($all (scalar field))
- BinData bitmask — positive type test. ($bitsAllClear)
- BinData bitmask and BinData field — positive type test. ($bitsAllSet)
- + positive type — positive type test. ($ (positional projection))
- positive `true`/`false` — positive type test. (projection boolean values)
- positive for each form and context — positive type test. (equality embedded / array)
- positive `null` on optional and non-nullable fields — positive type test. (null vs missing (bare null))
- each inline literal in `$match` and in `find` — positive type test. (inline mutable-array literals in aggrega)
- positive on `_id` — positive type test. (`$eq`/`$in`/`$gt` on ObjectId)

#### test/types/inference/review-regressions.test-d.ts (file proposed)

- test/types/inference: positive type for each added option — positive type test. (find (option subset))
- `sort: { score: { $meta: "textScore" } }` — positive type test. (find (sort value forms))
- `insertOne({ name })` on `_id: ObjectId` schema — positive type test. (insertOne (_id required))
- positive type + runtime: insertOne with `{ writeConcern: { w: 1 } }` — positive type test. (insertOne (no options))
- `eq<IB["insertedId"], string>()` style positive type test (p1:32 fails today) — positive type test. (insertOne (InsertOneResult generic))
- positive type — positive type test. (insertMany (_id required))
- positive type + runtime per option — positive type test. (updateOne (options subset))
- `updateOne(f, $ => $.pipe(...), { upsert: true })` — positive type test. (updateOne/updateMany (pipeline + options)
- `findOneAndUpdate(f, $ => $.pipe($.unset("x")))` — positive type test. (findOneAndUpdate (pipeline))
- positive type + runtime — positive type test. (estimatedDocumentCount (options))
- dot-path key + result type — positive type test. (distinct (dot-path key))
- positive type with `eq<..., string[]>` for optional array — positive type test. (distinct (optional array element type))
- positive type + runtime: aggregate allowDiskUse/comment/maxTimeMS — positive type test. (aggregate (no options))
- `{ projection: { "address.city": 1 } }`, `{ name: true }` — positive type test. (B1 find: projection)
- positive type via Equal helper — positive type test. (C1 InsertOneResult/InsertManyResult gene)
- Effect collection exposes escape hatch — positive type test. (E4 rawCollection absent in Effect)

#### test/types/inference/stage-improvements.test-d.ts (file proposed)

- new stage + stage-improvements.test-d.ts positive; failureCases negative for use outside $changeStream — positive type test. ($changeStreamSplitLargeEvent)
- new stage + positive type test `{namespace}` and `{}` — positive type test. ($listSampledQueries)
- test/stages/fill.test.ts + type test: value-only fill without sortBy compiles — positive type test. ($fill (sortBy required))
- positive type test + runtime — positive type test. ($lookup (`from` omission with `$document)
- positive type + runtime test for `$planCacheStats({allHosts:true})` — positive type test. ($planCacheStats (allHosts))
- positive type + runtime for nested inclusion — positive type test. ($project (nested object form))
- result of `{"meta.score":0}` has `meta:{created}` — positive type test. ($project (dotted exclusion))
- positive type for `$project({s:$.meta("textScore")})` (existing tests only use `$addFields`, fieldMiscOperators.test.ts:442-502) — positive type test. ($project ($meta keywords))
- positive type for object form, or document callback-only as design — positive type test. ($setWindowFields (object form))
- `$sort({score:{$meta:"textScore"}})` must compile (design question: arbitrary key vs real path) — positive type test. ($sort ($meta key name))
- `$sortByCount($=>$.toLower("$name"))` (result `_id: string`) — positive type test. ($sortByCount (builder expression))
- positive type + runtime: `$unionWith({pipeline:[$documents([...])]})` — positive type test. ($unionWith (coll required))
- decide policy; positive type test for non-array path if allowed — positive type test. ($unwind (short string form on non-array))
- positive type in review-regressions.test-d.ts: `users.aggregate($set({age:1}))` result keeps `name` — positive type test. ($set (object form loses document type))

#### test/types/inference/updateValidation.test-d.ts (file proposed)

- upsert applies `$setOnInsert`, match does not (replace stub at nuclear-edge-cases.test.ts:194); updateValidation.test-d.ts positive — positive type test. ($setOnInsert (UNTESTED))
- docs `scores:[0,5]` example; updateValidation positive and updateFailing negative — positive type test. ($pullAll (UNTESTED))
- positive `let`; integration `$$var` in `$expr` filter — positive type test. (updateOne/updateMany: let)
- positive `sort` (updateOne); integration: sort picks the first match — positive type test. (updateOne: sort (New in 8.0))
- positive type per option; integration maxTimeMS — positive type test. (updateOne/updateMany: maxTimeMS, writeCo)
- positive type `update:[{ $set:{...} }]` in bulkWrite; integration — positive type test. (bulkWrite updateOne/updateMany: pipeline)
- positive type + integration for pipeline findOneAndUpdate — positive type test. (findOneAndUpdate: pipeline form)
- positive `writeConcern`; integration — positive type test. (bulkWrite options: writeConcern)
- positive type `collation/hint/maxTimeMS/let/writeConcern` for deleteOne/deleteMany; integration collation delete — positive type test. (deleteOne / deleteMany: options)

## 6. Out-of-scope candidates (maintainers decide)

These items are CONFIRMED differences, but they are newer than 8.0, Atlas-only, or driver and admin APIs. They are not counted as 8.0 gaps.

**Atlas-only (not core server)**
- `$search`, `$searchMeta`, `$vectorSearch` (commented out at `src/sluice-stages.ts:1060-1063`). The v8.0 pages say Atlas. A later docs version lists self-managed 8.2+ for `$search` and `$searchMeta` (not checked on a v8.0 page).
- `$listSearchIndexes` (error on non-Atlas since 7.1), `$queryStats` (Atlas M10+, "unsupported and not guaranteed stable").
- `$meta` keywords `searchSequenceToken`, `searchScoreDetails`, `searchRootDocumentId`, `vectorSearchScore`. Variable `$$SEARCH_META`.

**Newer than 8.0**
- No v8.0 docs page (404 or "not available in this version"): `$score`, `$scoreFusion`, `$minMaxScaler`, `$sigmoid`, `$percentRank` (sluice has `$percentRank`; see UNSURE list).
- 8.3 per the manual pages: `$hash`, `$hexHash`, `$createObjectId`, `arrayIndexAs` for `$filter`, `$map`, `$reduce`; `as` and `valueAs` for `$reduce`; variable `$$IDX`; expression form of `$topN` (UNSURE).
- `$encStrContains`, `$encStrStartsWith`, `$encStrEndsWith`, `$encStrNormalizedEq` (8.2, Queryable Encryption only).
- `$convert` options `base`, `byteOrder` and `to: "object"` or `"array"` (newer than 8.0; exact version not pinned). `format` and binData conversions are 8.0 and are listed in section 4.
- `$changeStream.ignoreRemovedShards` is not on the v8.0 page.
- `$group` accumulator forms of `$concatArrays` and `$setUnion` are not on the v8.0 `$group` page. Sluice offers only the expression forms, which are valid in 8.0. The version gate is a design question.

**Driver, admin and management APIs (no code exists in `src/`)**
- Collection and database administration: `createIndex`, `dropIndex`, `dropIndexes`, `listIndexes`, `getIndexes`, `drop`, `renameCollection`, `stats`, `db.command` and `runCommand`. Use `rawCollection` (Promise flavour only).
- Collection-level `watch` (change streams as a cursor), sessions, transactions (`startSession`, `withTransaction`), `session` option.
- Tailable cursors: `tailable`, `awaitData`, `maxAwaitTimeMS`. Cursor options for sharding or internals: `allowPartialResults`, `singleBatch`, `oplogReplay`, `timeout`.
- Time series collection creation, Queryable Encryption setup, `setQuerySettings` (the stage `$querySettings` is 8.0 core and is in section 4).

**Notes**
- `docs/MQL_COMPLIANCE.md:53` lists `$querySettings` and `$listSampledQueries` as "Atlas-only". The v8.0 pages say both work on self-managed deployments. The same file marks the four declare-only stages as "full" (lines 31-45).
- `$function` is deprecated in 8.0 (with `$accumulator` and `$where`). Decide whether to add it.

## 7. Refuted, unsure and unverified claims

Do not report these again without new evidence. Counts: 29 refuted, 45 unsure, 16 unverified, 4 first-pass OK rows that were false. UNSURE means the v8.0 docs are silent or inconsistent on the point, and only a real MongoDB 8.0 server run can settle it. Examples: the `$size` integer rule, `$mod` with divisor `0`, `$regex` on array fields, the number of shapes in `$geoWithin`, `$bit` with two operators, `$pull` with `$not`, `$dateAdd` with `amount: 1.5`, `$round` and `$trunc` with one argument, the bare form of `$not`, and the no-match value of `$regexFindAll`.


### 7.1 Refuted claims

- **$changeStream (ignoreRemovedShards)** (slice 01): v8.0 syntax block lists only allChangesForCluster, fullDocument, fullDocumentBeforeChange, resumeAfter, showExpandedEvents, startAfter, startAtOperationTime
- **$count (zero docs)** (slice 01): Result is an array (`toList`), so zero elements is representable; docs v8.0 count: "If the input dataset is empty, $count doesn't return a result." Not a type bug
- **$documents (placement after $match)** (slice 01): p4:13 `users.aggregate($match(...), $documents([...]))` is rejected: "Agg<...> not assignable to StageFunction<...>"; p4:15 also rejected in a collection pipeline
- **$setWindowFields ($locf missing)** (slice 02): src/builder.ts:1396-1397 `locf` exists; b.ts:64 `$.locf("$age")` compiles clean
- **$unionWith (coll with $documents pipeline)** (slice 02): b.ts:79 `{coll:users2,pipeline:[{$documents:...}]}` does NOT compile: error TS2353 "'$documents' does not exist in type ..."
- **$first (expression form UNTESTED)** (slice 05): RT exist: test/integration/dual-semantics.test.ts:96 (`e_first: $.first("$arr")` in $addFields) and test/validation/fieldValidation.test.ts:268
- **$last (expression form UNTESTED)** (slice 05): RT: test/integration/dual-semantics.test.ts:97, test/validation/fieldValidation.test.ts:269/750/1037
- **$slice (negative n with position)** (slice 05): Wire order for 3 args is [array, position, n]: builder emits `varargs("slice")(array, n, position)` (src/builder.ts:585-590) so 2nd arg is really position
- **$sortArray (sortBy 0 negative test)** (slice 05): p3.ts:50 `sortBy: 0` is already an error: "Type '0' is not assignable to type '-1 \| 1 \| Dict<-1 \| 1>'"
- **$getField (stale it.skip at fieldMiscOperators.test.ts:153-158)** (slice 05): Docs: expression `field` is 7.2+, so the feature works on 8.0, but the test runner uses mongo:7.0: test/utils/setup.ts:29-30 (`new MongoDBContainer("mongo:7.0")`, comment
- **$$NOW (UNTESTED)** (slice 05): TD test/types/inference/complexIntegration.test-d.ts:443 uses `endDate: "$$NOW"` in $dateDiff (input only; result type not asserted)
- **$$REMOVE (typed unknown)** (slice 05): p1.ts:126-127 `{r:"$$REMOVE"}` assigns to `never` (type is `never`, key present)
- **$count** (slice 06): c.ts:127 (d18) TS2322 "Type 'number' is not assignable to type 'never'" so `{x:1}` is rejected; a.ts:49 `cnt: number`; no `.count(` type test on builder
- **$concatArrays / $setUnion** (slice 06): v8.0 `.../concatArrays/` and `.../setUnion/` pages: documented only as expression operators; v8.0 `$group` accumulator list (`.../v8.0/.../group/`) has neither
- **Docs caveat** (slice 06): `https://www.mongodb.com/docs/v8.0/...` pages load labelled "Database Manual v8.0"; the `window-functions/<op>` 404s are irrelevant since `.../aggregation/<op>/` v8.0 wor
- **$type (null type)** (slice 07): `{nick:{$type:"null"}}` compiles: F:460 clean in a/f/c
- **$type (existing tests)** (slice 07): that line is the aggregation expression `$.type("$value")` in `$addFields`, not the query `$type` operator
- **$ (positional) UNTESTED** (slice 08): test/integration/hardcore-updates.test.ts:436-446: `updateOne($ => ({_id:"c1","arrayField.subId":"target"}), {$addToSet:{"arrayField.$.data":{$each:[2,4]}}})` is a runtim
- **$pull (top-level `$elemMatch` on array of docs, haiku example)** (slice 08): p1.ts:87 and p4.ts:31 rejected
- **replaceOne (upsert:"yes" server rejects)** (slice 09): Driver drops it before sending: update.js:143-145 `if (typeof options.upsert === 'boolean') { op.upsert = options.upsert; }`
- **B6 bulkWrite op deleteOne/deleteMany** (slice 09): Driver DeleteOneModel (d.ts:4247-4254) has only filter, collation, hint; sluice crud.ts:158-160 already matches
- **Hole: `replaceOne({ upsert: "yes" })` server rejects** (slice 09): see replaceOne row: driver drops non-boolean upsert (update.js:143)
- **Inventory: @ts-expect-error counts** (slice 10): `grep -cE '^\s*//\s*@ts-expect-error'`: type-safety-fixes live=13 (report 14; line 83 is prose "Restore `@ts-expect-error`"), aggregate live=7 (report 8; aggregate.test-d
- **Inventory: expectType counts** (slice 10): `grep -cE 'expectType(<\|\()'` gives call counts one lower in every file that imports it (aggregate 20 not 21, failureCases 19 not 20, complexIntegration 59, pathValidatio
- **Inventory: type-safety-fixes "2 disabled negatives"** (slice 10): Only one TODO block: type-safety-fixes.test-d.ts:82-83
- **Inventory: documents.test-d.ts "no assertion at all"** (slice 10): documents.test-d.ts:16,36,56 use `x satisfies Agg<unknown, {...}>` (3 assertions)
- **$project: inclusion of missing field** (slice 10): b2:59 `$project(() => ({ nope: 1 }))` compiles; the result element type is `{ _id: ObjectId; nope: { __error: "$project field does not exist: nope"; __tag: "OpaqueError";
- **$unionWith `$out`/`$merge` in pipeline** (slice 10): Array form b2:41-45 (`[$out(archive2)]`, `[$out(loose)]`, `[$out(looseDict)]`, `[$limit(1), $out(archive)]`, `[$merge({into: archive})]`) all error `Collection type misma
- **Version: $documents 5.1** (slice 10): Docs https://www.mongodb.com/docs/v8.0/reference/operator/aggregation/documents/ : "New in version 6.0." not 5.1

### 7.2 Unsure claims (a real MongoDB 8.0 server run is needed)

- **$currentOp (currentOpTime required)** (slice 01): src:937 `currentOpTime: Date` required
- **$geoNear (uniqueDocs)** (slice 01): src:527 `uniqueDocs?: boolean`; p1:111 compiles
- **$geoNear (key optional)** (slice 01): Docs v8.0: "you must use key" when several 2d/2dsphere indexes (and on time series)
- **$indexStats (key value type)** (slice 01): v8.0 page says only "Index key specification"; keys like `"text"`, `"2dsphere"` are strings in real index specs
- **$lookup (docs "$facet/$geoNear inside $lookup callback")** (slice 01): v8.0 lookup page forbids only `$out` and `$merge` (confirmed above); no probe for `$facet`/`$geoNear` in $lookup callback beyond the `$facet` rows (p1:92-97 used `$facet`
- **$out ({db, coll} form)** (slice 02): a.ts:61 `{db,coll}` compiles; a.ts:63 `{coll}` errors "Property 'db' is missing" (SS:699)
- **$setWindowFields ($percentRank phantom)** (slice 02): src/builder.ts:1399-1400, SS:130,154,205 implement `$percentRank`
- **$sort (bare {$meta})** (slice 02): b.ts:95 compiles clean (SS:171)
- **$sortByCount (string without `$`)** (slice 02): c.ts:17 compiles clean
- **$round / $trunc** (slice 03): builder.ts:392-396,386-389 `this.options("round")(num)` -> `{ $round: <expr> }`, not an array
- **$trunc (single arg)** (slice 03): Docs /trunc/: "{ $trunc: [ <number>, <place> ] }", single-arg inferred only
- **$not** (slice 03): builder.ts:653 `this.id("not")`
- **$toDate** (slice 03): a.ts A47 `$.toDate(true)` compiles
- **$toInt** (slice 03): a.ts A46 `$.toInt(new Date())` compiles
- **$atanh** (slice 03): Not re-fetched; docs text needed ($atanh page)
- **$regexFind (null input)** (slice 04): Docs regexFind: silent on null input ("If the operator does not find a match, the result of the operator is a null" only)
- **$regexFindAll (no-match value)** (slice 04): Docs regexFindAll: "If a match is not found, returns an empty array", but collation example shows `"resultObject" : null` for non-matches
- **$regexMatch (null input / non-match)** (slice 04): Docs regexMatch: "`false` if a match doesn't exist" but collation example shows `null`; null input not documented
- **$dateAdd (amount 1.5)** (slice 04): b-accept.ts:35 compiles
- **$dateFromString (timezone + trailing Z)** (slice 04): Docs: "you cannot specify the timezone argument" when string ends in Z
- **$dateTrunc (docs conflict)** (slice 04): Docs page says both: binSize "Defaults to 1" and returns null if any input field "except startOfWeek" is missing; same with startOfWeek default Sunday vs "null if unit is
- **$mergeObjects (non-doc runtime)** (slice 05): Docs say only documents/null ("ignores null input values"); behaviour for 5 undocumented
- **$sampleRate (only valid in $match)** (slice 05): Docs v8.0 page does not state a $match-only restriction; only example is `{ $match: { $sampleRate: 0.33 } }` (query form)
- **$meta (searchScore outside $search)** (slice 05): p1.ts:100 `$addFields` + `$.meta("searchScore")` compiles
- **$meta (indexKey typed Dict<string>)** (slice 05): p1.ts:102 shows `{ [x: string]: string }`
- **$topN expression (Notes item)** (slice 05): src/builder.ts:1261-1266 is documented against /operator/aggregation/topN/ (accumulator page)
- **$setField/$mergeObjects (value literal typing)** (slice 05): p2.ts:49 shows `{ a: number; b: string; z: 1 }`; p1.ts:112 `c: 1`
- **$median** (slice 06): Docs median page: silent on result with no numeric input
- **$rank / $denseRank / $documentNumber** (slice 06): Docs setWindowFields: "Rank operators ..
- **$expMovingAvg** (slice 06): c.ts d12c, d12d no error
- **$shift** (slice 06): c.ts d19b compiles
- **Window `range` + `unit`** (slice 06): c.ts d14d compiles
- **$comment** (slice 07): `$comment` compiles as filter key F:952-956 clean (src/sluice.ts:479)
- **$rand** (slice 07): `{ $rand: {} }` compiles at filter top level F:994/996/998 (consequence of the top-level index signature, see below)
- **$currentDate (target restricted to Date\|null, "docs allow any field")** (slice 08): p1.ts:27 (`count: true` rejected: TS2353)
- **$pull (`$not`, `$all`)** (slice 08): p1.ts:95 (`$not` rejected), p1.ts:97 (`$all` rejected, TS2353)
- **$bit (two ops in one object)** (slice 08): p1.ts:17 clean
- **insertOne (string _id required)** (slice 09): p1:27 rejects
- **B1 find: limit (server rejects 1.5/negative)** (slice 09): Docs command/find say type non-negative integer but give no error text for 1.5 or negatives
- **B1 find: skip (server rejects)** (slice 09): docs type "positive integer", no error text; needs live 8.0
- **B6 bulkWrite op: arrayFilters `[{}]` rejected by server** (slice 09): docs positional-filtered page does not cover empty filter docs
- **C4 BulkWriteResult** (slice 09): d.ts:1132-1138 insertedIds/upsertedIds are index maps with `any` values (class BulkWriteResult)
- **Hole: "docs say findOneAndUpdate/findOneAndReplace sort New in 8.0"** (slice 09): docs findOneAndUpdate/Replace pages carry no version note on `sort`; only updateOne and replaceOne have "New in version 8.0" for sort (docs updateOne/replaceOne)
- **distinct on optional scalar** (slice 09): p1:50 `(string \| undefined)[]`
- **5.2 coverage table spot-check (18 rows)** (slice 10): Held exactly: $sortByCount 5/0, $merge 2/1, $redact 1/2, $replaceRoot 5/3, $replaceWith 3/1, $setWindowFields 5/4, $graphLookup 3/2, $bucket 1/3, $unionWith 1/0, $bucketA

### 7.3 Unverified claims

- **$changeStream (allChangesForCluster admin-only)** (slice 01): v8.0 fetch did not state the admin-db restriction; not probed
- **$currentOp (8.x output fields)** (slice 01): src has ~26 fields (src:928-957); v8.0 field list not enumerated against the page; low value
- **$densify (8.1 prefix rule)** (slice 01): Not in 8.0 docs fetched (8.0 page only has the 8.0 equal-bounds change); out of scope
- **$fill (8.0.20 linear change)** (slice 01): runtime server behaviour ("Changed in 8.0.20" on the page); not typed and not checkable by types
- **$limit (runtime `$limit(0)` error)** (slice 01): server run needed
- **$match ($text first stage)** (slice 02): Not probed: pipeline position is not expressed in the `$match` type (src/sluice.ts:463-481)
- **$project ($$REMOVE in computed field)** (slice 02): Not probed (needs builder support for `$$REMOVE` as value)
- **$sort (searchScore)** (slice 02): SS:168 allows it; not probed because it needs an Atlas Search pipeline to be meaningful
- **$project (literal $meta test)** (slice 02): Not probed
- **$dateFromParts (null)** (slice 04): Docs page fetched via summary: null behaviour not stated; needs live 8.0
- **date-part ops (null behaviour)** (slice 04): year page: "does not document how $year handles null or missing"; no live server allowed
- **Doc conflict: $dateToString %w vs dateFromString %w; indexOfCP "byte length"** (slice 04): dateFromString format table and indexOfCP start>length rule not re-fetched
- **$mergeObjects** (slice 06): Not probed for result type; docs: "ignores null input values; all null returns {}"
- **implicit AND (type tests)** (slice 07): multi-key filters compile in my probes (E:44 etc.), but whether an existing `*.test-d.ts` asserts multi-key typing needs semantic reading of many files; not done
- **5.3: `bulkWrite` 1 runtime file, 0 type files; `migrate` 1 runtime, 1 type** (slice 10): Not re-counted (outside my focus)
- **builder.ts:336 cast, sluice.ts:186-188 `any`** (slice 10): builder.ts:336 is `})) as unknown as PipelineBuilder<C>;` (read) but impact on public types not analysed; sluice.ts:186 `V extends (c: any) => infer R ? R` is a condition

### 7.4 First-pass OK rows that were false

- **$in** (slice 05): Signature fine (p3.ts:27-28 gives boolean; non-array `"$name"` rejected p3.ts:39). But grep of test/types for `$.in(` / `.in(`: zero hits. Only RT (test/operators/expressions.test.ts:251)
- **$literal** (slice 05): Signature fine. grep test/types for `.literal`: zero hits; only RT (test/operators/fieldMiscOperators.test.ts:314-368, convertOperator.test.ts)
- **$[]** (slice 08): Single level works: p1.ts:67 clean, runtime hardcore-updates.test.ts:326 (`$mul` with `"arrayField.$[].data.$[idx]"`). But the docs path `grades.$[].questions.$[score]` is rejected for `$set`: p4.ts:41 TS2322, and `$[].$[]` rejected p2.ts:114, p2.ts:116, p3.ts:65 (`orders.$[].items.$[]`). See nested-identifier row. Pas
- **bulkWrite options: ordered** (slice 08): Implementation is fine: registry.ts:279-281 `bulkWrite: (operations, options) => ({ execute: () => mongoCol.bulkWrite(operations, options) })`; p2.ts:90 clean. The cited runtime test bulk-write.test.ts:106-123 does NOT exercise the option: it calls `.execute({ ordered: true })` (line 118), which is ignored (see execute