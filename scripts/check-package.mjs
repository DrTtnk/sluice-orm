import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const workspace = process.cwd();
const directory = await mkdtemp(join(tmpdir(), "sluice-package-"));
const npmCli = process.env.npm_execpath;
if (!npmCli) throw new Error("Run this check via npm run test:package");
const run = (command, args, cwd = workspace) =>
  execFileSync(command, args, {
    cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"],
  });

try {
  const [packed] = JSON.parse(
    run(process.execPath, [
      npmCli,
      "pack",
      "--json",
      "--ignore-scripts",
      "--pack-destination",
      directory,
    ]),
  );
  await writeFile(join(directory, "package.json"), JSON.stringify({ private: true }));
  // Deliberately omit optional and development dependencies from the library.
  run(
    process.execPath,
    [
      npmCli,
      "install",
      join(directory, packed.filename),
      "@types/node",
      "--ignore-scripts",
      "--omit=dev",
      "--omit=optional",
      "--no-audit",
      "--no-fund",
    ],
    directory,
  );
  const consumer = `
import { registry, registryEffect, $group } from "sluice-orm";
import { Effect } from "effect";
import type { Db } from "mongodb";
declare const db: Db;
const schemas = { users: { Type: null! as { _id: string; name: string; age: number } } };
const users = registry("8.0", schemas)(db).users;
const projected: Promise<{ name: string }[]> = users.find(() => ({}), { projection: { name: 1, _id: 0 } }).toList();
const grouped: Promise<{ _id: null; count: number }[]> = users.aggregate($group($ => ({ _id: null, count: $.sum(1) }))).toList();
// @ts-expect-error - packaged update types must preserve required fields
users.updateOne(() => ({}), $ => $.pipe($.unset("name")));
Effect.gen(function* () {
  const r = yield* registryEffect("8.0", schemas);
  const result: { _id: null; count: number }[] = yield* r.users.aggregate($group($ => ({ _id: null, count: $.sum(1) }))).toList();
  // @ts-expect-error - the packaged Effect API validates update value types
  r.users.updateOne(() => ({}), { $inc: { name: 1 } });
  return result;
});
`;
  for (const extension of ["mts", "cts"])
    await writeFile(join(directory, `consumer.${extension}`), consumer);
  run(process.execPath, [
    resolve("node_modules/@typescript/native/bin/tsc"),
    "--ignoreConfig",
    "--noEmit",
    "--strict",
    "--module",
    "nodenext",
    "--target",
    "esnext",
    "--skipLibCheck",
    "false",
    "--types",
    "node",
    "--typeRoots",
    join(directory, "node_modules/@types"),
    join(directory, "consumer.mts"),
    join(directory, "consumer.cts"),
  ]);
  run(process.execPath, ["--input-type=module", "-e", "await import('sluice-orm')"], directory);
  run(process.execPath, ["-e", "require('sluice-orm')"], directory);
  console.log("Packed ESM and CommonJS consumers compile and import successfully.");
} catch (error) {
  if (error.stdout) process.stderr.write(error.stdout);
  throw error;
} finally {
  await rm(directory, { recursive: true, force: true });
}
