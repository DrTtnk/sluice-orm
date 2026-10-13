import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

const sourceFiles = (directory: string): string[] =>
  readdirSync(directory, { withFileTypes: true }).flatMap(entry =>
    entry.isDirectory() ? sourceFiles(join(directory, entry.name))
    : entry.name.endsWith(".ts") ? [join(directory, entry.name)]
    : [],
  );

describe("Package exports", () => {
  it("has no `export declare const`, because it type-checks but has no runtime value", () => {
    const declareOnly = sourceFiles("src").flatMap(file =>
      readFileSync(file, "utf8")
        .split("\n")
        .flatMap((line, index) =>
          /^export declare (const|function|let|var)\b/.test(line) ? [`${file}:${index + 1}: ${line}`] : [],
        ),
    );

    expect(declareOnly).toEqual([]);
  });
});
