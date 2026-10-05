import { readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

// Native tsc emits the ESM declarations. Mirror their relative references for
// CommonJS consumers so each exports condition resolves a matching module kind.
async function mirror(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) await mirror(path);
    else if (entry.name.endsWith(".d.ts")) {
      const source = await readFile(path, "utf8");
      await writeFile(
        path.replace(/\.d\.ts$/, ".d.cts"),
        source
          .replace(/(from\s+["']|import\(["'])(\.[^"']*)\.js(["'])/g, "$1$2.cjs$3")
          .replace(/^\/\/# sourceMappingURL=.*$/gm, ""),
      );
    }
  }
}
await mirror("dist");
