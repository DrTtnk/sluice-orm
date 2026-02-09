import { execSync } from "child_process";
import { existsSync } from "fs";
import { join } from "path";
import { describe, expect,it } from "vitest";

describe("Documentation Build Validation", () => {
  it("should build Docusaurus documentation successfully", () => {
    // Change to docs-site directory and run build
    const docsSitePath = join(process.cwd(), "docs-site");
    process.chdir(docsSitePath);

    // This should not throw an error
    expect(() => {
      execSync("npm run build", { stdio: "pipe" });
    }).not.toThrow();

    // Verify build output exists
    expect(existsSync(join(docsSitePath, "build"))).toBe(true);
    expect(existsSync(join(docsSitePath, "build", "index.html"))).toBe(true);
  });

  it("should contain advanced typings documentation", () => {
    const advancedTypingsPath = join(__dirname, "..", "docs-site", "docs", "advanced-typings.md");

    expect(existsSync(advancedTypingsPath)).toBe(true);

    // Could add more specific content validation here if needed
  });
});
