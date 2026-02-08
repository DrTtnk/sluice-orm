import eslint from "@eslint/js";
import tslint from "typescript-eslint";
import unusedImports from "eslint-plugin-unused-imports";
import simpleImportSort from "eslint-plugin-simple-import-sort";
import { rules as customRules } from "./eslint-rules.js";

const E = "error";
const W = "warn";
const O = "off";

export default tslint.config(
  { ignores: ["node_modules/", "dist/", "*.js", "*.mjs", ".turbo/"] },
  eslint.configs.recommended,
  {
    plugins: {
      "unused-imports": unusedImports,
      "simple-import-sort": simpleImportSort,
    },
    extends: [...tslint.configs.strictTypeChecked, ...tslint.configs.stylisticTypeChecked],
    files: ["src/**/*.ts", "test/**/*.ts"],
    rules: {
      "@typescript-eslint/no-unsafe-call": W,
      "@typescript-eslint/no-unsafe-argument": W,
      "@typescript-eslint/no-unsafe-member-access": W,
      "@typescript-eslint/no-unsafe-assignment": W,
      "@typescript-eslint/await-thenable": W,
      "@typescript-eslint/unbound-method": [E, { ignoreStatic: true }],
      "@typescript-eslint/restrict-template-expressions": [E, { allowNumber: true }],
      "@typescript-eslint/no-confusing-void-expression": [E, { ignoreArrowShorthand: true }],
      "arrow-body-style": [O, "never"],
      "object-shorthand": [E, "always"],
      "@typescript-eslint/consistent-type-definitions": [E, "type"],
      "prefer-arrow-callback": [O, { allowNamedFunctions: false }],
      "@typescript-eslint/no-unused-vars": [
        O,
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
        },
      ],
      "@typescript-eslint/array-type": [E, { default: "array", readonly: "array" }],
      "@typescript-eslint/no-explicit-any": W,
      "unused-imports/no-unused-imports": E,
      "simple-import-sort/imports": E,
      "simple-import-sort/exports": E,
      "@typescript-eslint/no-unnecessary-type-parameters": O,
      "@typescript-eslint/no-redundant-type-constituents": W,
      "@typescript-eslint/consistent-indexed-object-style": O,
      // Custom rules
      "custom/no-expect-typeof": E,
      "custom/aggregate-must-tolist": E,
    },
    languageOptions: {
      parserOptions: {
        project: "./tsconfig.test.json",
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
  // Add custom rules
  {
    files: ["src/**/*.ts", "test/**/*.ts"],
    plugins: {
      custom: {
        rules: customRules,
      },
    },
    rules: {
      "custom/no-expect-typeof": E,
      "custom/aggregate-must-tolist": E,
    },
  },
);
