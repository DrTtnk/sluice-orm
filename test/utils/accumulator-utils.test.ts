import { describe, expect, it } from "vitest";

import { functionToString } from "../../src/accumulator-utils.js";

describe("functionToString", () => {
  describe("traditional functions", () => {
    it("should preserve traditional function syntax", () => {
      const fn = function (x: number, y: number) {
        return x + y;
      };
      const result = functionToString(fn);
      expect(result).toContain("function");
      expect(result).toContain("x");
      expect(result).toContain("y");
      expect(result).toContain("return x + y");
    });

    it("should handle function with no parameters", () => {
      const fn = function () {
        return 42;
      };
      const result = functionToString(fn);
      expect(result).toContain("function");
      expect(result).toContain("return 42");
    });

    it("should handle multi-statement functions", () => {
      const fn = function (a: number, b: number) {
        const sum = a + b;
        const product = a * b;
        return {
          sum,
          product,
        };
      };
      const result = functionToString(fn);
      expect(result).toContain("function");
      expect(result).toContain("const sum");
      expect(result).toContain("const product");
    });
  });

  describe("arrow functions", () => {
    it("should convert simple arrow function with parentheses", () => {
      const fn = (x: number, y: number) => x + y;
      const result = functionToString(fn);
      expect(result).toMatch(/^function\s*\([^)]*\)/);
      expect(result).toContain("return");
      expect(result).toContain("x + y");
    });

    it("should convert arrow function with single parameter (no parentheses)", () => {
      const fn = (x: number) => x * 2;
      const result = functionToString(fn);
      expect(result).toMatch(/^function\s*\([^)]*\)/);
      expect(result).toContain("return");
      expect(result).toContain("x * 2");
    });

    it("should convert arrow function with block body", () => {
      const fn = (x: number, y: number) => {
        const sum = x + y;
        return sum;
      };
      const result = functionToString(fn);
      expect(result).toMatch(/^function\s*\([^)]*\)/);
      expect(result).toContain("const sum");
      expect(result).toContain("return sum");
    });

    it("should handle arrow function with no parameters", () => {
      const fn = () => 42;
      const result = functionToString(fn);
      expect(result).toMatch(/^function\s*\(\s*\)/);
      expect(result).toContain("return");
      expect(result).toContain("42");
    });

    it("should handle arrow function returning object literal", () => {
      const fn = (x: number) => ({ value: x });
      const result = functionToString(fn);
      expect(result).toMatch(/^function\s*\([^)]*\)/);
      expect(result).toContain("return");
      // Note: object literal in arrow is tricky, may need parens
    });

    it("should handle complex multi-parameter arrow function", () => {
      const fn = (state: { count: number }, value: number) => {
        state.count += value;
        return state;
      };
      const result = functionToString(fn);
      expect(result).toMatch(/^function\s*\([^)]*\)/);
      expect(result).toContain("state.count");
      expect(result).toContain("return state");
    });
  });

  describe("real-world accumulator functions", () => {
    it("should convert init function", () => {
      const init = (seedValue: number, salt: string) => ({
        hash: seedValue,
        count: 0,
        salt,
      });
      const result = functionToString(init);
      expect(result).toMatch(/^function\s*\([^)]*\)/);
      expect(result).toContain("return");
    });

    it("should convert accumulate function", () => {
      const accumulate = (state: { hash: number; count: number }, email: string) => {
        for (let i = 0; i < email.length; i++) {
          const char = email.charCodeAt(i);
          state.hash = (state.hash << 5) - state.hash + char;
          state.hash |= 0;
        }
        state.count++;
        return state;
      };
      const result = functionToString(accumulate);
      expect(result).toMatch(/^function\s*\([^)]*\)/);
      expect(result).toContain("for");
      expect(result).toContain("charCodeAt");
    });

    it("should convert merge function", () => {
      const merge = (
        state1: { hash: number; count: number; salt: string },
        state2: { hash: number; count: number; salt: string },
      ) => ({
        hash: state1.hash + state2.hash,
        count: state1.count + state2.count,
        salt: state1.salt,
      });
      const result = functionToString(merge);
      expect(result).toMatch(/^function\s*\([^)]*\)/);
      expect(result).toContain("return");
    });

    it("should convert finalize function", () => {
      const finalize = (state: { hash: number; count: number; salt: string }) =>
        state.salt + state.hash.toString(16) + "_" + String(state.count);
      const result = functionToString(finalize);
      expect(result).toMatch(/^function\s*\([^)]*\)/);
      expect(result).toContain("return");
      expect(result).toContain("toString(16)");
    });
  });
});
