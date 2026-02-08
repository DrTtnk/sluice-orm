/**
 * Custom ESLint rules for the project
 */

export const rules = {
  "no-expect-typeof": {
    meta: {
      type: "problem",
      docs: {
        description: "Disallow expectTypeOf, use expectType instead",
        category: "Best Practices",
        recommended: true,
      },
      messages: {
        noExpectTypeOf: "Use expectType instead of expectTypeOf",
      },
      schema: [],
    },
    create(context) {
      return {
        CallExpression(node) {
          if (node.callee.name === "expectTypeOf") {
            context.report({
              node,
              messageId: "noExpectTypeOf",
            });
          }
        },
      };
    },
  },

  "aggregate-must-tolist": {
    meta: {
      type: "problem",
      docs: {
        description: "aggregate calls must be followed by .toList()",
        category: "Best Practices",
        recommended: true,
      },
      messages: {
        aggregateMustToList: "aggregate() must be followed by .toList()",
      },
      schema: [],
    },
    create(context) {
      return {
        CallExpression(node) {
          // Check if this is a call to aggregate
          if (
            node.callee.type === "MemberExpression" &&
            node.callee.property.name === "aggregate"
          ) {
            // Check if the parent is a MemberExpression with property 'toList'
            const ancestors = context.sourceCode.getAncestors(node);
            const parent = ancestors[ancestors.length - 1];
            if (
              !parent ||
              parent.type !== "MemberExpression" ||
              parent.property.name !== "toList" ||
              parent.object !== node
            ) {
              context.report({
                node,
                messageId: "aggregateMustToList",
              });
            }
          }
        },
      };
    },
  },
};