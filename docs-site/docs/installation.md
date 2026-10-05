---
sidebar_position: 2
---

# Installation

## Prerequisites

- **Node.js 20+**
- **TypeScript 5.9+** for consumers; native **TypeScript 7.0.2** for repository builds
- **MongoDB 4.0+** (for MongoDB 8.0 features, use version 8.0+)

## Install Sluice

```bash
npm install sluice-orm mongodb effect
```

For TypeScript consumers, install `@types/node` and include `"node"` in
`compilerOptions.types` when your compiler configuration restricts ambient types.

## Optional Dependencies

### Schema Validation

Sluice is **schema-agnostic** - you can use any validation library or plain TypeScript types:

```bash
# Effect Schema (recommended)
npm install effect

# Or Zod
npm install zod

# Or plain TypeScript (no runtime validation)
# No additional dependencies needed
```

### Effect Integration

For functional programming with Effect.ts:

```bash
npm install effect
```

## Peer Dependencies

Sluice has minimal peer dependencies:

- **mongodb**: `^6.18.0` - MongoDB driver
- **effect**: `^3.19.14` - Required by the current shared entry point, including Promise-only usage

## Development Dependencies

For repository development, use `npm ci`. The lockfile installs native TypeScript 7
for builds and a TypeScript 6 API compatibility alias for ESLint. Public declaration
dependencies are installed for consumers too.

## Next Steps

- **[Quick Start](./quick-start.md)** — Your first type-safe pipeline
- **[Core Concepts](./core-concepts/schemas.md)** — Deep dive into schemas