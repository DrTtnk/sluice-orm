---
sidebar_position: 2
---

# Installation

## Prerequisites

- **Node.js 18+**
- **TypeScript 5.0+**
- **MongoDB 4.0+** (for MongoDB 8.0 features, use version 8.0+)

## Install Sluice

```bash
npm install sluice-orm mongodb
```

## Optional Dependencies

### Schema Validation

Sluice is **schema-agnostic** - you can use any validation library or plain TypeScript types:

```bash
# Effect Schema (recommended)
npm install @effect/schema

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

- **mongodb**: `^6.0.0` - MongoDB driver
- **effect**: `^3.0.0` - Only if using Effect integration

## Development Dependencies

For development and testing:

```bash
npm install --save-dev typescript @types/node vitest
```

## Next Steps

- **[Quick Start](./quick-start.md)** — Your first type-safe pipeline
- **[Core Concepts](./core-concepts/schemas.md)** — Deep dive into schemas