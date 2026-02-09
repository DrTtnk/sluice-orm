#!/usr/bin/env bash

# Build script for @sluice/sluice
set -e

echo "Building @sluice/sluice..."

rm -rf dist/

echo "Running type checks..."
npm run type-check

echo "Building with tsup..."
npm run build

# Verify build output
for f in dist/index.js dist/index.cjs dist/index.d.ts; do
  [ -f "$f" ] || { echo "Build failed: $f not found"; exit 1; }
done

echo "Build completed successfully!"