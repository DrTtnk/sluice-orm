#!/usr/bin/env bash

# Build script for @sluice/sluice
# This script is used in CI/CD pipelines

set -e

echo "🏗️  Building @sluice/sluice..."

# Clean previous build
echo "🧹 Cleaning previous build..."
rm -rf dist/

# Type check
echo "🔍 Running type checks..."
npm run test:types

# Lint
echo "🧹 Running linter..."
npm run lint

# Build
echo "📦 Building TypeScript..."
npm run build

# Verify build output
echo "✅ Verifying build output..."
if [ ! -f "dist/index.js" ]; then
  echo "❌ Build failed: dist/index.js not found"
  exit 1
fi

if [ ! -f "dist/index.d.ts" ]; then
  echo "❌ Build failed: dist/index.d.ts not found"
  exit 1
fi

echo "🎉 Build completed successfully!"
echo "📦 Output available in dist/"