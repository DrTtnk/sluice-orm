#!/usr/bin/env bash

# Release script for @sluice/sluice
# This script handles the release process

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if we're on main/master branch
BRANCH=$(git branch --show-current)
if [[ "$BRANCH" != "main" && "$BRANCH" != "master" ]]; then
  echo -e "${RED}❌ Error: Must be on main/master branch to release${NC}"
  exit 1
fi

# Check if working directory is clean
if [[ -n $(git status --porcelain) ]]; then
  echo -e "${RED}❌ Error: Working directory is not clean. Please commit or stash changes.${NC}"
  exit 1
fi

# Get the version to release
CURRENT_VERSION=$(node -p "require('./package.json').version")
echo -e "${YELLOW}Current version: ${CURRENT_VERSION}${NC}"

# Ask for new version
read -p "Enter new version (current: $CURRENT_VERSION): " NEW_VERSION

if [[ -z "$NEW_VERSION" ]]; then
  echo -e "${RED}❌ Error: Version cannot be empty${NC}"
  exit 1
fi

# Validate version format
if ! [[ $NEW_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?$ ]]; then
  echo -e "${RED}❌ Error: Invalid version format. Use semantic versioning (e.g., 1.2.3 or 1.2.3-beta.1)${NC}"
  exit 1
fi

echo -e "${YELLOW}Preparing release v${NEW_VERSION}...${NC}"

# Update package.json version
npm version $NEW_VERSION --no-git-tag-version

# Build the project
echo -e "${YELLOW}Building project...${NC}"
npm run build

# Run tests
echo -e "${YELLOW}Running tests...${NC}"
npm test

# Commit the version change
echo -e "${YELLOW}Committing version change...${NC}"
git add package.json package-lock.json
git commit -m "chore: release v${NEW_VERSION}"

# Create git tag
echo -e "${YELLOW}Creating git tag...${NC}"
git tag "v${NEW_VERSION}"

# Push commits and tags
echo -e "${YELLOW}Pushing to remote...${NC}"
git push origin $BRANCH
git push origin "v${NEW_VERSION}"

# Publish to npm
echo -e "${YELLOW}Publishing to npm...${NC}"
npm publish --access public

echo -e "${GREEN}🎉 Successfully released @sluice/sluice v${NEW_VERSION}!${NC}"
echo -e "${GREEN}📦 Published to npm: https://www.npmjs.com/package/@sluice/sluice${NC}"
echo -e "${GREEN}🏷️  GitHub release: https://github.com/zefiro/sluice/releases/tag/v${NEW_VERSION}${NC}"