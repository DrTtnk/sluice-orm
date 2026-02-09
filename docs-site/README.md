# Sluice ORM Documentation

This directory contains the Docusaurus documentation site for Sluice ORM.

## Development

```bash
# Install dependencies
npm install

# Start development server
npm start

# Build for production
npm run build

# Serve production build locally
npm run serve
```

## Deployment

The documentation is automatically deployed to GitHub Pages via the workflow in `.github/workflows/deploy-docs.yml`.

## Structure

- `docs/` - Documentation pages
- `src/` - React components and custom pages
- `static/` - Static assets (images, etc.)
- `docusaurus.config.ts` - Docusaurus configuration
- `sidebars.ts` - Sidebar navigation configuration

## Key Features

### Advanced Typings Showcase

The documentation includes a comprehensive showcase of Sluice's advanced typing capabilities, particularly:

- **Discriminated Unions**: Type-safe handling of polymorphic data with union types
- **Real-world Examples**: Analytics events, notifications, and complex domain models
- **Type-safe Aggregation**: Conditional field access and variant-specific operations
- **Array of Unions**: Processing arrays containing discriminated union types

### API Reference

Complete API documentation including:

- All aggregation pipeline stages
- Expression operators and accumulators
- CRUD operations
- Effect integration
- Type definitions

### Examples

Real-world examples covering:

- E-commerce analytics
- User behavior analysis
- Content management systems
- Social media analytics
- Real-time dashboards

## Contributing

When adding new documentation:

1. Follow the existing structure in `docs/`
2. Update `sidebars.ts` if adding new sections
3. Use proper frontmatter for metadata
4. Include code examples with syntax highlighting
5. Test locally with `npm start`

## Advanced Typings Content

The `advanced-typings.md` page showcases Sluice's most powerful feature: handling complex union types with full type safety. It demonstrates:

- Creating discriminated unions with literal types
- Type-safe field access within union variants
- Conditional expressions for variant-specific logic
- Processing arrays of union types
- Real-world patterns like event sourcing and multi-tenant applications

This documentation serves as both a reference and a demonstration of Sluice's advanced type system capabilities.