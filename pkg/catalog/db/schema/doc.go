// Package schema provides serialization utilities for storing GraphQL schema
// metadata in database tables. It has zero runtime dependencies — it can be
// compiled and tested without importing engine, database, or provider packages.
//
// Functions:
//
//   - MarshalDirectives / UnmarshalDirectives: ast.DirectiveList ↔ JSON
//   - MarshalType / UnmarshalType: *ast.Type ↔ string
//   - ClassifyType / ClassifyField: directive-based HugrType classification
package schema
