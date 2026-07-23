// Package store is the entity-storage catalog provider (design 034).
//
// Unlike pkg/catalog/db — which shreds a FULLY compiled GraphQL schema into
// the _schema_* tables — this provider stores only the BASE entities that
// arrive from sources (modules, data objects, their declared fields,
// functions, source-defined types and relations) in the CoreDB `catalog`
// schema, and reconstructs the served GraphQL schema on the fly at read time.
//
// The served schema is composed of three layers:
//
//   - the SYSTEM layer — scalars, introspection (__*), @system types and the
//     base/query/GIS directives. These are assembled at startup from the
//     binary (static.New, same set the old provider persisted under the
//     "_system" catalog) and held in memory. They are NEVER written to
//     catalog.*: the writer skips sdl.IsSystemType and the reader serves them
//     from the static layer.
//
//   - the SOURCE entities — reconstructed from catalog.* on demand (ForName /
//     Types synthesis). Data objects, source types, functions.
//
//   - the SYNTHESIZED roots and derived types — Query/Mutation/Subscription
//     and per-module roots (never stored, always assembled), plus the derived
//     helpers (_X_aggregation, filters, …) generated lazily by name.
//
// Write path (a partial compiler: prefix + validate + extension merge, NO type
// generation) → collect base entities → Reconcile into catalog.*. Read path is
// cached with per-data-source invalidation (schemaCache, ported from
// pkg/catalog/db): a reconcile evicts only the changed/removed sources.
//
// All SQL addresses the tables literally as `core.catalog.<table>`: the CoreDB
// is always attached as `core` (production and tests alike, via the coredb
// Source) and DML runs from the DuckDB context on both backends (implicit
// VARCHAR→STRUCT casts on DuckDB, INOUT assignment casts on Postgres), so the
// prefix is a constant and the statement texts match core-db's pinned
// probeCatalogStatements verbatim.
//
// The store is flag-switched against pkg/catalog/db during the transition and
// replaces it once read-side generation reaches parity.
package store
