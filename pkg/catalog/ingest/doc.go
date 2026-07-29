// Package ingest is the WRITE side of the catalog: it takes one data source's
// SDL and turns it into the physical model the catalog storage persists.
//
// What it does, in phase order (Default):
//
//   - VALIDATE — the source's declarations are well formed and internally
//     consistent, and its `extend type` blocks are legal;
//   - PREPARE — the source's own extensions are merged into its definitions,
//     every object type is tagged with @catalog, and the source's prefix is
//     applied to type names and to every reference that names one;
//   - FINALIZE — @join and @function_call are checked against what they point
//     at, in the source and across the seam through the target provider.
//
// VALIDATE and PREPARE mutate the source's definitions IN PLACE. That is the
// contract the storage relies on: it walks the source afterwards
// (store.collect) rather than a separate output. What Compile returns is only
// what could not go in place — the extensions and the @dependency set.
//
// There is no generation here. The served GraphQL surface — filters, mutation
// inputs, aggregations, module roots — is built on READ by the catalog storage
// from the catalog.* tables (pkg/catalog/store/gen_*.go). The GENERATE and
// ASSEMBLE phases exist in the phase list and have no rules; the pipeline skips
// an empty phase.
//
// The package was carved out of pkg/catalog/compiler in design-036, when that
// package's other half — 5 600 lines of generation rules — was deleted. The
// name says what is left: not a compiler that produces a schema, but the step
// that admits a source into the catalog.
package ingest
