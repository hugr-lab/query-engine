package types

func init() {
	// Core scalars (12)
	Register(&stringScalar{})
	Register(&intScalar{})
	Register(&floatScalar{})
	Register(&booleanScalar{})
	Register(&bigIntScalar{})
	Register(&dateScalar{})
	Register(&timestampScalar{})
	Register(&datetimeScalar{})
	Register(&timeScalar{})
	Register(&intervalScalar{})
	Register(&jsonScalar{})
	Register(&geometryScalar{})

	// Engine-specific scalars (5) — initially in types/, to be migrated to engine packages later
	Register(&intRangeScalar{})
	Register(&bigIntRangeScalar{})
	Register(&timestampRangeScalar{})
	Register(&h3CellScalar{})
	Register(&vectorScalar{})
}
