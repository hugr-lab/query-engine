package types

import "time"

// DateTime represents a naive date-time value WITHOUT timezone.
// Used by the DateTime scalar to distinguish from Timestamp (which uses time.Time directly).
// Engine SQLValue dispatches on this type to cast as TIMESTAMP instead of TIMESTAMPTZ.
type DateTime time.Time
