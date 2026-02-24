package compare

// DiffKind categorizes a difference between two schemas.
type DiffKind int

const (
	DiffMissing DiffKind = iota // present in old, absent in new
	DiffExtra                   // absent in old, present in new
	DiffChanged                 // present in both, but different
)

func (k DiffKind) String() string {
	switch k {
	case DiffMissing:
		return "MISSING"
	case DiffExtra:
		return "EXTRA"
	case DiffChanged:
		return "CHANGED"
	default:
		return "UNKNOWN"
	}
}

// Diff represents a single structural difference between two schemas.
type Diff struct {
	Path    string   // dot-separated path, e.g. "types.User.fields.name"
	Kind    DiffKind // MISSING, EXTRA, or CHANGED
	Message string   // human-readable description
}

// Result holds comparison output: actual diffs and known issues.
type Result struct {
	Diffs       []Diff
	KnownIssues []Diff
}

// Equal returns true when there are no unexpected diffs.
func (r *Result) Equal() bool {
	return len(r.Diffs) == 0
}
