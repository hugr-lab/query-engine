package compare

// CompareOption configures comparison behavior.
type CompareOption func(*compareConfig)

type compareConfig struct {
	skipSystemTypes     bool
	skipTypes           map[string]bool
	ignoreDescriptions  bool
	ignoreDirectiveArgs map[string]bool
	ignoreDirectives    map[string]bool
	allowExtraTypes     bool
	knownIssues         map[string]bool // paths to move to KnownIssues
}

func defaultConfig() *compareConfig {
	return &compareConfig{
		skipTypes:           make(map[string]bool),
		ignoreDirectiveArgs: make(map[string]bool),
		ignoreDirectives:    make(map[string]bool),
		knownIssues:         make(map[string]bool),
	}
}

// SkipSystemTypes excludes types starting with "__" from comparison.
func SkipSystemTypes() CompareOption {
	return func(c *compareConfig) {
		c.skipSystemTypes = true
	}
}

// SkipTypes excludes specific types by name from comparison.
func SkipTypes(names ...string) CompareOption {
	return func(c *compareConfig) {
		for _, n := range names {
			c.skipTypes[n] = true
		}
	}
}

// IgnoreDescriptions ignores description differences on types and fields.
func IgnoreDescriptions() CompareOption {
	return func(c *compareConfig) {
		c.ignoreDescriptions = true
	}
}

// IgnoreDirectiveArgs ignores argument differences on specified directives.
func IgnoreDirectiveArgs(directives ...string) CompareOption {
	return func(c *compareConfig) {
		for _, d := range directives {
			c.ignoreDirectiveArgs[d] = true
		}
	}
}

// IgnoreDirectives skips named directives entirely during comparison.
// Both missing and extra occurrences of these directives are ignored.
func IgnoreDirectives(names ...string) CompareOption {
	return func(c *compareConfig) {
		for _, n := range names {
			c.ignoreDirectives[n] = true
		}
	}
}

// AllowExtraTypes treats types present in new but missing in old as non-diffs.
func AllowExtraTypes() CompareOption {
	return func(c *compareConfig) {
		c.allowExtraTypes = true
	}
}

// KnownIssues marks diffs at specified paths as known issues,
// moving them to Result.KnownIssues instead of Result.Diffs.
func KnownIssues(paths ...string) CompareOption {
	return func(c *compareConfig) {
		for _, p := range paths {
			c.knownIssues[p] = true
		}
	}
}
