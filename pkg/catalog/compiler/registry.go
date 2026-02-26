package compiler

import "github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"

var globalRules []base.Rule

// RegisterRules adds rules to the global rule registry.
// Typically called from init() in rules/ or engine packages.
func RegisterRules(rules ...base.Rule) {
	globalRules = append(globalRules, rules...)
}

// GlobalRules returns all registered rules.
func GlobalRules() []base.Rule {
	return globalRules
}
