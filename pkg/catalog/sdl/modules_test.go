package sdl

import (
	"testing"
)

func TestModuleTypeName(t *testing.T) {
	tests := []struct {
		name       string
		module     string
		objectType ModuleObjectType
		want       string
	}{
		{"root query", "", ModuleQuery, "Query"},
		{"root mutation", "", ModuleMutation, "Mutation"},
		{"root function", "", ModuleFunction, "Function"},
		{"root mutation function", "", ModuleMutationFunction, "MutationFunction"},
		{"root subscription", "", ModuleSubscription, "Subscription"},
		{"module query", "core", ModuleQuery, "_module_core_query"},
		{"module mutation", "core", ModuleMutation, "_module_core_mutation"},
		{"module function", "core", ModuleFunction, "_module_core_function"},
		{"module mutation function", "core", ModuleMutationFunction, "_module_core_mut_function"},
		// The subscription names must match the module assembler, which builds
		// them inline: "_module_" + strings.ReplaceAll(mod, ".", "_") + "_subscription"
		// (compiler/rules/assemble_modules.go).
		{"module subscription", "core", ModuleSubscription, "_module_core_subscription"},
		{"nested module query", "a.b", ModuleQuery, "_module_a_b_query"},
		{"nested module subscription", "a.b", ModuleSubscription, "_module_a_b_subscription"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ModuleTypeName(tt.module, tt.objectType); got != tt.want {
				t.Errorf("ModuleTypeName(%q, %v) = %q, want %q", tt.module, tt.objectType, got, tt.want)
			}
		})
	}
}
