package arrowingest

import (
	"strings"
	"testing"
)

func TestNewSourceUsesUniqueViewName(t *testing.T) {
	first := NewSource(nil)
	second := NewSource(nil)

	if first.View() == second.View() {
		t.Fatalf("sources share view name %q", first.View())
	}
	for _, name := range []string{first.View(), second.View()} {
		if !strings.HasPrefix(name, viewNamePrefix) {
			t.Fatalf("view name %q does not start with %q", name, viewNamePrefix)
		}
	}
}
