package db

import (
	"strings"
	"testing"
)

func TestNewArrowIngestSourceUsesUniqueViewName(t *testing.T) {
	first := NewArrowIngestSource(nil)
	second := NewArrowIngestSource(nil)

	if first.View() == second.View() {
		t.Fatalf("sources share view name %q", first.View())
	}
	for _, name := range []string{first.View(), second.View()} {
		if !strings.HasPrefix(name, arrowIngestViewNamePrefix) {
			t.Fatalf("view name %q does not start with %q", name, arrowIngestViewNamePrefix)
		}
	}
}
