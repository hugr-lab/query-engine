package base

import "testing"

func TestEngineIngestCapabilities(t *testing.T) {
	tests := []struct {
		name      string
		caps      EngineIngestCapabilities
		available bool
		valid     bool
	}{
		{name: "disabled", caps: EngineIngestCapabilities{}, available: false, valid: true},
		{name: "insert", caps: EngineIngestCapabilities{Insert: true}, available: true, valid: true},
		{name: "insert and merge", caps: EngineIngestCapabilities{Insert: true, Merge: true}, available: true, valid: true},
		{name: "merge without insert", caps: EngineIngestCapabilities{Merge: true}, available: true, valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.caps.Available(); got != tt.available {
				t.Fatalf("Available() = %t, want %t", got, tt.available)
			}
			if got := tt.caps.Valid(); got != tt.valid {
				t.Fatalf("Valid() = %t, want %t", got, tt.valid)
			}
		})
	}
}
