package types

import (
	"reflect"
	"testing"
)

func TestParseVector(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    Vector
		wantErr bool
	}{
		{
			name:    "float64 slice",
			input:   []float64{1.1, 2.2, 3.3},
			want:    Vector{1.1, 2.2, 3.3},
			wantErr: false,
		},
		{
			name:    "any slice with float64",
			input:   []any{1.0, 2.0, 3.0},
			want:    Vector{1.0, 2.0, 3.0},
			wantErr: false,
		},
		{
			name:    "any slice with non-float64",
			input:   []any{1.0, "bad", 3.0},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "string with numbers",
			input:   "1.5,2.5,3.5",
			want:    Vector{1.5, 2.5, 3.5},
			wantErr: false,
		},
		{
			name:    "string with spaces and brackets",
			input:   "[1, 2, 3]",
			want:    Vector{1, 2, 3},
			wantErr: false,
		},
		{
			name:    "string with empty elements",
			input:   "1,,3",
			want:    Vector{1, 0, 3},
			wantErr: false,
		},
		{
			name:    "string with invalid character",
			input:   "1,a,3",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "unsupported type",
			input:   123,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty float64 slice",
			input:   []float64{},
			want:    Vector{},
			wantErr: false,
		},
		{
			name:    "empty any slice",
			input:   []any{},
			want:    Vector{},
			wantErr: false,
		},
		{
			name:    "empty string",
			input:   "",
			want:    Vector{},
			wantErr: false,
		},
		{
			name:    "string with only brackets",
			input:   "[]",
			want:    Vector{},
			wantErr: false,
		},
		{
			name:    "string with trailing comma",
			input:   "1,2,3,",
			want:    Vector{1, 2, 3, 0},
			wantErr: false,
		},
		{
			name:    "string with leading comma",
			input:   ",1,2,3",
			want:    Vector{0, 1, 2, 3},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseVector(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseVector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseVector() = %v, want %v", got, tt.want)
			}
		})
	}
}
