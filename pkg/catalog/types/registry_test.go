package types

import (
	"testing"
)

func TestAllScalarsRegistered(t *testing.T) {
	expected := []string{
		"String", "Int", "Float", "Boolean", "BigInt",
		"Date", "Timestamp", "Time", "Interval",
		"JSON", "Geometry",
		"IntRange", "BigIntRange", "TimestampRange",
		"H3Cell", "Vector",
	}
	for _, name := range expected {
		if !IsScalar(name) {
			t.Errorf("scalar %q not registered", name)
		}
		s := Lookup(name)
		if s == nil {
			t.Errorf("Lookup(%q) returned nil", name)
			continue
		}
		if s.Name() != name {
			t.Errorf("Lookup(%q).Name() = %q", name, s.Name())
		}
	}
}

func TestScalarCount(t *testing.T) {
	count := 0
	for range Scalars() {
		count++
	}
	if count != 17 {
		t.Errorf("expected 17 scalars, got %d", count)
	}
}

func TestBuildSucceeds(t *testing.T) {
	if err := Build(); err != nil {
		t.Fatalf("Build() failed: %v", err)
	}
}

func TestSourcesProducesParsableSDL(t *testing.T) {
	srcs := Sources()
	if len(srcs) == 0 {
		t.Fatal("Sources() returned empty")
	}
	for _, src := range srcs {
		if src.Input == "" {
			t.Error("Sources() returned source with empty Input")
		}
	}
}

func TestIntCapabilities(t *testing.T) {
	s := Lookup("Int")
	if s == nil {
		t.Fatal("Int not found")
	}
	if _, ok := s.(Filterable); !ok {
		t.Error("Int does not implement Filterable")
	}
	if _, ok := s.(ListFilterable); !ok {
		t.Error("Int does not implement ListFilterable")
	}
	if _, ok := s.(Aggregatable); !ok {
		t.Error("Int does not implement Aggregatable")
	}
	if _, ok := s.(MeasurementAggregatable); !ok {
		t.Error("Int does not implement MeasurementAggregatable")
	}
}

func TestIntervalNoAggregation(t *testing.T) {
	s := Lookup("Interval")
	if s == nil {
		t.Fatal("Interval not found")
	}
	if _, ok := s.(Filterable); !ok {
		t.Error("Interval should implement Filterable")
	}
	if _, ok := s.(Aggregatable); ok {
		t.Error("Interval should NOT implement Aggregatable")
	}
	if _, ok := s.(MeasurementAggregatable); ok {
		t.Error("Interval should NOT implement MeasurementAggregatable")
	}
}

func TestGeometryExtraField(t *testing.T) {
	s := Lookup("Geometry")
	if s == nil {
		t.Fatal("Geometry not found")
	}
	efp, ok := s.(ExtraFieldProvider)
	if !ok {
		t.Fatal("Geometry does not implement ExtraFieldProvider")
	}
	if efp.ExtraFieldName() != "Measurement" {
		t.Errorf("ExtraFieldName() = %q, want Measurement", efp.ExtraFieldName())
	}
	ef := efp.GenerateExtraField("geom")
	if ef == nil {
		t.Fatal("GenerateExtraField returned nil")
	}
	if ef.Name != "_geom_measurement" {
		t.Errorf("field name = %q, want _geom_measurement", ef.Name)
	}
}

func TestVectorExtraField(t *testing.T) {
	s := Lookup("Vector")
	if s == nil {
		t.Fatal("Vector not found")
	}
	efp, ok := s.(ExtraFieldProvider)
	if !ok {
		t.Fatal("Vector does not implement ExtraFieldProvider")
	}
	if efp.ExtraFieldName() != "VectorDistance" {
		t.Errorf("ExtraFieldName() = %q, want VectorDistance", efp.ExtraFieldName())
	}
	ef := efp.GenerateExtraField("embedding")
	if ef == nil {
		t.Fatal("GenerateExtraField returned nil")
	}
	if ef.Name != "_embedding_distance" {
		t.Errorf("field name = %q, want _embedding_distance", ef.Name)
	}
}

func TestTimestampExtraField(t *testing.T) {
	s := Lookup("Timestamp")
	if s == nil {
		t.Fatal("Timestamp not found")
	}
	efp, ok := s.(ExtraFieldProvider)
	if !ok {
		t.Fatal("Timestamp does not implement ExtraFieldProvider")
	}
	if efp.ExtraFieldName() != "Extract" {
		t.Errorf("ExtraFieldName() = %q, want Extract", efp.ExtraFieldName())
	}
	ef := efp.GenerateExtraField("created_at")
	if ef == nil {
		t.Fatal("GenerateExtraField returned nil")
	}
	if ef.Name != "_created_at_part" {
		t.Errorf("field name = %q, want _created_at_part", ef.Name)
	}
}

func TestDateExtraField(t *testing.T) {
	s := Lookup("Date")
	if s == nil {
		t.Fatal("Date not found")
	}
	if _, ok := s.(ExtraFieldProvider); !ok {
		t.Error("Date should implement ExtraFieldProvider")
	}
}

func TestH3CellMinimal(t *testing.T) {
	s := Lookup("H3Cell")
	if s == nil {
		t.Fatal("H3Cell not found")
	}
	if _, ok := s.(Filterable); ok {
		t.Error("H3Cell should NOT implement Filterable")
	}
	if _, ok := s.(Aggregatable); ok {
		t.Error("H3Cell should NOT implement Aggregatable")
	}
}

func TestBooleanNoListFilter(t *testing.T) {
	s := Lookup("Boolean")
	if s == nil {
		t.Fatal("Boolean not found")
	}
	if _, ok := s.(ListFilterable); ok {
		t.Error("Boolean should NOT implement ListFilterable")
	}
	if _, ok := s.(Filterable); !ok {
		t.Error("Boolean should implement Filterable")
	}
}

func TestJSONNoListFilter(t *testing.T) {
	s := Lookup("JSON")
	if s == nil {
		t.Fatal("JSON not found")
	}
	if _, ok := s.(ListFilterable); ok {
		t.Error("JSON should NOT implement ListFilterable")
	}
	if _, ok := s.(Filterable); !ok {
		t.Error("JSON should implement Filterable")
	}
	if _, ok := s.(Aggregatable); !ok {
		t.Error("JSON should implement Aggregatable")
	}
}

// Edge case tests
func TestIsScalarNonexistent(t *testing.T) {
	if IsScalar("nonexistent") {
		t.Error("IsScalar(nonexistent) should return false")
	}
}

func TestLookupNonexistent(t *testing.T) {
	if Lookup("nonexistent") != nil {
		t.Error("Lookup(nonexistent) should return nil")
	}
}

func TestDuplicateRegisterOverwrites(t *testing.T) {
	// Save original
	orig := Lookup("String")
	// Register a new one with the same name
	custom := &stringScalar{}
	Register(custom)
	got := Lookup("String")
	if got != custom {
		t.Error("duplicate Register should overwrite (last-write-wins)")
	}
	// Restore
	Register(orig)
}

func TestFilterTypeNames(t *testing.T) {
	tests := []struct {
		scalar     string
		filterName string
	}{
		{"String", "StringFilter"},
		{"Int", "IntFilter"},
		{"Float", "FloatFilter"},
		{"Boolean", "BooleanFilter"},
		{"BigInt", "BigIntFilter"},
		{"Date", "DateFilter"},
		{"Timestamp", "TimestampFilter"},
		{"Time", "TimeFilter"},
		{"Interval", "IntervalFilter"},
		{"JSON", "JSONFilter"},
		{"Geometry", "GeometryFilter"},
		{"IntRange", "IntRangeFilter"},
		{"BigIntRange", "BigIntRangeFilter"},
		{"TimestampRange", "TimestampRangeFilter"},
		{"Vector", "VectorFilter"},
	}
	for _, tt := range tests {
		s := Lookup(tt.scalar)
		if s == nil {
			t.Errorf("scalar %q not found", tt.scalar)
			continue
		}
		f, ok := s.(Filterable)
		if !ok {
			continue // some scalars don't implement Filterable
		}
		if f.FilterTypeName() != tt.filterName {
			t.Errorf("%s.FilterTypeName() = %q, want %q", tt.scalar, f.FilterTypeName(), tt.filterName)
		}
	}
}

func TestAggregationTypeNames(t *testing.T) {
	tests := []struct {
		scalar  string
		aggName string
	}{
		{"String", "StringAggregation"},
		{"Int", "IntAggregation"},
		{"Float", "FloatAggregation"},
		{"Boolean", "BooleanAggregation"},
		{"BigInt", "BigIntAggregation"},
		{"Date", "DateAggregation"},
		{"Timestamp", "TimestampAggregation"},
		{"Time", "TimeAggregation"},
		{"JSON", "JSONAggregation"},
		{"Geometry", "GeometryAggregation"},
	}
	for _, tt := range tests {
		s := Lookup(tt.scalar)
		if s == nil {
			t.Errorf("scalar %q not found", tt.scalar)
			continue
		}
		a, ok := s.(Aggregatable)
		if !ok {
			t.Errorf("%s does not implement Aggregatable", tt.scalar)
			continue
		}
		if a.AggregationTypeName() != tt.aggName {
			t.Errorf("%s.AggregationTypeName() = %q, want %q", tt.scalar, a.AggregationTypeName(), tt.aggName)
		}
	}
}
