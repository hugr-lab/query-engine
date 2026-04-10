package app

import (
	"strings"
	"testing"
)

func TestStructType_FieldFromSource_OnOutput_OK(t *testing.T) {
	s := Struct("user").Field("id", String).FieldFromSource("name", String, "user_name")
	if len(s.fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(s.fields))
	}
	if s.fields[1].fieldSource != "user_name" {
		t.Errorf("expected fieldSource = user_name, got %q", s.fields[1].fieldSource)
	}
}

func TestStructType_FieldFromSource_OnInput_Panics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic, got nil")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T", r)
		}
		if !strings.Contains(msg, "only valid on output structs") {
			t.Errorf("expected panic message about output structs, got: %s", msg)
		}
	}()
	InputStruct("bad").Field("a", String).FieldFromSource("b", String, "original")
}

func TestStructType_AsType_OutputStruct(t *testing.T) {
	s := Struct("my_result").Field("a", String)
	typ := s.AsType()
	if typ.graphql != "my_result" {
		t.Errorf("expected graphql = my_result, got %q", typ.graphql)
	}
	if typ.structDef == nil {
		t.Error("expected structDef to be non-nil")
	}
	if typ.structDef.kind != StructKindOutput {
		t.Errorf("expected StructKindOutput, got %v", typ.structDef.kind)
	}
}

func TestStructType_AsType_InputStruct(t *testing.T) {
	s := InputStruct("my_input").Field("a", String)
	typ := s.AsType()
	if typ.graphql != "my_input" {
		t.Errorf("expected graphql = my_input, got %q", typ.graphql)
	}
	if typ.structDef == nil {
		t.Error("expected structDef to be non-nil")
	}
	if typ.structDef.kind != StructKindInput {
		t.Errorf("expected StructKindInput, got %v", typ.structDef.kind)
	}
}

func TestEqualStructFields_Identical(t *testing.T) {
	a := Struct("user").Field("id", String).Field("name", String)
	b := Struct("user").Field("id", String).Field("name", String)
	if !equalStructFields(a, b) {
		t.Error("expected identical structs to compare equal")
	}
}

func TestEqualStructFields_DifferentFields(t *testing.T) {
	a := Struct("user").Field("id", String)
	b := Struct("user").Field("id", Int64)
	if equalStructFields(a, b) {
		t.Error("expected different field types to compare unequal")
	}
}

func TestEqualStructFields_DifferentLength(t *testing.T) {
	a := Struct("user").Field("id", String)
	b := Struct("user").Field("id", String).Field("name", String)
	if equalStructFields(a, b) {
		t.Error("expected different field counts to compare unequal")
	}
}

func TestEqualStructFields_DifferentKind(t *testing.T) {
	a := Struct("user").Field("id", String)
	b := InputStruct("user").Field("id", String)
	if equalStructFields(a, b) {
		t.Error("expected different kinds to compare unequal")
	}
}

func TestStructType_FieldList(t *testing.T) {
	s := Struct("post").FieldList("tags", String)
	if len(s.fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(s.fields))
	}
	if !s.fields[0].isList {
		t.Error("expected isList = true")
	}
}

func TestStructType_FieldNullable(t *testing.T) {
	s := Struct("user").FieldNullable("middle_name", String)
	if !s.fields[0].nullable {
		t.Error("expected nullable = true")
	}
}
