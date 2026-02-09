package compiler

import (
	"slices"

	"github.com/vektah/gqlparser/v2/ast"
)

type Options struct {
	Name         string
	ReadOnly     bool
	Prefix       string
	EngineType   string
	AsModule     bool
	Capabilities *EngineCapabilities

	catalog *ast.Directive
}

func (o *Options) IsSequenceDefaultSupported() bool {
	return o.Capabilities != nil && o.Capabilities.General.SupportDefaultSequences
}

func (o *Options) CheckFieldType(typ *ast.Type) error {
	if IsScalarType(typ.Name()) && !o.IsTypeSupported(typ.Name()) {
		return ErrorPosf(typ.Position, "engine %s doesn't support type %s", o.EngineType, typ.Name())
	}
	if typ.NamedType == "" && !o.IsArraysSupported() {
		return ErrorPosf(typ.Position, "field has unsupported array type")
	}
	if typ.NamedType != "" && !IsScalarType(typ.NamedType) &&
		!o.IsStructuredTypesSupported() {
		return ErrorPosf(typ.Position, "field has unsupported type %s", typ.NamedType)
	}
	return nil
}

func (o *Options) IsTypeSupported(typ string) bool {
	return o.Capabilities == nil || !slices.Contains(o.Capabilities.General.UnsupportedTypes, typ)
}

func (o *Options) IsStructuredTypesSupported() bool {
	return o.Capabilities == nil || !o.Capabilities.General.UnsupportStructuredTypes
}

func (o *Options) IsArraysSupported() bool {
	return o.Capabilities == nil || !o.Capabilities.General.UnsupportArrays
}

func (o *Options) IsTablesSupported() bool {
	return o.Capabilities == nil || !o.Capabilities.General.UnsupportTables
}

func (o *Options) SupportInsert() bool {
	return !o.ReadOnly || o.Capabilities != nil && o.Capabilities.Insert.Insert
}

func (o *Options) SupportUpdate() bool {
	return !o.ReadOnly || o.Capabilities != nil && o.Capabilities.Update.Update
}

func (o *Options) SupportDelete() bool {
	return !o.ReadOnly || o.Capabilities != nil && o.Capabilities.Delete.Delete
}

func (o *Options) SupportDeleteWithoutPKs() bool {
	return !o.ReadOnly || o.Capabilities != nil && o.Capabilities.Delete.DeleteWithoutPKs
}

func (o *Options) SupportUpdateWithoutPKs() bool {
	return !o.ReadOnly || o.Capabilities != nil && o.Capabilities.Update.UpdateWithoutPKs
}

func (o *Options) SupportInsertReturning() bool {
	return !o.ReadOnly || o.Capabilities != nil && o.Capabilities.Insert.Returning
}

func (o *Options) SupportInsertReferences() bool {
	return !o.ReadOnly || o.Capabilities != nil && o.Capabilities.Insert.InsertReferences
}

func (o *Options) SupportUpdatePKs() bool {
	return !o.ReadOnly || o.Capabilities != nil && o.Capabilities.Update.UpdatePKColumns
}

type EngineCapabilities struct {
	General EngineGeneralCapabilities
	Insert  EngineInsertCapabilities
	Update  EngineUpdateCapabilities
	Delete  EngineDeleteCapabilities
}

type EngineInsertCapabilities struct {
	Insert           bool
	Returning        bool
	InsertReferences bool
}

type EngineUpdateCapabilities struct {
	Update           bool
	UpdatePKColumns  bool
	UpdateWithoutPKs bool
}

type EngineDeleteCapabilities struct {
	Delete           bool
	DeleteWithoutPKs bool
}

type EngineGeneralCapabilities struct {
	SupportDefaultSequences  bool
	UnsupportedTypes         []string
	UnsupportStructuredTypes bool
	UnsupportArrays          bool

	UnsupportTables bool
}
