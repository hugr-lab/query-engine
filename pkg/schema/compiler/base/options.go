package base

import "slices"

// Options configures compilation behavior.
type Options struct {
	Name         string
	ReadOnly     bool
	Prefix       string
	EngineType   string
	AsModule     bool
	IsExtension  bool
	Capabilities *EngineCapabilities
}

// ApplyPrefix prepends the prefix to a name with underscore separator if set.
func (o *Options) ApplyPrefix(name string) string {
	if o.Prefix == "" {
		return name
	}
	return o.Prefix + "_" + name
}

func (o *Options) IsSequenceDefaultSupported() bool {
	return o.Capabilities != nil && o.Capabilities.General.SupportDefaultSequences
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

func (o *Options) IsCrossCatalogReferencesSupported() bool {
	return o.Capabilities != nil && o.Capabilities.General.SupportCrossCatalogReferences
}

// EngineCapabilities declares what an engine supports.
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
	SupportDefaultSequences       bool
	UnsupportedTypes              []string
	UnsupportStructuredTypes      bool
	UnsupportArrays               bool
	UnsupportTables               bool
	SupportCrossCatalogReferences bool
}
