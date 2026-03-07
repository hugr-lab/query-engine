package base

import "github.com/vektah/gqlparser/v2/ast"

// RecoverObjectInfo reconstructs ObjectInfo from an already-compiled provider
// definition by reading its directives. Used during incremental compilation
// to recover metadata about existing types in the provider schema.
func RecoverObjectInfo(def *ast.Definition) *ObjectInfo {
	info := &ObjectInfo{
		Name: def.Name,
	}

	// @original_name(name: "...")
	if orig := DirectiveArgString(def.Directives.ForName(OriginalNameDirectiveName), ArgName); orig != "" {
		info.OriginalName = orig
	} else {
		info.OriginalName = def.Name
	}

	// @table(name: "...", is_m2m: ...)
	if tableDir := def.Directives.ForName(ObjectTableDirectiveName); tableDir != nil {
		info.TableName = DirectiveArgString(tableDir, ArgName)
		if arg := tableDir.Arguments.ForName(ArgIsM2M); arg != nil && arg.Value != nil {
			info.IsM2M = arg.Value.Raw == "true"
		}
	}

	// @view(name: "...")
	if viewDir := def.Directives.ForName(ObjectViewDirectiveName); viewDir != nil {
		info.IsView = true
		info.TableName = DirectiveArgString(viewDir, ArgName)
	}

	// @module(name: "...")
	info.Module = DirectiveArgString(def.Directives.ForName(ModuleDirectiveName), ArgName)

	// @replace
	info.IsReplace = IsReplaceDefinition(def)

	// @cube
	info.IsCube = def.Directives.ForName(ObjectCubeDirectiveName) != nil

	// @hypertable
	info.IsHypertable = def.Directives.ForName(ObjectHyperTableDirectiveName) != nil

	// @pk on fields
	for _, f := range def.Fields {
		if f.Directives.ForName(FieldPrimaryKeyDirectiveName) != nil {
			info.PrimaryKey = append(info.PrimaryKey, f.Name)
		}
	}

	// @args(name: "...", required: ...)
	if argsDir := def.Directives.ForName(ViewArgsDirectiveName); argsDir != nil {
		info.InputArgsName = DirectiveArgString(argsDir, ArgName)
		if reqArg := argsDir.Arguments.ForName(ArgRequired); reqArg != nil && reqArg.Value != nil {
			info.RequiredArgs = reqArg.Value.Raw == "true"
		}
	}

	return info
}
