package perm

import (
	"context"
)

// Op is a data-object operation for table-level permission lookups.
type Op string

const (
	OpQuery  Op = "query"
	OpInsert Op = "insert"
	OpUpdate Op = "update"
	OpDelete Op = "delete"
)

// DataObjectTypePrefix marks permission rows that carry table-level rules.
// Such rows use a synthetic type_name ("data-object:query", "data-object:insert",
// "data-object:update", "data-object:delete") and put the data-object's GraphQL
// type name (or "*" for all data objects) in field_name. The ":" makes collision
// with real GraphQL type names impossible, so field-level rows are unaffected.
const DataObjectTypePrefix = "data-object:"

// dataObjectMatch returns the data-object permission row for the operation and
// object type name. An exact field_name match wins over a "*" row.
func (r *RolePermissions) dataObjectMatch(op Op, objType string) *Permission {
	if objType == "" {
		return nil
	}
	typeName := DataObjectTypePrefix + string(op)
	var wildcard *Permission
	for i := range r.Permissions {
		p := &r.Permissions[i]
		if p.Object != typeName {
			continue
		}
		switch p.Field {
		case objType:
			return p
		case "*":
			if wildcard == nil {
				wildcard = p
			}
		}
	}
	return wildcard
}

// DataObjectFilter returns the table-level filter for the data object, with
// [$auth.*] placeholders substituted. The filter is a property of the TABLE:
// the planner applies it wherever the object is materialised — top-level
// queries, _by_pk, relations, _join, aggregations, and mutation WHERE clauses.
// For OpDelete/OpUpdate the op-specific row's filter takes precedence; when it
// is absent the OpQuery filter applies (you cannot delete/update rows you
// cannot read, unless an op row explicitly widens or narrows the scope).
func (r *RolePermissions) DataObjectFilter(ctx context.Context, objType string, op Op) map[string]any {
	if r.Disabled {
		return nil
	}
	p := r.dataObjectMatch(op, objType)
	if (p == nil || len(p.Filter) == 0) && op != OpQuery {
		p = r.dataObjectMatch(OpQuery, objType)
	}
	if p == nil {
		return nil
	}
	return applyContextVariable(ctx, p.Filter, nil)
}

// DataObjectData returns the table-level data values for insert/update
// mutations, with [$auth.*] placeholders substituted. These values are applied
// over the client's data (force-stamp): a data-object:insert row with
// {owner_id: "[$auth.user_id]"} makes it impossible to insert rows for another
// principal regardless of the submitted data.
func (r *RolePermissions) DataObjectData(ctx context.Context, objType string, op Op) map[string]any {
	if r.Disabled {
		return nil
	}
	p := r.dataObjectMatch(op, objType)
	if p == nil {
		return nil
	}
	return applyContextVariable(ctx, p.Data, nil)
}

// DataObjectDisabled reports whether the operation on the data object is
// denied. A disabled data-object:query row denies the table on EVERY path
// (reads and mutations alike); op-specific rows can additionally deny a single
// operation.
func (r *RolePermissions) DataObjectDisabled(objType string, op Op) bool {
	if r.Disabled {
		return true
	}
	if p := r.dataObjectMatch(op, objType); p != nil && p.Disabled {
		return true
	}
	if op == OpQuery {
		return false
	}
	if p := r.dataObjectMatch(OpQuery, objType); p != nil && p.Disabled {
		return true
	}
	return false
}

// DataObjectHidden reports whether the data object is hidden from
// introspection (set on the data-object:query row). Fields returning a hidden
// data object are omitted from the introspected schema but remain queryable
// unless also disabled.
func (r *RolePermissions) DataObjectHidden(objType string) bool {
	if r.Disabled {
		return true
	}
	p := r.dataObjectMatch(OpQuery, objType)
	return p != nil && p.Hidden
}
