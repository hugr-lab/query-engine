package hugr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	ingestContentType    = "application/vnd.apache.arrow.stream"
	ingestDataObjectArg  = "data_object"
	ingestViewNamePrefix = "_hugr_ingest_"
)

// IngestResponse is the success payload returned by /ipc/ingest.
type IngestResponse struct {
	DataObject string   `json:"data_object"`
	Inserted   int64    `json:"inserted"`
	Columns    []string `json:"columns"`
}

type ingestErrorBody struct {
	Error string `json:"error"`
}

// ipcIngestHandler accepts an Apache Arrow IPC stream in the request body and
// inserts the records into the target data object referenced by the
// `data_object` query parameter.
//
// Wire protocol (first iteration):
//   - Method: POST
//   - URL:    /ipc/ingest?data_object=<dotted-path-or-type-name>
//   - Headers: Content-Type: application/vnd.apache.arrow.stream
//   - Body:    Arrow IPC stream (schema + record batches)
//   - Response: 200 OK, JSON {"data_object": ..., "inserted": N, "columns": [...]}
//
// Restrictions intentionally enforced on this iteration (per design):
//   - INSERT only (no on-conflict / merge / upsert / returning)
//   - target must be a table data object (views are rejected)
//   - reference fields are skipped (not insertable through this path)
//   - permissions are checked against the synthetic insert mutation input
func (s *Service) ipcIngestHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		writeIngestError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	dataObject := r.URL.Query().Get(ingestDataObjectArg)
	if dataObject == "" {
		writeIngestError(w, http.StatusBadRequest, "missing data_object query parameter")
		return
	}

	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, ingestContentType) {
		writeIngestError(w, http.StatusUnsupportedMediaType,
			fmt.Sprintf("Content-Type must be %s, got %q", ingestContentType, ct))
		return
	}

	ctx := r.Context()
	// Auth middleware already populated permissions; make sure we have them
	// (handles direct-handler callers that bypass the middleware in tests).
	if perm.PermissionsFromCtx(ctx) == nil {
		newCtx, err := s.perm.ContextWithPermissions(ctx)
		if err != nil {
			if errors.Is(err, auth.ErrForbidden) {
				writeIngestError(w, http.StatusForbidden, err.Error())
				return
			}
			writeIngestError(w, http.StatusInternalServerError, err.Error())
			return
		}
		ctx = newCtx
	}

	info, mutationField, err := s.resolveIngestTarget(ctx, dataObject)
	if err != nil {
		writeIngestError(w, http.StatusBadRequest, err.Error())
		return
	}

	eng, err := s.ds.Engine(info.Catalog)
	if err != nil {
		writeIngestError(w, http.StatusBadRequest,
			fmt.Sprintf("engine for catalog %q not available: %v", info.Catalog, err))
		return
	}

	reader, err := ipc.NewReader(r.Body, ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		writeIngestError(w, http.StatusBadRequest, "invalid arrow stream: "+err.Error())
		return
	}
	defer reader.Release()

	columns, err := resolveIngestColumns(reader.Schema(), info)
	if err != nil {
		writeIngestError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(columns) == 0 {
		writeIngestError(w, http.StatusBadRequest,
			"no insertable columns matched between arrow stream and data object")
		return
	}

	if err := checkIngestPermission(ctx, info, mutationField, columns); err != nil {
		if errors.Is(err, auth.ErrForbidden) {
			writeIngestError(w, http.StatusForbidden, err.Error())
			return
		}
		writeIngestError(w, http.StatusInternalServerError, err.Error())
		return
	}

	inserted, err := s.executeIngest(ctx, info, eng, reader, columns)
	if err != nil {
		writeIngestError(w, http.StatusInternalServerError, err.Error())
		return
	}

	out := IngestResponse{
		DataObject: dataObject,
		Inserted:   inserted,
		Columns:    columnNames(columns),
	}
	_ = json.NewEncoder(w).Encode(out)
}

// ingestColumn binds an Arrow input column to an SDL field of the target table.
type ingestColumn struct {
	ArrowName string     // column name as it appears in the incoming Arrow schema
	Field     *sdl.Field // resolved SDL field of the target data object
}

func columnNames(cs []ingestColumn) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.ArrowName
	}
	return out
}

// resolveIngestTarget walks the GraphQL schema to find the target data object
// and the corresponding insert mutation field. The dataObject argument can be
// either a dotted Query path (e.g. "pg_store.public.events") or a bare GraphQL
// type name (e.g. "pg_store_public_events").
func (s *Service) resolveIngestTarget(ctx context.Context, dataObject string) (*sdl.Object, *ast.FieldDefinition, error) {
	provider := s.schema.Provider()

	var def *ast.Definition
	if strings.Contains(dataObject, ".") {
		queryDef := provider.ForName(ctx, base.QueryBaseName)
		if queryDef == nil {
			return nil, nil, fmt.Errorf("query base type not found in schema")
		}
		cur := queryDef
		for _, part := range strings.Split(dataObject, ".") {
			f := cur.Fields.ForName(part)
			if f == nil {
				return nil, nil, fmt.Errorf("data object %q: segment %q not found", dataObject, part)
			}
			cur = provider.ForName(ctx, f.Type.Name())
			if cur == nil {
				return nil, nil, fmt.Errorf("data object %q: type %q not found", dataObject, f.Type.Name())
			}
		}
		def = cur
	} else {
		def = provider.ForName(ctx, dataObject)
	}
	if def == nil {
		return nil, nil, fmt.Errorf("data object %q not found in schema", dataObject)
	}
	if !sdl.IsDataObject(def) {
		return nil, nil, fmt.Errorf("%q is not a data object", dataObject)
	}
	info := sdl.DataObjectInfo(def)
	if info == nil {
		return nil, nil, fmt.Errorf("data object %q: no info", dataObject)
	}
	if info.Type != sdl.TableDataObject {
		return nil, nil, fmt.Errorf("data object %q is not a table (got %q) — only tables are ingestable", dataObject, info.Type)
	}
	if info.Catalog == "" {
		return nil, nil, fmt.Errorf("data object %q has no catalog", dataObject)
	}

	// Find the insert mutation field; we need it for permission checks.
	_, mutationField := sdl.ObjectMutationDefinition(ctx, provider, def, sdl.MutationTypeInsert)
	if mutationField == nil {
		return nil, nil, fmt.Errorf("data object %q has no insert mutation defined", dataObject)
	}
	return info, mutationField, nil
}

// resolveIngestColumns maps Arrow schema fields onto SDL fields of the table.
// Reference / virtual / computed fields are intentionally rejected because
// they are not directly insertable.
func resolveIngestColumns(schema *arrow.Schema, info *sdl.Object) ([]ingestColumn, error) {
	if schema == nil {
		return nil, fmt.Errorf("arrow stream has no schema")
	}
	cols := make([]ingestColumn, 0, schema.NumFields())
	seen := map[string]struct{}{}
	for _, f := range schema.Fields() {
		if _, dup := seen[f.Name]; dup {
			return nil, fmt.Errorf("duplicate arrow column %q", f.Name)
		}
		seen[f.Name] = struct{}{}

		fi := info.FieldForName(f.Name)
		if fi == nil {
			return nil, fmt.Errorf("column %q is not defined in data object %q",
				f.Name, info.Definition().Name)
		}
		if fi.IsReferencesSubquery() {
			return nil, fmt.Errorf("column %q is a reference and cannot be ingested directly",
				f.Name)
		}
		if fi.IsNotDBField() {
			return nil, fmt.Errorf("column %q is a computed/virtual field and cannot be ingested",
				f.Name)
		}
		if fi.FieldSourceName("", false) == "-" {
			return nil, fmt.Errorf("column %q has no database mapping", f.Name)
		}
		cols = append(cols, ingestColumn{ArrowName: f.Name, Field: fi})
	}
	return cols, nil
}

// checkIngestPermission verifies that the caller may invoke the insert mutation
// and write each of the supplied columns. It mirrors RolePermissions.CheckQuery
// + CheckMutationInput but operates on the synthetic per-column payload that
// an Arrow batch represents.
func checkIngestPermission(ctx context.Context, info *sdl.Object, mutationField *ast.FieldDefinition, cols []ingestColumn) error {
	if auth.IsFullAccess(ctx) {
		return nil
	}
	rp := perm.PermissionsFromCtx(ctx)
	if rp == nil {
		// No permissions configured = allow (matches behaviour of
		// CheckQuery callers in the rest of the engine).
		return nil
	}
	if rp.Disabled {
		return auth.ErrForbidden
	}

	// 1) mutation field itself must be enabled on the parent module type.
	parent := ""
	if mutationField != nil {
		if pd, ok := mutationFieldParent(info); ok {
			parent = pd
		}
		if _, ok := rp.Enabled(parent, mutationField.Name); !ok {
			return auth.ErrForbidden
		}
	}

	// 2) each ingested column must be enabled on the insert input type.
	inputName := info.InputInsertDataName()
	if inputName == "" {
		return nil
	}
	for _, c := range cols {
		if _, ok := rp.Enabled(inputName, c.ArrowName); !ok {
			return auth.ErrForbidden
		}
	}
	return nil
}

// mutationFieldParent returns the name of the GraphQL type that owns the
// insert mutation field for this data object. That type is the field-level
// permission scope (Permission.Object) used by RolePermissions.Enabled.
func mutationFieldParent(info *sdl.Object) (string, bool) {
	mod := sdl.ObjectModule(info.Definition())
	return sdl.ModuleTypeName(mod, sdl.ModuleMutation), true
}

// executeIngest registers the Arrow record reader as a DuckDB view and runs
// `INSERT INTO <target>(<cols>) SELECT <cols> FROM <view>`. The view is bound
// to the underlying DuckDB driver connection and released after the INSERT
// completes (success or failure).
func (s *Service) executeIngest(ctx context.Context, info *sdl.Object, eng engines.Engine, reader *ipc.Reader, cols []ingestColumn) (int64, error) {
	ar, err := s.db.Arrow(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire duckdb arrow conn: %w", err)
	}
	defer ar.Close()

	viewName := ingestViewNamePrefix + strings.ReplaceAll(uuid.NewString(), "-", "")
	release, err := ar.RegisterView(reader, viewName)
	if err != nil {
		return 0, fmt.Errorf("register arrow view: %w", err)
	}
	defer release()

	sqlStr := buildIngestSQL(ctx, info, eng, cols, viewName)
	res, err := ar.Exec(ctx, sqlStr)
	if err != nil {
		return 0, fmt.Errorf("ingest insert failed: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// buildIngestSQL constructs the INSERT ... SELECT statement that drains the
// registered Arrow view into the target table. The target is fully qualified
// with the catalog (data-source) identifier so that DuckDB's postgres
// extension can route the INSERT through the attached database.
func buildIngestSQL(ctx context.Context, info *sdl.Object, eng engines.Engine, cols []ingestColumn, viewName string) string {
	target := info.SQL(ctx, engines.Ident(info.Catalog))

	colNames := make([]string, len(cols))
	selectExprs := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Field.FieldSourceName("", true)
		selectExprs[i] = engines.Ident(c.ArrowName)
	}

	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
		target,
		strings.Join(colNames, ", "),
		strings.Join(selectExprs, ", "),
		engines.Ident(viewName),
	)
}

func writeIngestError(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ingestErrorBody{Error: msg})
}
