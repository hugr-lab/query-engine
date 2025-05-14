package hugr

import (
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/planner"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// ipcHandler handles the IPC (Inter-Process Communication) requests.
// It is used to execute queries and return results in a streaming format.
// Accepts a JSON payload with the query and variables (like normal GraphQL).
func (s *Service) ipcHandler(w http.ResponseWriter, r *http.Request) {
	req, err := s.parseRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	mw := multipart.NewWriter(w)
	err = mw.SetBoundary("HUGR")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "multipart/mixed; boundary="+mw.Boundary())

	defer mw.Close()
	// write data
	err = s.queryIPC(r.Context(), mw, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Service) queryIPC(ctx context.Context, mw *multipart.Writer, req Request) error {
	// add permissions to context
	ctx, err := s.perm.ContextWithPermissions(ctx)
	if err != nil {
		return err
	}
	schema := s.catalog.Schema()
	query, errs := gqlparser.LoadQuery(schema, req.Query)
	if len(errs) != 0 {
		// write errors to IPC
		return writeErrorsToIPC(mw, memory.DefaultAllocator, "", errs)
	}
	if len(query.Operations) == 0 {
		return gqlerror.Errorf("no operations found")
	}

	ctx = planner.ContextWithRawResultsFlag(ctx)
	aloc := memory.NewGoAllocator()

	for _, op := range query.Operations {
		qq, _ := compiler.QueryRequestInfo(op.SelectionSet)
		qm := flatQuery(qq)
		if len(qm) == 0 {
			return nil
		}
		data, ext, err := s.processOperation(ctx, schema, op, req.Variables)
		if err != nil {
			return writeErrorsToIPC(mw, aloc, op.Name, types.WarpGraphQLError(err))
		}
		for path, q := range qm {
			qd := extractData(path, data)
			if len(query.Operations) > 1 {
				path = op.Name + "." + path
			}
			err = writeDataIPC(mw, aloc, "data."+path, q, qd)
			if err != nil {
				types.DataClose(data)
				return err
			}
		}
		types.DataClose(data)
		// write extensions to IPC
		if len(ext) != 0 {
			path := "extensions"
			if len(query.Operations) > 1 {
				path = op.Name + "." + path
			}
			return writeExtensionsToIPC(mw, aloc, path, ext)
		}
	}

	return nil
}

func extractData(path string, data map[string]any) any {
	if path == "" {
		return data
	}
	pp := strings.SplitN(path, ".", 2)
	if len(pp) == 1 {
		return data[pp[0]]
	}
	d, ok := data[pp[0]]
	if !ok || d == nil {
		return nil
	}
	dm, ok := d.(map[string]any)
	if !ok {
		return nil
	}
	return extractData(pp[1], dm)
}

func flatQuery(queries []compiler.QueryRequest) map[string]compiler.QueryRequest {
	// flatten query
	if len(queries) == 0 {
		return nil
	}
	flat := make(map[string]compiler.QueryRequest, len(queries))
	for _, q := range queries {
		switch q.QueryType {
		case compiler.QueryTypeNone:
			// seek children
			for k, v := range flatQuery(q.Subset) {
				flat[q.Field.Alias+"."+k] = v
			}
		case compiler.QueryTypeQuery,
			compiler.QueryTypeFunction,
			compiler.QueryTypeFunctionMutation,
			compiler.QueryTypeMutation,
			compiler.QueryTypeMeta:
			flat[q.Field.Alias] = q
		}
	}
	return flat
}

func writeDataIPC(w *multipart.Writer, aloc memory.Allocator, path string, query compiler.QueryRequest, data any) error {
	switch data := data.(type) {
	case *db.ArrowTable:
		return writeArrowTableToIPC(w, aloc, path, query, data)
	default:
		return writeJsonValueToIPC(w, aloc, path, query, data)
	}
}

func writeArrowTableToIPC(w *multipart.Writer, aloc memory.Allocator, path string, query compiler.QueryRequest, data *db.ArrowTable) error {
	hdr := textproto.MIMEHeader{}
	hdr.Set("Content-Type", "application/vnd.apache.arrow.stream")
	hdr.Set("X-Hugr-Part-Type", "data")
	hdr.Set("X-Hugr-Path", path)
	hdr.Set("X-Hugr-Format", "table")
	if data == nil || data.NumChunks() == 0 {
		hdr.Set("X-Hugr-Empty", "true")
		hdr.Set("X-Hugr-Chunk", "0")
		hdr.Set("X-Hugr-Table-Info", "{}")
	}
	var meta map[string]string
	if data != nil && data.NumChunks() > 0 {
		hdr.Set("X-Hugr-Chunk", strconv.Itoa(data.NumChunks()))
		hdr.Set("X-Hugr-Table-Info", data.Info())
		meta = map[string]string{
			"chunks": strconv.Itoa(data.NumChunks()),
		}
		if gi, ok := geometryInfo(query.Field); ok {
			hdr.Set("X-Hugr-Geometry-Fields", gi)
			hdr.Set("X-Hugr-Geometry", "true")
		}
	}
	pw, err := w.CreatePart(hdr)
	if err != nil {
		return err
	}

	if data == nil || data.NumChunks() == 0 {
		return nil
	}

	iw := ipc.NewWriter(pw)
	defer iw.Close()
	// add geom fields to the metadata
	defer data.Release()
	for cn := range data.NumChunks() {
		chunk := data.Chunk(cn)
		if chunk == nil {
			continue
		}
		meta["chunk"] = strconv.Itoa(cn)
		ns := addMeta(chunk.Schema(), meta)
		err := iw.Write(array.NewRecord(ns, chunk.Columns(), chunk.NumRows()))
		if err != nil {
			return err
		}
	}
	return nil
}

func geometryInfo(query *ast.Field) (string, bool) {
	gi := geomFieldsInfoFromQuery(query)
	if len(gi) == 0 {
		return "", false
	}
	gff := make([]string, 0, len(gi))
	for k, v := range gi {
		if k == "" {
			k = query.Alias
		}
		if query.Definition.Type.NamedType != "" {
			v.Format = "GeoJSON"
		}
		gff = append(gff, fmt.Sprintf(`"%s": {"srid": "%s", "format": "%s"}`, k, v.SRID, v.Format))
	}
	return fmt.Sprintf("{%s}", strings.Join(gff, ",")), true
}

type geomInfo struct {
	SRID   string `json:"srid"`
	Format string `json:"format"`
}

func geomFieldsInfoFromQuery(query *ast.Field) map[string]geomInfo {
	if compiler.IsScalarType(query.Definition.Type.Name()) {
		if query.Definition.Type.NamedType != compiler.GeometryTypeName {
			return nil
		}
		fi := compiler.FieldInfo(query)
		if fi == nil {
			return nil
		}
		return map[string]geomInfo{
			"": geomInfo{
				Format: "WKB",
				SRID:   fi.GeometrySRID(),
			},
		}
	}
	meta := map[string]geomInfo{}
	for _, s := range engines.SelectedFields(query.SelectionSet) {
		info := geomFieldsInfoFromQuery(s.Field)
		if len(info) == 0 {
			continue
		}
		for field, info := range geomFieldsInfoFromQuery(s.Field) {
			if field != "" {
				field = s.Field.Alias + "." + field
				info.Format = "GeoJSONString"
			}
			if field == "" {
				field = s.Field.Alias
			}
			meta[field] = info
		}
	}
	return meta
}

func writeJsonValueToIPC(w *multipart.Writer, aloc memory.Allocator, path string, query compiler.QueryRequest, val any) error {
	hdr := textproto.MIMEHeader{}
	hdr.Set("Content-Type", "application/json")
	hdr.Set("X-Hugr-Part-Type", "data")
	hdr.Set("X-Hugr-Path", path)
	hdr.Set("X-Hugr-Format", "object")
	if gi, ok := geometryInfo(query.Field); ok {
		hdr.Set("X-Hugr-Geometry-Fields", gi)
		hdr.Set("X-Hugr-Geometry", "true")
	}
	pw, err := w.CreatePart(hdr)
	if err != nil {
		return err
	}
	return json.NewEncoder(pw).Encode(val)
}

func writeExtensionsToIPC(w *multipart.Writer, aloc memory.Allocator, path string, ext any) error {
	hdr := textproto.MIMEHeader{}
	hdr.Set("Content-Type", "application/json")
	hdr.Set("X-Hugr-Part-Type", "extensions")
	if path != "" {
		hdr.Set("X-Hugr-Path", path)
	}
	hdr.Set("X-Hugr-Format", "object")
	pw, err := w.CreatePart(hdr)
	if err != nil {
		return err
	}
	return json.NewEncoder(pw).Encode(ext)
}

func writeErrorsToIPC(w *multipart.Writer, aloc memory.Allocator, path string, errs gqlerror.List) error {
	hdr := textproto.MIMEHeader{}
	hdr.Set("Content-Type", "application/json")
	hdr.Set("X-Hugr-Part-Type", "errors")
	if path != "" {
		hdr.Set("X-Hugr-Path", path)
	}
	hdr.Set("X-Hugr-Format", "object")
	pw, err := w.CreatePart(hdr)
	if err != nil {
		return err
	}
	return json.NewEncoder(pw).Encode(errs)
}

func addMeta(schema *arrow.Schema, meta map[string]string) *arrow.Schema {
	keys := append([]string{}, schema.Metadata().Keys()...)
	vals := append([]string{}, schema.Metadata().Values()...)
	for k, v := range meta {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	m := arrow.NewMetadata(keys, vals)
	return arrow.NewSchema(schema.Fields(), &m)
}
