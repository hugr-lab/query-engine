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
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/planner"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func (s *Service) ipcHandler(w http.ResponseWriter, r *http.Request) {
	// Check if client requests streaming via headers
	if s.isStreamingRequest(r) {
		s.ipcStreamHandler(w, r)
		return
	}

	// Regular HTTP multipart response (existing implementation)
	s.ipcMultiPartHandler(w, r)
}

// ipcMultiPartHandler handles the IPC (Inter-Process Communication) requests.
// It is used to execute queries and return results in a streaming format.
// Accepts a JSON payload with the query and variables (like normal GraphQL).
func (s *Service) ipcMultiPartHandler(w http.ResponseWriter, r *http.Request) {
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

func (s *Service) queryIPC(ctx context.Context, mw *multipart.Writer, req types.Request) error {
	// add permissions to context
	ctx, err := s.perm.ContextWithPermissions(ctx)
	if err != nil {
		return err
	}
	schema := s.catalog.Schema()
	query, errs := gqlparser.LoadQueryWithRules(schema, req.Query, types.GraphQLQueryRules)
	if len(errs) != 0 {
		// write errors to IPC
		return writeErrorsToIPC(mw, "", errs)
	}
	if len(query.Operations) == 0 {
		return gqlerror.Errorf("no operations found")
	}

	ctx = planner.ContextWithRawResultsFlag(ctx)

	for _, op := range query.Operations {
		if req.OperationName != "" && req.OperationName != op.Name {
			continue
		}
		qq, _ := compiler.QueryRequestInfo(op.SelectionSet)
		qm := compiler.FlatQuery(qq)
		if len(qm) == 0 {
			return nil
		}
		data, ext, err := s.ProcessOperation(ctx, schema, op, req.Variables)
		if err != nil {
			return writeErrorsToIPC(mw, op.Name, types.WarpGraphQLError(err))
		}
		for path, q := range qm {
			qd := types.ExtractResponseData(path, data)
			if len(query.Operations) > 1 && req.OperationName == "" {
				path = op.Name + "." + path
			}
			err = writeDataIPC(mw, "data."+path, q, qd)
			if err != nil {
				types.DataClose(data)
				return err
			}
		}
		types.DataClose(data)
		// write extensions to IPC
		if len(ext) != 0 {
			path := "extensions"
			if len(query.Operations) > 1 && req.OperationName == "" {
				path = op.Name + "." + path
			}
			return writeExtensionsToIPC(mw, path, ext)
		}
	}

	return nil
}

func writeDataIPC(w *multipart.Writer, path string, query compiler.QueryRequest, data any) error {
	switch data := data.(type) {
	case db.ArrowTable:
		return writeArrowTableToIPC(w, path, query, data)
	default:
		return writeJsonValueToIPC(w, path, query, data)
	}
}

func writeArrowTableToIPC(w *multipart.Writer, path string, query compiler.QueryRequest, data db.ArrowTable) error {
	hdr := textproto.MIMEHeader{}
	hdr.Set("Content-Type", "application/vnd.apache.arrow.stream")
	hdr.Set("X-Hugr-Part-Type", "data")
	hdr.Set("X-Hugr-Path", path)
	hdr.Set("X-Hugr-Format", "table")
	if data == nil {
		hdr.Set("X-Hugr-Empty", "true")
		hdr.Set("X-Hugr-Table-Info", "{}")
		_, err := w.CreatePart(hdr)
		if err != nil {
			return err
		}
		return nil
	}
	defer data.Release()
	rr, err := data.Reader(false)
	if err != nil {
		return err
	}
	defer rr.Release()
	i := 0
	// writing first chunk to ensure we have at least one record
	if !rr.Next() {
		// empty table, write empty part
		hdr.Set("X-Hugr-Empty", "true")
		hdr.Set("X-Hugr-Table-Info", "{}")
		_, err := w.CreatePart(hdr)
		if err != nil {
			return err
		}
		return nil
	}
	meta := map[string]string{}
	hdr.Set("X-Hugr-Table-Info", data.Info())
	if gi, ok := geometryInfo(query.Field); ok {
		hdr.Set("X-Hugr-Geometry-Fields", gi)
		hdr.Set("X-Hugr-Geometry", "true")
	}
	pw, err := w.CreatePart(hdr)
	if err != nil {
		return err
	}
	chunk := rr.Record()
	if rr.Err() != nil {
		return rr.Err()
	}
	if chunk == nil {
		return gqlerror.Errorf("no data found for query %s", query.Field.Alias)
	}
	iw := ipc.NewWriter(pw)
	defer iw.Close()

	meta["chunk"] = strconv.Itoa(i)
	ns := addMeta(chunk.Schema(), meta)
	err = iw.Write(array.NewRecord(ns, chunk.Columns(), chunk.NumRows()))
	if err != nil {
		return err
	}
	i++

	// write remaining chunks
	for rr.Next() {
		chunk = rr.Record()
		if rr.Err() != nil {
			return rr.Err()
		}
		if chunk == nil {
			continue
		}
		meta["chunk"] = strconv.Itoa(i)
		ns := addMeta(chunk.Schema(), meta)
		err := iw.Write(array.NewRecord(ns, chunk.Columns(), chunk.NumRows()))
		if err != nil {
			return err
		}
		i++
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
		if query.Definition.Type.NamedType == compiler.H3CellTypeName {
			// H3 cell type is special, it has no geometry, but we can return its info
			return map[string]geomInfo{
				"": {
					Format: "H3Cell",
				},
			}
		}
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

func writeJsonValueToIPC(w *multipart.Writer, path string, query compiler.QueryRequest, val any) error {
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

func writeExtensionsToIPC(w *multipart.Writer, path string, ext any) error {
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

func writeErrorsToIPC(w *multipart.Writer, path string, errs gqlerror.List) error {
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
