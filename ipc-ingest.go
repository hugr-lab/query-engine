package hugr

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowingest "github.com/hugr-lab/query-engine/pkg/arrow-ingest"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/perm"
)

const (
	ingestContentType   = "application/vnd.apache.arrow.stream"
	ingestDataObjectArg = "data_object"
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
// inserts it into a table data object. The planner resolves the target schema,
// validates insert inputs/permissions, casts Arrow values, and builds the
// INSERT FROM SELECT statement over a temporary Arrow view.
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

	reader, err := ipc.NewReader(r.Body, ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		writeIngestError(w, http.StatusBadRequest, "invalid arrow stream: "+err.Error())
		return
	}
	defer reader.Release()
	source := arrowingest.NewSource(reader)

	plan, err := s.planner.PlanArrowIngest(ctx, s.schema.Provider(), dataObject, source)
	if err != nil {
		if errors.Is(err, auth.ErrForbidden) {
			writeIngestError(w, http.StatusForbidden, err.Error())
			return
		}
		writeIngestError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := plan.Compile(); err != nil {
		writeIngestError(w, http.StatusBadRequest, err.Error())
		return
	}

	if len(plan.Params) != 0 {
		writeIngestError(w, http.StatusInternalServerError, "arrow ingest plan produced SQL parameters")
		return
	}
	res, err := s.db.ExecArrowIngest(ctx, source, plan.CompiledQuery)
	if err != nil {
		writeIngestError(w, http.StatusInternalServerError, err.Error())
		return
	}
	inserted, _ := res.RowsAffected()

	out := IngestResponse{
		DataObject: dataObject,
		Inserted:   inserted,
		Columns:    ingestSchemaColumnNames(reader.Schema()),
	}
	_ = json.NewEncoder(w).Encode(out)
}

func ingestSchemaColumnNames(schema *arrow.Schema) []string {
	if schema == nil {
		return nil
	}
	out := make([]string, 0, schema.NumFields())
	for _, f := range schema.Fields() {
		out = append(out, f.Name)
	}
	return out
}

func writeIngestError(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ingestErrorBody{Error: msg})
}
