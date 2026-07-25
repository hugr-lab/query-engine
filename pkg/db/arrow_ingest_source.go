package db

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/google/uuid"
)

const arrowIngestViewNamePrefix = "_hugr_arrow_view_"

// ArrowIngestSource binds an Arrow reader to the globally unique DuckDB view
// name used by both the planner and the ingest executor.
type ArrowIngestSource struct {
	Reader   array.RecordReader
	viewName string
}

func NewArrowIngestSource(reader array.RecordReader) ArrowIngestSource {
	return ArrowIngestSource{
		Reader:   reader,
		viewName: arrowIngestViewNamePrefix + strings.ReplaceAll(uuid.NewString(), "-", ""),
	}
}

func (s ArrowIngestSource) View() string {
	return s.viewName
}

// NeedsSpatial reports whether the Arrow source carries geometry extension
// metadata that requires DuckDB's spatial extension before registering the view.
func (s ArrowIngestSource) NeedsSpatial() bool {
	if s.Reader == nil || s.Reader.Schema() == nil {
		return false
	}
	for _, f := range s.Reader.Schema().Fields() {
		if extType, ok := f.Type.(arrow.ExtensionType); ok && isGeometryArrowExtension(extType.ExtensionName()) {
			return true
		}
		if ext, ok := f.Metadata.GetValue("ARROW:extension:name"); ok && isGeometryArrowExtension(ext) {
			return true
		}
		if ext, ok := f.Metadata.GetValue("extension:name"); ok && isGeometryArrowExtension(ext) {
			return true
		}
	}
	return false
}

// RegisterView registers the source reader under the source view name.
func (s ArrowIngestSource) RegisterView(arrowConn interface {
	RegisterView(reader array.RecordReader, viewName string) (func(), error)
}) (func(), error) {
	if s.Reader == nil {
		return nil, fmt.Errorf("missing arrow reader")
	}
	if s.View() == "" {
		return nil, fmt.Errorf("missing arrow view name")
	}
	return arrowConn.RegisterView(s.Reader, s.View())
}

func isGeometryArrowExtension(ext string) bool {
	ext = strings.ToLower(ext)
	return strings.HasPrefix(ext, "geoarrow.") ||
		ext == "hugr.geojson" ||
		ext == "geojson" ||
		ext == "hugr.hexwkb" ||
		ext == "hexwkb"
}
