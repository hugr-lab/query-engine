package hugr

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/hugr-lab/query-engine/types"
)

func (s *Service) Ingest(ctx context.Context, table string, reader array.RecordReader, opts ...types.IngestOpt) (int64, error) {
	return 0, nil
}
