package catalogs

import (
	"bytes"
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/formatter"
)

func TestCatalog(t *testing.T) {
	source := sources.NewFileSource("../../internal/fixture")

	catalog, err := NewCatalog(context.Background(), "tf", "tf", &engines.Postgres{}, source, false)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(catalog.schema)

	var buf bytes.Buffer
	formatter.NewFormatter(&buf).FormatSchema(catalog.schema)

	t.Log(buf.String())
}
