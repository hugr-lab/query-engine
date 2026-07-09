//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngest_HTTP_GeometryPoints_DuckDB(t *testing.T) {
	env := setupEnv(t)

	rec, schema := makeGeometryTypesRecord(t, []geometryTypesRow{
		{name: "geo-a", value: 1, active: true, point: xyPoint{X: 30.5, Y: 50.25}, shapeOrigin: xyPoint{X: 0, Y: 0}},
		{name: "geo-b", value: 2, active: true, point: xyPoint{X: -73.935242, Y: 40.730610}, shapeOrigin: xyPoint{X: 1, Y: 1}},
	})
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	resp, err := http.Post(env.server.URL+"/ipc/ingest?data_object="+env.dataObject,
		"application/vnd.apache.arrow.stream", &buf)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "body=%s", string(body))

	var out hugrclient.IngestResult
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Equal(t, int64(2), out.Inserted)
	assert.ElementsMatch(t, geometryTypesColumns(), out.Columns)

	ro := env.openRO(t)
	defer ro.Close()
	_, err = ro.Exec("LOAD spatial")
	require.NoError(t, err)

	rows, err := ro.Query(fmt.Sprintf(`
			SELECT name,
				%s
		FROM events
		WHERE name LIKE 'geo-%%'
		ORDER BY name
	`, geometrySelectList()))
	require.NoError(t, err)
	defer rows.Close()

	got := map[string][]string{}
	for rows.Next() {
		name, values := scanNamedGeometryValues(t, rows)
		got[name] = values
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string][]string{
		"geo-a": geometryExpected("POINT(30.5 50.25)", "0", "0"),
		"geo-b": geometryExpected("POINT(-73.935242 40.73061)", "1", "1"),
	}, got)
}
