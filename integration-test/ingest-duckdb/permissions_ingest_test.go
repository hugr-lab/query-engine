//go:build duckdb_arrow

package ingest_duckdb_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	hugrclient "github.com/hugr-lab/query-engine/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngest_DuckDB_PermissionData(t *testing.T) {
	env := setupEnv(t)

	const ownerID = 4242
	role := "ingest_perm_" + env.dsName
	registerIngestPermissionRole(t, env.service, role, moduleMutationName(env.dsName))

	now := arrow.Timestamp(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC).UnixMicro())
	rec := makeEventsRecord(t,
		[]string{"perm-alpha", "perm-beta"},
		[]float64{11.5, 12.5},
		[]bool{true, true},
		[]string{"", ""},
		[]arrow.Timestamp{now, now},
	)
	defer rec.Release()

	permClient := hugrclient.NewClient(env.server.URL+"/ipc",
		hugrclient.WithApiKey(ingestTestAPIKey),
		hugrclient.WithUserRole(role),
		hugrclient.WithUserInfo(strconv.Itoa(ownerID), "permission-user"),
	)
	res, err := permClient.IngestRecord(context.Background(), env.dataObject, rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(2), res.Inserted)
	assert.NotContains(t, res.Columns, "owner_id", "owner_id must be injected by permissions, not sent in Arrow")

	ro := env.openRO(t)
	defer ro.Close()
	rows, err := ro.Query("SELECT name, owner_id FROM events ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	got := map[string]int64{}
	for rows.Next() {
		var (
			name    string
			ownerID int64
		)
		require.NoError(t, rows.Scan(&name, &ownerID))
		got[name] = ownerID
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string]int64{
		"perm-alpha": ownerID,
		"perm-beta":  ownerID,
	}, got)
}

func TestIngest_DuckDB_PermissionDataGeometry(t *testing.T) {
	env := setupEnv(t)

	role := "ingest_perm_geom_" + env.dsName
	registerIngestPermissionRoleData(t, env.service, role, moduleMutationName(env.dsName), map[string]any{
		"geom": "POINT (7.25 8.5)",
	})

	now := arrow.Timestamp(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC).UnixMicro())
	rec := makeEventsRecord(t,
		[]string{"perm-geom-alpha", "perm-geom-beta"},
		[]float64{21.5, 22.5},
		[]bool{true, true},
		[]string{"", ""},
		[]arrow.Timestamp{now, now},
	)
	defer rec.Release()

	permClient := hugrclient.NewClient(env.server.URL+"/ipc",
		hugrclient.WithApiKey(ingestTestAPIKey),
		hugrclient.WithUserRole(role),
		hugrclient.WithUserInfo("7", "permission-geometry-user"),
	)
	res, err := permClient.IngestRecord(context.Background(), env.dataObject, rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(2), res.Inserted)
	assert.NotContains(t, res.Columns, "geom", "geom must be injected by permissions, not sent in Arrow")

	ro := env.openRO(t)
	defer ro.Close()
	_, err = ro.Exec("LOAD spatial")
	require.NoError(t, err)

	rows, err := ro.Query("SELECT name, ST_AsText(geom) FROM events ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	got := map[string]string{}
	for rows.Next() {
		var name, geom string
		require.NoError(t, rows.Scan(&name, &geom))
		got[name] = compactWKT(geom)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string]string{
		"perm-geom-alpha": "POINT(7.25 8.5)",
		"perm-geom-beta":  "POINT(7.25 8.5)",
	}, got)
}
