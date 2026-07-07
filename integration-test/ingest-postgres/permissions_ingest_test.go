//go:build duckdb_arrow

package ingest_postgres_test

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

func TestIngest_Postgres_PermissionData(t *testing.T) {
	env := setupEnv(t)

	const ownerID = 4343
	role := "ingest_perm_pg"
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
	res, err := permClient.IngestRecord(context.Background(), "pg_ingest.events", rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(2), res.Inserted)
	assert.NotContains(t, res.Columns, "owner_id", "owner_id must be injected by permissions, not sent in Arrow")

	rows, err := env.pgConn.Query("SELECT name, owner_id FROM events ORDER BY name")
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

func TestIngest_Postgres_PermissionDataGeometry(t *testing.T) {
	env := setupEnv(t)

	role := "ingest_perm_geom_pg"
	registerIngestPermissionRoleData(t, env.service, role, moduleMutationName(env.dsName), map[string]any{
		"geom_point_native": "POINT (7.25 8.5)",
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
	res, err := permClient.IngestRecord(context.Background(), "pg_ingest.events", rec)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(2), res.Inserted)
	assert.NotContains(t, res.Columns, "geom_point_native", "geom_point_native must be injected by permissions, not sent in Arrow")

	rows, err := env.pgConn.Query("SELECT name, ST_AsText(geom_point_native), ST_SRID(geom_point_native) FROM events ORDER BY name")
	require.NoError(t, err)
	defer rows.Close()

	got := map[string]string{}
	gotSRID := map[string]int{}
	for rows.Next() {
		var name, geom string
		var srid int
		require.NoError(t, rows.Scan(&name, &geom, &srid))
		got[name] = compactWKT(geom)
		gotSRID[name] = srid
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string]string{
		"perm-geom-alpha": "POINT(7.25 8.5)",
		"perm-geom-beta":  "POINT(7.25 8.5)",
	}, got)
	assert.Equal(t, map[string]int{
		"perm-geom-alpha": 0,
		"perm-geom-beta":  0,
	}, gotSRID)
}
