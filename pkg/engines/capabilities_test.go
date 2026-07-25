package engines

import "testing"

func TestDuckLakeCapabilitiesSupportIngest(t *testing.T) {
	caps := NewDuckLake().Capabilities()

	if !caps.Insert.Insert {
		t.Fatal("DuckLake should support regular INSERT")
	}
	if !caps.Ingest.Insert {
		t.Fatal("DuckLake should support append-only IPC ingest")
	}
	if caps.Ingest.Merge {
		t.Fatal("DuckLake MERGE ingest is not implemented yet")
	}
}

func TestIcebergCapabilitiesSupportTargetedInsertAndIngest(t *testing.T) {
	caps := NewIceberg().Capabilities()

	if !caps.Insert.Insert {
		t.Fatal("Iceberg should support targeted INSERT")
	}
	if !caps.Ingest.Insert {
		t.Fatal("Iceberg should support append-only IPC ingest")
	}
	if caps.Ingest.Merge {
		t.Fatal("Iceberg MERGE ingest is not implemented yet")
	}
	if caps.Insert.Returning {
		t.Fatal("Iceberg INSERT RETURNING is not supported")
	}
	if caps.Insert.InsertReferences {
		t.Fatal("Iceberg insert references are not supported")
	}
}
