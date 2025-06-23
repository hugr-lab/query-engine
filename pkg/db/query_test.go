package db

import (
	"context"
	"encoding/json"
	"testing"
)

func TestQueryJsonTable(t *testing.T) {
	db, err := NewPool("")
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	query := `
		SELECT 1 AS col1, 'test' AS col2
		UNION
		SELECT 2 AS col1, 'test2' AS col2
	`

	table, err := db.QueryJsonTableArrow(ctx, query, false)
	if err != nil {
		t.Fatalf("QueryJsonTable() error = %v", err)
	}
	if table == nil {
		t.Fatalf("QueryJsonTable() returned nil table")
	}
	rr, err := table.Records()
	if err != nil {
		t.Fatalf("table.Records() error = %v", err)
	}
	if len(rr) == 0 {
		t.Fatalf("table.Records() returned empty records")
	}
	defer ReleaseRecords(rr)
	if got := RecordsColNums(rr); got != 2 {
		t.Errorf("NumCols() = %v, want %v", got, 2)
	}
	if got := RecordsRowNums(rr); got != 2 {
		t.Errorf("NumRows() = %v, want %v", got, 2)
	}
}

func TestQueryTableToSlice(t *testing.T) {
	db, err := NewPool("")
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	query := `		
		SELECT *
		FROM (
			VALUES (1, 'test'), (2, 'test2')
		) AS t(col1, col2)
	`

	var data []map[string]interface{}
	err = db.QueryTableToSlice(ctx, &data, query)
	if err != nil {
		t.Fatalf("QueryTableToSlice() error = %v", err)
	}
	if len(data) != 2 {
		t.Errorf("len(data) = %v, want %v", len(data), 2)
	}
	if got, want := data[0]["col1"], 1.; got != want {
		t.Errorf("data[0][\"col1\"] = %v, want %v", got, want)
	}
	if got, want := data[0]["col2"], "test"; got != want {
		t.Errorf("data[0][\"col2\"] = %v, want %v", got, want)
	}
}

func TestQueryJsonRow(t *testing.T) {
	db, err := NewPool("")
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	query := "SELECT 1 AS col1, 'test' AS col2"

	row, err := db.QueryJsonRow(ctx, query)
	if err != nil {
		t.Fatalf("QueryJsonRow() error = %v", err)
	}
	if row == nil {
		t.Fatalf("QueryJsonRow() returned nil row")
	}

	var data map[string]interface{}
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	err = json.Unmarshal(b, &data)
	if err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got, want := data["col1"], float64(1); got != want {
		t.Errorf("data[\"col1\"] = %v, want %v", got, want)
	}
	if got, want := data["col2"], "test"; got != want {
		t.Errorf("data[\"col2\"] = %v, want %v", got, want)
	}
}

func TestQueryRowToData(t *testing.T) {
	db, err := NewPool("")
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	query := "SELECT 1 AS col1, 'test' AS col2"

	var data map[string]interface{}
	err = db.QueryRowToData(ctx, &data, query)
	if err != nil {
		t.Fatalf("QueryRowToSlice() error = %v", err)
	}
	if got, want := data["col1"], 1.; got != want {
		t.Errorf("data[\"col1\"] = %v, want %v", got, want)
	}
	if got, want := data["col2"], "test"; got != want {
		t.Errorf("data[\"col2\"] = %v, want %v", got, want)
	}
}
