//go:build duckdb_arrow

package engines

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

// mkudLineStringGeoJSON matches MKUD /GetTrafficLightById directions[].coordinates
// (GeoJSON serialized as a JSON string value).
const mkudLineStringGeoJSON = `{"type":"LineString","coordinates":[[66.52748007,66.630207063],[66.52747884,66.630249448],[66.527478439,66.630289574],[66.527479052,66.630334744],[66.527480859,66.630376662],[66.527483277,66.630409109],[66.527486523,66.630439297],[66.5274906,66.630467227],[66.527495505,66.630492899],[66.527501239,66.630516312],[66.527507802,66.630537466],[66.527516773,66.63055987],[66.52752516,66.630576056],[66.527534377,66.630589983],[66.527546531,66.630603714],[66.527557572,66.630612673],[66.527571915,66.630620442],[66.52758478,66.630624431],[66.527598474,66.630626162],[66.527612996,66.630625635]]}`

// trafficLightLightFieldForHTTPIntegration matches GraphQL
// light { directions { coordinates } } with OpenAPI StringAsGeoJson → ST_GeomFromGeoJSON([coordinates]).
func trafficLightLightFieldForHTTPIntegration() *ast.Field {
	coordsDef := &ast.FieldDefinition{
		Name: "coordinates",
		Type: ast.NamedType("Geometry", nil),
		Directives: ast.DirectiveList{
			base.FieldSqlDirective("ST_GeomFromGeoJSON([coordinates])"),
		},
	}
	dirType := &ast.Definition{
		Name:   "Direction",
		Fields: []*ast.FieldDefinition{coordsDef},
	}
	return &ast.Field{
		Alias: "light",
		Name:  "light",
		SelectionSet: ast.SelectionSet{
			&ast.Field{
				Alias: "directionsOut",
				Name:  "directions",
				SelectionSet: ast.SelectionSet{
					&ast.Field{
						Alias:            "coords",
						Name:             "coordinates",
						Definition:       coordsDef,
						ObjectDefinition: dirType,
					},
				},
				ObjectDefinition: dirType,
				Definition: &ast.FieldDefinition{
					Name: "directions",
					Type: &ast.Type{NamedType: "", Elem: ast.NamedType("Direction", nil)},
				},
			},
		},
		Definition: &ast.FieldDefinition{
			Name: "light", Type: ast.NamedType("Object", nil),
		},
		ObjectDefinition: &ast.Definition{Fields: []*ast.FieldDefinition{
			{Name: "light", Type: ast.NamedType("Object", nil)},
		}},
	}
}

// TestDuckDB_HTTPResponseTrafficLightDirectionsCoordinates runs json_transform + RepackObject
// against a JSON document shaped like the MKUD HTTP body fragment (directions[].coordinates
// as a GeoJSON string). This is the DuckDB path used for HTTP scalar JSON function results
// with nested OpenAPI Geometry fields.
func TestDuckDB_HTTPResponseTrafficLightDirectionsCoordinates(t *testing.T) {
	payload, err := json.Marshal(map[string]any{
		"directions": []map[string]any{
			{
				"conflictDirections": []string{},
				"coordinates":        mkudLineStringGeoJSON,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	lightField := trafficLightLightFieldForHTTPIntegration()
	e := NewDuckDB()

	jsonTransform := JsonToStruct(lightField, "", true, true)
	repackExpr := e.RepackObject("_value", lightField)

	// Column alias "light" holds the raw JSON; json_transform(light, …) matches functionCallSQL JsonCast.
	query := `SELECT CAST(to_json(` + repackExpr + `) AS VARCHAR) AS out FROM (
		SELECT (` + jsonTransform + `) AS _value
		FROM (SELECT $1::JSON AS light)
	) sub`

	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("duckdb connector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	_, err = db.ExecContext(context.Background(), `INSTALL spatial; LOAD spatial;`)
	if err != nil {
		t.Fatalf("load spatial: %v", err)
	}

	var out string
	err = db.QueryRowContext(context.Background(), query, string(payload)).Scan(&out)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	var root map[string]any
	if err := json.Unmarshal([]byte(out), &root); err != nil {
		t.Fatalf("unmarshal result: %v\n%s", err, out)
	}
	dirAny, ok := root["directionsOut"]
	if !ok {
		t.Fatalf("missing directionsOut in %s", out)
	}
	dirs, ok := dirAny.([]any)
	if !ok || len(dirs) == 0 {
		t.Fatalf("directionsOut not a non-empty array: %#v", dirAny)
	}
	row0, ok := dirs[0].(map[string]any)
	if !ok {
		t.Fatalf("direction row type %T", dirs[0])
	}
	coordsStr := coordsToStringForAssert(t, row0["coords"])
	if !strings.Contains(coordsStr, `"type":"LineString"`) && !strings.Contains(coordsStr, `"type": "LineString"`) {
		t.Fatalf("expected LineString GeoJSON in coords, got: %s", coordsStr)
	}
}

func coordsToStringForAssert(t *testing.T, v any) string {
	t.Helper()
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		b, err := json.Marshal(x)
		if err != nil {
			t.Fatalf("marshal coords: %v", err)
		}
		return string(b)
	default:
		t.Fatalf("coords type %T", v)
		return ""
	}
}
