package metadata

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/engines"

	_ "embed"
)

//go:embed schema_test.graphql
var testSchemaData string

func TestProcessQuery(t *testing.T) {
	provider, err := static.New()
	if err != nil {
		t.Fatal(err)
	}
	ss := catalog.NewService(provider)
	e := &engines.DuckDB{}
	cat, err := sources.NewStringSource("test", e, compiler.Options{
		Name:         "test",
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}, testSchemaData)
	if err != nil {
		t.Fatal(err)
	}
	err = ss.AddCatalog(context.Background(), "test", cat)
	if err != nil {
		t.Fatal(err)
	}

	query := `
	 query IntrospectionQuery {
      __schema {

        queryType { name }
        mutationType { name }
        subscriptionType { name }
        types {
          ...FullType
        }
        directives {
          name
          description

          locations
          args {
            ...InputValue
          }
        }
      }
    }

    fragment FullType on __Type {
      kind
      name
      description


      fields(includeDeprecated: true) {
        name
        description
        args {
          ...InputValue
        }
        type {
          ...TypeRef
        }
        isDeprecated
        deprecationReason
      }
      inputFields {
        ...InputValue
      }
      interfaces {
        ...TypeRef
      }
      enumValues(includeDeprecated: true) {
        name
        description
        isDeprecated
        deprecationReason
      }
      possibleTypes {
        ...TypeRef
      }
    }

    fragment InputValue on __InputValue {
      name
      description
      type { ...TypeRef }
      defaultValue


    }

    fragment TypeRef on __Type {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
	`

	op, err := ss.ParseQuery(context.Background(), query, nil, "IntrospectionQuery")
	if err != nil {
		t.Fatal(err)
	}

	rqt, _ := sdl.QueryRequestInfo(op.Definition.SelectionSet)

	for _, r := range rqt {
		data, err := ProcessQuery(t.Context(), ss.Provider(), r, 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%+v", data)
	}

}
