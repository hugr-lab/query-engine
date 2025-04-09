package metadata

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalogs"
	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2"

	_ "embed"
)

//go:embed schema.graphql
var testSchemaData string

func TestProcessQuery(t *testing.T) {
	cat, err := catalogs.NewCatalog(context.Background(),
		"test", "", &engines.DuckDB{}, sources.NewStringSource(testSchemaData), false)
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

	qd, errs := gqlparser.LoadQuery(cat.Schema(), query)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	if len(qd.Operations) == 0 {
		return
	}

	rqt, _ := compiler.QueryRequestInfo(qd.Operations[0].SelectionSet)

	for _, r := range rqt {
		data, err := ProcessQuery(t.Context(), cat.Schema(), r, 10)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%+v", data)
	}

}
