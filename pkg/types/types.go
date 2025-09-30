package types

import (
	"time"

	"github.com/vektah/gqlparser/v2/validator/rules"
)

var GraphQLQueryRules *rules.Rules

func init() {
	// remove unused variable GraphQL validation rule
	rr := rules.NewDefaultRules()
	rr.RemoveRule(rules.NoUnusedVariablesRule.Name)
	GraphQLQueryRules = rr
}

type ScalarTypes interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string | ~bool | Int32Range | Int64Range | TimeRange | BaseRange | time.Time
}

type Dimentional interface {
	Len() int
}
