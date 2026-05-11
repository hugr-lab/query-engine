package engines

import (
	"regexp"
	"strconv"
)

// sqlParamRegExp matches a `$N` SQL parameter placeholder.
var sqlParamRegExp = regexp.MustCompile(`\$(\d+)`)

// castParamRefs rewrites every `$N` placeholder in sql whose N is strictly
// greater than skipBelow to `CAST($N AS sqlType)`. Placeholders bound before
// the JSONFieldFilter pipeline started (e.g. a COALESCE default that was
// appended into params earlier) are left untouched. Engines use this from
// their inline JSONFieldFilter switch to reconcile a stringified parameter
// (after value coercion) with the typed JSON-extraction side, e.g. for
// DATE / TIME / TIMESTAMP / INTERVAL.
func castParamRefs(sql, sqlType string, skipBelow int) string {
	if sqlType == "" {
		return sql
	}
	return sqlParamRegExp.ReplaceAllStringFunc(sql, func(m string) string {
		idx, err := strconv.Atoi(m[1:])
		if err != nil || idx <= skipBelow {
			return m
		}
		return "CAST(" + m + " AS " + sqlType + ")"
	})
}
