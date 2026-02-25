package sdl

import (
	"regexp"
	"strings"
)

func RemoveFieldsDuplicates(fields []string) []string {
	unique := make(map[string]struct{}, len(fields))
	var uniqueFields []string
	for _, f := range fields {
		if _, ok := unique[f]; ok {
			continue
		}
		unique[f] = struct{}{}
		uniqueFields = append(uniqueFields, f)
	}
	return uniqueFields
}

var reSQLField = regexp.MustCompile(`\[\$?[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*\]`)

func ExtractFieldsFromSQL(sql string) []string {
	if sql == "" {
		return nil
	}
	matches := reSQLField.FindAllString(sql, -1)
	if matches == nil {
		return nil
	}
	fields := make([]string, 0, len(matches))
	for _, match := range matches {
		fields = append(fields, strings.Trim(match, "[]"))
	}
	return RemoveFieldsDuplicates(fields)
}
