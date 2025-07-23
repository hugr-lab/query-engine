package gis

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/jq"
	"github.com/hugr-lab/query-engine/pkg/planner"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/paulmach/orb/geojson"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
)

var (
	ErrEmptyRequest = errors.New("empty request")
)

const (
	resultTypeGeoJSON   = "geojson"
	resultTypeGeoNDJSON = "geo-ndjson"
	resultTypeGML       = "gml"
	resultTypeArrow     = "arrow"
)

// Handle graphQL POST requests and returns feature collection geojson/ndjson.
func (s *Service) queryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// parse content type
	contentType, ct, err := parseContentType(r.Header.Get("Accept"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
		return
	}
	var req types.Request
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	feature := r.URL.Query().Get("feature")
	var features []string
	if feature != "" {
		features = strings.Split(feature, ",")
	}
	fm, err := parseRequest(s.schema.Schema(), &req, features)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if ct == resultTypeArrow && len(fm) != 1 {
		http.Error(w, "Arrow format supports only for one feature", http.StatusBadRequest)
		return
	}
	if ct == resultTypeArrow {
		var fd featureDefinition
		for _, f := range fm {
			fd = f
			break
		}
		if fd.PropertiesJQ != "" && fd.Definition == nil {
			http.Error(w, "Arrow format with transformation requires feature definition", http.StatusBadRequest)
			return
		}
	}

	ctx := planner.ContextWithRawResultsFlag(r.Context())
	res, err := s.qe.Query(ctx, req.Query, req.Variables)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer res.Close()
	if res.Err() != nil {
		http.Error(w, res.Err().Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)

	// create jq transformer if needed
	writeSummary := len(fm) > 0
	for name, f := range fm {
		if f.Summary == "" {
			writeSummary = false
		}
		if f.PropertiesJQ != "" {
			t, err := jq.NewTransformer(ctx, f.PropertiesJQ, jq.WithVariables(req.Variables), jq.WithQuerier(s.qe))
			if err != nil {
				http.Error(w, "JQ compiler: "+err.Error(), http.StatusInternalServerError)
				return
			}
			f.transformer = t
			fm[name] = f
		}
	}

	col := featureCollection{
		res:               res,
		fm:                fm,
		writeSummary:      writeSummary,
		writeResultsCount: writeSummary,
		writeExtent:       writeSummary,
	}

	// process features to collection
	switch ct {
	case resultTypeGeoJSON:
		err = col.EncodeJSON(ctx, w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case resultTypeGeoNDJSON:
		_, err = col.EncodeFeatures(ctx, w,
			func(w io.Writer, feature *geojson.Feature, fd featureDefinition) error {
				b, err := feature.MarshalJSON()
				if err != nil {
					return fmt.Errorf("error encoding feature %s: %w", fd.Name, err)
				}
				_, err = w.Write(b)
				return err
			},
			"\n",
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case resultTypeGML:
		col.namespaceBaseURL = baseURL(r)
		col.namespaceSchemaDir = "_query"
		err = col.EncodeGML(ctx, w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case resultTypeArrow:
	default:
		http.Error(w, "Unsupported Accept header: "+ct, http.StatusNotAcceptable)
		return
	}
}

// parseContentType parses the content type header and returns the type and subtype.
func parseContentType(contentType string) (string, string, error) {
	cpp := strings.Split(contentType, ",")
	for _, c := range cpp {
		switch c {
		case "", "*/*", "application/geo+json":
			return "application/geo+json", resultTypeGeoJSON, nil
		case "application/json":
			return "application/json", resultTypeGeoJSON, nil
		case "application/x-ndjson":
			return "application/x-ndjson", resultTypeGeoNDJSON, nil
		case "application/geo+json-seq":
			return "application/geo+json-seq", resultTypeGeoNDJSON, nil
		case "text/xml; subtype=gml/3.1.1", "application/gml+xml; subtype=gml/3.1.2":
			return "text/xml; subtype=gml/3.1.1", resultTypeGML, nil
		case "application/gml+xml":
			return "application/gml+xml", resultTypeGML, nil
		case "application/x-arrow":
			return "application/x-arrow", resultTypeArrow, nil
		case "application/vnd.apache.arrow.file":
			return "application/vnd.apache.arrow.file", resultTypeArrow, nil
		case "application/vnd.apache.arrow.stream":
			return "application/vnd.apache.arrow.stream", resultTypeArrow, nil
		case "application/x-arrow-stream":
			return "application/x-arrow-stream", resultTypeArrow, nil
		}
	}
	if len(cpp) > 0 {
		return "", "", fmt.Errorf("unsupported content type: %s", contentType)
	}
	return "", "", fmt.Errorf("empty content type")
}

// parseRequest parses the GraphQL request, filter it if feature list provided and returns the feature definitions and any errors.
func parseRequest(schema *ast.Schema, req *types.Request, features []string) (featureMap map[string]featureDefinition, err error) {
	qd, errs := gqlparser.LoadQueryWithRules(schema, req.Query, types.GraphQLQueryRules)
	if len(errs) != 0 {
		return nil, errs
	}
	if len(qd.Operations) == 0 {
		return nil, ErrEmptyRequest
	}

	if req.OperationName == "" && len(qd.Operations) > 1 {
		return nil, errors.New("operation name is required for multiple operations")
	}
	op := qd.Operations.ForName(req.OperationName)
	if op == nil {
		return nil, errors.New("operation not found: " + req.OperationName)
	}
	if len(op.SelectionSet) == 0 {
		return nil, ErrEmptyRequest
	}
	filtered, fm, err := filterRecursive(op.SelectionSet, features, req.Variables, false)
	if err != nil {
		return nil, err
	}
	if len(filtered) == 0 {
		return nil, ErrEmptyRequest
	}
	// add summary requests if needed
	ss := engines.SelectedFields(op.SelectionSet)
	for _, f := range fm {
		if f.Summary == "" {
			continue
		}
		s := ss.ForPath(f.Summary)
		if s == nil {
			return nil, fmt.Errorf("summary field %s not found in feature %s", f.Summary, f.Name)
		}
		if s.Field.SelectionSet == nil {
			return nil, compiler.ErrorPosf(s.Field.Position, "summary field %s is not a selection set in feature %s", f.Summary, f.Name)
		}
		if f.ExtentPath != "" && engines.SelectedFields(s.Field.SelectionSet).ForPath(f.ExtentPath) == nil {
			return nil, compiler.ErrorPosf(s.Field.Position, "extent field %s not found in summary %s of feature %s", f.ExtentPath, f.Summary, f.Name)
		}
		if f.CountPath != "" && engines.SelectedFields(s.Field.SelectionSet).ForPath(f.CountPath) == nil {
			return nil, compiler.ErrorPosf(s.Field.Position, "count field %s not found in summary %s of feature %s", f.CountPath, f.Summary, f.Name)
		}
		filtered = addSelectionByPath(filtered, op.SelectionSet, f.Summary)
	}

	var sb strings.Builder
	formatter.NewFormatter(&sb).FormatQueryDocument(qd)
	req.Query = sb.String()
	return fm, nil
}

func filterRecursive(selSet ast.SelectionSet, features []string, vars map[string]any, summaryOnly bool) (ast.SelectionSet, map[string]featureDefinition, error) {
	ss := engines.SelectedFields(selSet)
	if len(ss) == 0 {
		return nil, nil, nil
	}
	var filtered engines.SelectionSet
	featuresMap := make(map[string]featureDefinition)
	for _, s := range ss {
		if len(s.Field.SelectionSet) == 0 {
			continue
		}
		df := s.Field.Directives.ForName(base.GisFeatureDirectiveName)
		if df == nil {
			selections, fm, err := filterRecursive(s.Field.SelectionSet, features, vars, summaryOnly)
			if err != nil {
				return nil, nil, err
			}
			if len(selections) == 0 {
				continue
			}
			for name, f := range fm {
				featuresMap[s.Field.Alias+"."+name] = f
			}
			filtered = append(filtered, engines.SelectedField{
				Field: &ast.Field{
					Name:             s.Field.Name,
					Alias:            s.Field.Alias,
					SelectionSet:     selections,
					Position:         s.Field.Position,
					Directives:       s.Field.Directives,
					Comment:          s.Field.Comment,
					ObjectDefinition: s.Field.ObjectDefinition,
					Definition:       s.Field.Definition,
				},
			})
			continue
		}
		name := compiler.DirectiveArgValue(df, "name", vars)
		if name == "" || features != nil && !slices.Contains(features, name) {
			continue
		}
		fd, err := newFeatureDefinition(df, vars)
		if err != nil {
			return nil, nil, compiler.ErrorPosf(s.Field.Position, "error parsing feature %s: %v", name, err)
		}
		if !summaryOnly {
			filtered = append(filtered, s)
			if engines.SelectedFields(s.Field.SelectionSet).ForPath(fd.GeometryField) == nil {
				return nil, nil, compiler.ErrorPosf(s.Field.Position, "geometry field %s not found in feature %s", fd.GeometryField, name)
			}
			if fd.IdField != "" && engines.SelectedFields(s.Field.SelectionSet).ForPath(fd.IdField) == nil {
				return nil, nil, compiler.ErrorPosf(s.Field.Position, "id field %s not found in feature %s", fd.IdField, name)
			}
		}
		featuresMap[s.Field.Alias] = fd
	}
	return filtered.AsSelectionSet(), featuresMap, nil
}

func addSelectionByPath(origin, from ast.SelectionSet, path string) ast.SelectionSet {
	if path == "" {
		return from
	}
	pp := strings.SplitN(path, ".", 2)
	if len(pp) == 0 {
		return origin
	}
	s := engines.SelectedFields(from).ForAlias(pp[0])
	if s == nil {
		return origin
	}
	if len(pp) == 1 {
		return append(origin, s.Field)
	}
	so := engines.SelectedFields(origin).ForAlias(pp[0])
	if so == nil {
		ss := addSelectionByPath(nil, s.Field.SelectionSet, pp[1])
		if ss == nil {
			return origin
		}
		return append(origin, &ast.Field{
			Name:             s.Field.Name,
			Alias:            s.Field.Alias,
			SelectionSet:     ss,
			Position:         s.Field.Position,
			Directives:       s.Field.Directives,
			Comment:          s.Field.Comment,
			ObjectDefinition: s.Field.ObjectDefinition,
			Definition:       s.Field.Definition,
		})
	}
	ss := addSelectionByPath(so.Field.SelectionSet, s.Field.SelectionSet, pp[1])
	if ss == nil {
		return origin
	}
	so.Field.SelectionSet = ss
	return origin
}
