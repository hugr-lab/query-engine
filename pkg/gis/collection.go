package gis

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/geojson"
)

var (
	ErrInvalidFeaturePath        = errors.New("invalid feature path")
	ErrInvalidSummaryExtentValue = errors.New("invalid summary extent value")
)

type featureCollection struct {
	namespaceSchemaDir string
	namespaceBaseURL   string
	res                *types.Response
	fm                 map[string]featureDefinition
	writeSummary       bool
	writeResultsCount  bool
	writeExtent        bool
}

func (fc *featureCollection) EncodeJSON(ctx context.Context, w io.Writer) error {
	if fc.res == nil {
		return types.ErrNoData
	}
	if fc.fm == nil {
		fc.fm = make(map[string]featureDefinition)
	}
	_, err := w.Write([]byte(`{"type":"FeatureCollection"`))
	if err != nil {
		return err
	}

	// summary collection info
	if fc.writeSummary {
		_, err = w.Write([]byte(`,`))
		if err != nil {
			return err
		}

		cs, err := fc.Summary()
		if err != nil {
			return err
		}
		if !fc.writeExtent {
			cs.SkipExtent = true
		}
		err = cs.EncodeJSON(w)
		if err != nil {
			return err
		}
	}

	_, err = w.Write([]byte(`,"features":[`))
	if err != nil {
		return err
	}
	n, err := fc.EncodeFeatures(ctx, w,
		func(w io.Writer, feature *geojson.Feature, fd featureDefinition) error {
			if len(fc.fm) > 1 && feature.ID != nil {
				feature.ID = fd.Name + ":" + fmt.Sprint(feature.ID)
			}
			b, err := feature.MarshalJSON()
			if err != nil {
				return fmt.Errorf("error encoding feature %s: %w", fd.Name, err)
			}
			_, err = w.Write(b)
			return err
		},
		",",
	)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(`]`))
	if err != nil {
		return err
	}
	if fc.writeResultsCount {
		_, err = fmt.Fprintf(w, `,"numberReturned":%d`, n)
	}
	_, err = w.Write([]byte(`}`))
	return err
}

func (fc *featureCollection) EncodeGML(ctx context.Context, w io.Writer) error {
	// 1. write XML header
	_, err := w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>`))
	if err != nil {
		return err
	}
	// 2. write wfs:FeatureCollection start tag
	e := xml.NewEncoder(w)
	defer e.Close()
	wfs := xml.StartElement{Name: xml.Name{Local: "wfs:FeatureCollection"}}
	wfs.Attr, err = fc.wfsCollectionAttributes()
	if err != nil {
		return fmt.Errorf("error writing wfs:FeatureCollection attributes: %w", err)
	}
	if err := e.EncodeToken(wfs); err != nil {
		return fmt.Errorf("error writing wfs:FeatureCollection start tag: %w", err)
	}
	// 3. write features
	_, err = fc.EncodeFeatures(ctx, w,
		func(w io.Writer, feature *geojson.Feature, fd featureDefinition) error {
			prefix := ""
			name := fd.Name
			if pp := strings.SplitN(fd.Name, ":", 2); len(pp) > 1 {
				prefix = pp[0]
				name = pp[1]
			}
			// 4.1 write <gml:featureMember> start tag
			id := name + "." + fmt.Sprint(feature.ID)
			fm := xml.StartElement{Name: xml.Name{Local: "gml:featureMember"}}
			fm.Attr = []xml.Attr{
				{Name: xml.Name{Local: "gml:id"}, Value: id},
			}
			if err := e.EncodeToken(fm); err != nil {
				return fmt.Errorf("error writing gml:featureMember start tag: %w", err)
			}
			defer e.EncodeToken(fm.End())
			// 4.2 write feature type start tag
			ft := xml.StartElement{Name: xml.Name{Local: fd.Name}}
			if err := e.EncodeToken(ft); err != nil {
				return fmt.Errorf("error writing feature type start tag: %w", err)
			}
			defer e.EncodeToken(ft.End())
			// 4.2 encode feature properties
			for k, v := range feature.Properties {
				if v == nil {
					continue
				}
				val, err := encodeGmlPropertyValue(k, v, fd.Definition)
				if errors.Is(err, errPropertyIsNotDefined) {
					continue
				}
				if err != nil {
					return fmt.Errorf("error encoding feature property %s: %w", k, err)
				}
				if prefix != "" {
					k = prefix + ":" + k
				}
				// write property
				err = e.EncodeElement(val, xml.StartElement{Name: xml.Name{Local: k}})
				if err != nil {
					return fmt.Errorf("error writing feature property %s: %w", k, err)
				}
			}

			if feature.Geometry == nil {
				return nil
			}
			// 4.3 encode feature geometry
			geomFieldName := "geom"
			if prefix != "" {
				geomFieldName = prefix + ":" + geomFieldName
			}
			gt := xml.StartElement{Name: xml.Name{Local: geomFieldName}}
			if err := e.EncodeToken(gt); err != nil {
				return fmt.Errorf("error writing geometry field start tag: %w", err)
			}
			// 4.4 encode geometry
			err = encodeGmlGeometry(e, feature.Geometry, fd.GeometrySRID)
			if err != nil {
				return fmt.Errorf("error encoding feature geometry: %w", err)
			}

			// 4.5 write end tag for geometry field
			if err := e.EncodeToken(gt.End()); err != nil {
				return fmt.Errorf("error writing feature geometry end tag: %w", err)
			}
			return nil
		},
		"",
	)

	// 5. write wfs:FeatureCollection end tag
	return e.EncodeToken(wfs.End())
}

var (
	baseWFSCollectionAttributes = []xml.Attr{
		{Name: xml.Name{Local: "xmlns:wfs"}, Value: "http://www.opengis.net/wfs"},
		{Name: xml.Name{Local: "xmlns:gml"}, Value: "http://www.opengis.net/gml"},
		{Name: xml.Name{Local: "xmlns:xsi"}, Value: "http://www.w3.org/2001/XMLSchema-instance"},
	}
	baseWFSCollectionSchemaLocation = []string{
		"http://www.opengis.net/wfs",
		"http://www.opengis.net/gml",
		"http://schemas.opengis.net/gml/3.1.1/base/gml.xsd",
	}
)

func (fc *featureCollection) wfsCollectionAttributes() ([]xml.Attr, error) {
	sl := baseWFSCollectionSchemaLocation
	attr := baseWFSCollectionAttributes
	pm := make(map[string]struct{})
	for _, f := range fc.fm {
		if fc.namespaceBaseURL == "" {
			continue
		}
		pp := strings.SplitN(f.Name, ":", 2)
		name := pp[0]
		prefix := ""
		if len(pp) > 1 {
			prefix = pp[0]
			name = pp[1]
		}
		if strings.Contains(name, ":") {
			return nil, fmt.Errorf("invalid feature name %s, should not contain more than one ':'", f.Name)
		}
		if prefix == "" {
			if len(fc.fm) > 1 {
				// if there are multiple feature types, we need to add namespace
				// to avoid conflicts
				return nil, fmt.Errorf("feature name %s should have a prefix to avoid conflicts in multi-query request", f.Name)
			}
			if fc.namespaceSchemaDir == "" {
				name = fc.namespaceSchemaDir + "/" + name
			}
			sl = append(sl,
				"\""+fc.namespaceBaseURL+"/gis/wfs/collections/"+name+"\"",
				"\""+fc.namespaceBaseURL+"/gis/wfs/schemas/"+name+".xsd\"",
			)
			attr = append(attr, xml.Attr{
				Name:  xml.Name{Local: "xmlns"},
				Value: fc.namespaceBaseURL + "/gis/wfs/collections/" + name,
			})
			return append(attr,
				xml.Attr{
					Name: xml.Name{Local: "xsi:schemaLocation"},
					Value: strings.Join(append(baseWFSCollectionSchemaLocation,
						fc.namespaceBaseURL+"/gis/wfs/collections/"+name,
						fc.namespaceBaseURL+"/gis/wfs/schemas/"+name+".xsd",
					), " "),
				}), nil
		}
		if _, ok := pm[prefix]; ok {
			// prefix already exists, skip
			continue
		}
		if fc.namespaceSchemaDir != "" {
			prefix = fc.namespaceSchemaDir + "/" + prefix
		}
		pm[prefix] = struct{}{}
		sl = append(sl,
			fc.namespaceBaseURL+"/gis/wfs/collections/"+prefix,
			fc.namespaceBaseURL+"/gis/wfs/schemas/"+prefix+".xsd",
		)
		attr = append(attr, xml.Attr{
			Name:  xml.Name{Local: "xmlns:" + prefix},
			Value: fc.namespaceBaseURL + "/gis/wfs/collections/" + prefix,
		})
	}
	attr = append(attr, xml.Attr{
		Name:  xml.Name{Local: "xsi:schemaLocation"},
		Value: strings.Join(sl, " "),
	})
	if fc.writeSummary {
		cs, err := fc.Summary()
		if err != nil {
			return nil, fmt.Errorf("error getting collection summary: %w", err)
		}
		attr = append(attr, xml.Attr{
			Name:  xml.Name{Local: "numberMatched"},
			Value: fmt.Sprintf("%d", cs.Count),
		})
	}
	attr = append(attr, xml.Attr{
		Name:  xml.Name{Local: "timeStamp"},
		Value: time.Now().UTC().Format(time.RFC3339),
	})

	return attr, nil
}

type collectionSummary struct {
	SkipExtent bool
	Extent     orb.Bound
	Count      int
	SRID       int
}

func (cs *collectionSummary) IsEmpty() bool {
	return cs.Extent.IsEmpty() && cs.Count < 0 && cs.SRID < 0
}

func (cs *collectionSummary) EncodeJSON(w io.Writer) error {
	if cs.IsEmpty() {
		return nil
	}
	wrote := false
	// bbox
	if !cs.Extent.IsEmpty() && !cs.SkipExtent {
		_, err := fmt.Fprintf(w, `"bbox":[%f,%f,%f,%f]`, cs.Extent.Min.X(), cs.Extent.Min.Y(), cs.Extent.Max.X(), cs.Extent.Max.Y())
		if err != nil {
			return err
		}
		wrote = true
	}
	// count
	if cs.Count >= 0 {
		if wrote {
			_, err := w.Write([]byte(","))
			if err != nil {
				return err
			}
		}
		_, err := fmt.Fprintf(w, `"numberMatched":%d`, cs.Count)
		if err != nil {
			return err
		}
		wrote = true
	}

	return nil
}

func (fc *featureCollection) Summary() (collectionSummary, error) {
	var cs collectionSummary

	for _, f := range fc.fm {
		if f.Summary == "" || cs.IsEmpty() {
			// if one of the feature types has no summary, we skip all summary
			return collectionSummary{}, nil
		}
		data := types.ExtractResponseData(f.Summary, fc.res.Data)
		if data == nil {
			return cs, ErrInvalidFeaturePath
		}
		// data should be a db.JsonValue
		v, ok := data.(*db.JsonValue)
		if v == nil || !ok {
			return cs, ErrInvalidFeaturePath
		}
		var raw map[string]any
		err := json.Unmarshal([]byte(*v), &raw)
		if err != nil {
			return cs, errors.Join(ErrInvalidFeaturePath, err)
		}
		if !cs.SkipExtent {
			data = types.ExtractResponseData(f.ExtentPath, raw)
			if data == nil {
				cs.SkipExtent = true
				continue
			}
			g, err := types.ParseGeometryValue(data)
			if err != nil {
				return cs, errors.Join(ErrInvalidSummaryExtentValue, err)
			}
			if cs.Extent.IsEmpty() {
				cs.Extent = g.Bound()
			} else {
				cs.Extent = cs.Extent.Union(g.Bound())
			}
		}
		if f.CountPath == "" {
			cs.Count = -1
			continue
		}
		if cs.Count >= 0 {
			data = types.ExtractResponseData(f.CountPath, raw)
			if data == nil {
				cs.Count = -1
				continue
			}
			c := 0
			switch data := data.(type) {
			case int:
				c = data
			case int64:
				c = int(data)
			case float64:
				c = int(data)
			default:
				return cs, errors.Join(ErrInvalidSummaryExtentValue, fmt.Errorf("invalid count value: %v (%[1]T)", data))
			}
			cs.Count += c
		}
		if f.GeometrySRID != 0 && cs.SRID != 0 && f.GeometrySRID != cs.SRID {
			cs.SRID = -1
			continue
		}
		if f.GeometrySRID != 0 && cs.SRID == 0 {
			cs.SRID = f.GeometrySRID
		}
	}

	return cs, nil
}

func (fc *featureCollection) EncodeFeatures(ctx context.Context, w io.Writer, writeFunc encodeFeatureFunc, sep string) (n int64, err error) {
	if fc.res == nil {
		return 0, types.ErrNoData
	}
	if fc.fm == nil {
		fc.fm = make(map[string]featureDefinition)
	}
	for path, f := range fc.fm {
		data := types.ExtractResponseData(path, fc.res.Data)
		if data == nil {
			return 0, fmt.Errorf("feature path %s not found in response data", path)
		}
		switch data := data.(type) {
		case db.ArrowTable:
			n, err = encodeArrowTable(ctx, w, data, f, writeFunc, sep)
			if err != nil {
				return 0, fmt.Errorf("error encoding arrow table for path %s: %w", path, err)
			}
		default:
			return 0, fmt.Errorf("unsupported data type %T for path %s", data, path)
		}
	}
	return n, nil
}

type encodeFeatureFunc func(w io.Writer, feature *geojson.Feature, fd featureDefinition) error

func encodeArrowTable(ctx context.Context, w io.Writer, data db.ArrowTable, fd featureDefinition, writeFunc encodeFeatureFunc, sep string) (int64, error) {
	if data == nil {
		return 0, nil
	}
	n := int64(0)
	reader, err := data.Reader(false)
	if err != nil {
		return 0, err
	}
	defer reader.Release()
	firstRec := true
	for reader.Next() {
		rec := reader.RecordBatch()
		if reader.Err() != nil {
			return 0, reader.Err()
		}
		if !firstRec {
			_, err = w.Write([]byte(sep))
			if err != nil {
				return 0, fmt.Errorf("error writing separator: %w", err)
			}
		}
		if firstRec {
			firstRec = false
		}

		_, err = encodeRecord(ctx, w, rec, fd, writeFunc, sep)
		if err != nil {
			return 0, fmt.Errorf("error encoding record: %w", err)
		}

		n += int64(rec.NumRows())
	}
	return n, nil
}

func encodeRecord(ctx context.Context, w io.Writer, rec arrow.RecordBatch, fd featureDefinition, writeFunc encodeFeatureFunc, sep string) (int64, error) {
	data := make(map[string]any)
	for i := 0; i < int(rec.NumRows()); i++ {
		for j := 0; j < int(rec.NumCols()); j++ {
			col := rec.Column(j)
			name := rec.ColumnName(j)
			if col == nil {
				data[name] = nil
				continue
			}
			data[name] = col.GetOneForMarshal(i)
		}
		geom, err := transformGeometry(data, fd)
		if err != nil {
			return 0, err
		}
		prop, err := transformProperties(ctx, data, fd, nil)
		if err != nil {
			return 0, err
		}
		// create geojson feature
		feature := geojson.NewFeature(geom)
		feature.Properties = prop
		feature.ID = featureId(data, fd)
		if len(prop) == 0 {
			feature.Properties = make(geojson.Properties)
		}
		feature.Properties["_layer"] = fd.Name
		if fd.WriteBBox {
			feature.BBox = geojson.NewBBox(geom.Bound())
		}
		if i > 0 {
			_, err = w.Write([]byte(sep))
			if err != nil {
				return 0, fmt.Errorf("error writing separator: %w", err)
			}
		}
		err = writeFunc(w, feature, fd)
		if err != nil {
			return 0, fmt.Errorf("error writing feature: %w", err)
		}
	}

	return rec.NumRows(), nil
}

func featureId(data map[string]any, fd featureDefinition) any {
	if fd.IdField == "" {
		return nil
	}
	return types.ExtractResponseData(fd.IdField, data)
}

func transformGeometry(data map[string]any, fd featureDefinition) (geom orb.Geometry, err error) {
	if !strings.Contains(fd.GeometryField, ".") {
		v, ok := data[fd.GeometryField]
		if !ok {
			return nil, fmt.Errorf("geometry field %s not found in data", fd.GeometryField)
		}
		delete(data, fd.GeometryField)
		switch v := v.(type) {
		case []byte:
			return wkb.Unmarshal(v)
		case string:
			return wkb.Unmarshal([]byte(v))
		default:
			return nil, fmt.Errorf("invalid geometry value type %T for field %s", v, fd.GeometryField)
		}
	}
	geomVal := types.ExtractResponseData(fd.GeometryField, data)
	removeByPath(data, fd.GeometryField)
	return types.ParseGeometryValue(geomVal)
}

func transformProperties(ctx context.Context, data map[string]any, fd featureDefinition, vars map[string]any) (map[string]any, error) {
	if fd.transformer == nil {
		// no properties transformation defined
		return data, nil
	}
	res, err := fd.transformer.Transform(ctx, data, vars)
	if err != nil {
		return nil, err
	}
	switch res := res.(type) {
	case map[string]any:
		return res, err
	case nil:
		return nil, nil
	case []any:
		if len(res) == 0 {
			return nil, nil
		}
		if len(res) == 1 {
			m, ok := res[0].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("unexpected type %T in properties transformation", res[0])
			}
			return m, nil
		}
		return nil, fmt.Errorf("unexpected array length %d in properties transformation", len(res))
	default:
		return nil, fmt.Errorf("unexpected type %T in properties transformation", res)
	}
}

func removeByPath(data map[string]any, path string) {
	if path == "" {
		return
	}
	pp := strings.SplitN(path, ".", 2)
	if len(pp) == 1 {
		delete(data, pp[0])
		return
	}
	d, ok := data[pp[0]]
	if !ok || d == nil {
		return
	}
	switch d := d.(type) {
	case map[string]any:
		removeByPath(d, pp[1])
	default:
		return // cannot remove from non-map types
	}
}
