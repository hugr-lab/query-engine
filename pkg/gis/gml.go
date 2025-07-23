package gis

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/paulmach/orb"
)

var errPropertyIsNotDefined = errors.New("property is not defined in feature definition")

func encodeGmlPropertyValue(name string, value any, def *openapi3.Schema) (string, error) {
	if def == nil {
		return fmt.Sprint(value), nil
	}
	if def.Properties == nil {
		return fmt.Sprint(value), nil
	}
	prop, ok := def.Properties[name]
	if !ok || prop.Value == nil {
		return "", errPropertyIsNotDefined
	}
	switch {
	case prop.Value.Type.Is(openapi3.TypeString):
		switch prop.Value.Format {
		case "date-time":
			// encode as ISO 8601 date-time
			if t, ok := value.(time.Time); ok {
				return t.Format(time.RFC3339), nil
			}
		case "date":
			// encode as ISO 8601 date
			if t, ok := value.(time.Time); ok {
				return t.Format("2006-01-02"), nil
			}
		}
		return fmt.Sprint(value), nil
	case prop.Value.Type.Is(openapi3.TypeInteger):
		return fmt.Sprintf("%d", value), nil
	case prop.Value.Type.Is(openapi3.TypeNumber):
		return fmt.Sprintf("%f", value), nil
	case prop.Value.Type.Is(openapi3.TypeBoolean):
		if v, ok := value.(bool); ok {
			if v {
				return "true", nil
			}
			return "false", nil
		}
		return fmt.Sprint(value), nil
	case prop.Value.Type.Is(openapi3.TypeArray) || prop.Value.Type.Is(openapi3.TypeObject):
		b, err := json.Marshal(value) // ensure value is JSON-serializable
		if err != nil {
			return "", fmt.Errorf("error encoding array property %s: %w", name, err)
		}
		return string(b), nil
	default:
		return "", fmt.Errorf("property %s type %s is not supported, cannot encode as GML", name, prop.Value.Type)
	}
}

func encodeGmlGeometry(e *xml.Encoder, geom orb.Geometry, srid int) error {
	switch g := geom.(type) {
	case orb.Point:
		return marshalPointGML(g, e)
	case orb.LineString:
		return marshalLineStringGML(g, e)
	case orb.Polygon:
		return marshalPolygonGML(g, e)
	case orb.MultiPoint:
		return marshalMultiPointGML(g, e)
	case orb.MultiLineString:
		return marshalMultiLineStringGML(g, e)
	case orb.MultiPolygon:
		return marshalMultiPolygonGML(g, e)
	case orb.Collection:
		return marshalCollectionGML(g, e)
	default:
		return fmt.Errorf("unsupported geometry type %T", g)
	}

}

// marshalPointGML marshal point to GML.
func marshalPointGML(point orb.Point, e *xml.Encoder) error {
	pointElement := xml.StartElement{Name: xml.Name{Local: "gml:Point"}}
	if err := e.EncodeToken(pointElement); err != nil {
		return err
	}
	// Encode the point coordinates in GML format
	if err := marshalPosGML(point, e); err != nil {
		return err
	}

	if err := e.EncodeToken(pointElement.End()); err != nil {
		return err
	}

	return nil
}

// marshalPosGML marshal position to GML.
func marshalPosGML(point orb.Point, e *xml.Encoder) error {
	// Encode the point coordinates in GML format
	coords := fmt.Sprintf("%f %f", point[0], point[1])
	posElement := xml.StartElement{Name: xml.Name{Local: "gml:pos"}}
	if err := e.EncodeElement(coords, posElement); err != nil {
		return err
	}
	return nil
}

// marshalLineStringGML marshal line to GML.
func marshalLineStringGML(line orb.LineString, e *xml.Encoder) error {
	lineStartElement := xml.StartElement{Name: xml.Name{Local: "gml:LineString"}}
	if err := e.EncodeToken(lineStartElement); err != nil {
		return err
	}

	// Encode each point of the line
	for _, p := range line {
		if err := marshalPosGML(p, e); err != nil {
			return err
		}
	}

	if err := e.EncodeToken(lineStartElement.End()); err != nil {
		return err
	}

	return nil
}

// marshalRingGML marshal ring to GML.
func marshalRingGML(boundaryType string, ring orb.Ring, e *xml.Encoder) error {
	boundaryStartElement := xml.StartElement{Name: xml.Name{Local: fmt.Sprintf("gml:%s", boundaryType)}}
	if err := e.EncodeToken(boundaryStartElement); err != nil {
		return err
	}
	linearRingStartElement := xml.StartElement{Name: xml.Name{Local: "gml:LinearRing"}}
	if err := e.EncodeToken(linearRingStartElement); err != nil {
		return err
	}

	posList := xml.StartElement{Name: xml.Name{Local: "gml:posList"}}
	var pp []string
	for _, p := range ring {
		pp = append(pp, fmt.Sprintf("%f %f", p[0], p[1]))
	}
	if err := e.EncodeElement(strings.Join(pp, " "), posList); err != nil {
		return err
	}

	if err := e.EncodeToken(linearRingStartElement.End()); err != nil {
		return err
	}
	if err := e.EncodeToken(boundaryStartElement.End()); err != nil {
		return err
	}

	return nil
}

// marshalPolygonGML marshal polygon to GML.
func marshalPolygonGML(polygon orb.Polygon, e *xml.Encoder) error {
	polygonStartElement := xml.StartElement{Name: xml.Name{Local: "gml:Polygon"}}
	if err := e.EncodeToken(polygonStartElement); err != nil {
		return err
	}
	if len(polygon) != 0 {
		// Encode the exterior ring (outer boundary)
		if err := marshalRingGML("exterior", polygon[0], e); err != nil {
			return err
		}

		// Encode the interior rings (inner boundaries), if any
		for _, innerRing := range polygon[1:] {
			if err := marshalRingGML("interior", innerRing, e); err != nil {
				return err
			}
		}
	}

	if err := e.EncodeToken(polygonStartElement.End()); err != nil {
		return err
	}

	return nil
}

// marshalMultiPointGML marshal multi-point to GML.
func marshalMultiPointGML(multiPoint orb.MultiPoint, e *xml.Encoder) error {
	multiPointStartElement := xml.StartElement{Name: xml.Name{Local: "gml:MultiPoint"}}
	if err := e.EncodeToken(multiPointStartElement); err != nil {
		return err
	}

	// Encode each point of the multi-point
	for _, p := range multiPoint {
		pointMemberStartElement := xml.StartElement{Name: xml.Name{Local: "gml:pointMember"}}
		if err := e.EncodeToken(pointMemberStartElement); err != nil {
			return err
		}
		if err := marshalPointGML(p, e); err != nil {
			return err
		}
		if err := e.EncodeToken(pointMemberStartElement.End()); err != nil {
			return err
		}
	}

	if err := e.EncodeToken(multiPointStartElement.End()); err != nil {
		return err
	}

	return nil
}

// marshalMultiLineStringGML marshal multi-line to GML.
func marshalMultiLineStringGML(multiLine orb.MultiLineString, e *xml.Encoder) error {
	multiLineStartElement := xml.StartElement{Name: xml.Name{Local: "gml:MultiCurve"}}
	if err := e.EncodeToken(multiLineStartElement); err != nil {
		return err
	}

	// Encode each line of the multi-line
	for _, line := range multiLine {
		curveMemberStartElement := xml.StartElement{Name: xml.Name{Local: "gml:curveMember"}}
		if err := e.EncodeToken(curveMemberStartElement); err != nil {
			return err
		}

		if err := marshalLineStringGML(line, e); err != nil {
			return err
		}

		if err := e.EncodeToken(curveMemberStartElement.End()); err != nil {
			return err
		}
	}

	if err := e.EncodeToken(multiLineStartElement.End()); err != nil {
		return err
	}

	return nil
}

// marshalMultiPolygonGML marshal multi-polygon to GML.
func marshalMultiPolygonGML(multiPolygon orb.MultiPolygon, e *xml.Encoder) error {
	multiPolygonStartElement := xml.StartElement{Name: xml.Name{Local: "gml:MultiSurface"}}
	if err := e.EncodeToken(multiPolygonStartElement); err != nil {
		return err
	}

	// Encode each polygon of the multi-polygon
	// Each polygon is wrapped in a <gml:surfaceMember> element
	for _, polygon := range multiPolygon {
		surfaceMemberStartElement := xml.StartElement{Name: xml.Name{Local: "gml:surfaceMember"}}
		if err := e.EncodeToken(surfaceMemberStartElement); err != nil {
			return err
		}
		if err := marshalPolygonGML(polygon, e); err != nil {
			return err
		}
		if err := e.EncodeToken(surfaceMemberStartElement.End()); err != nil {
			return err
		}
	}
	if err := e.EncodeToken(multiPolygonStartElement.End()); err != nil {
		return err
	}
	return nil
}

// marshalCollectionGML marshal collection to GML.
func marshalCollectionGML(collection orb.Collection, e *xml.Encoder) error {
	multiGeometryStartElement := xml.StartElement{Name: xml.Name{Local: "gml:MultiGeometry"}}
	if err := e.EncodeToken(multiGeometryStartElement); err != nil {
		return err
	}

	for _, geom := range collection {
		var err error
		switch g := geom.(type) {
		case orb.Point:
			err = marshalPointGML(g, e)
		case orb.LineString:
			err = marshalLineStringGML(g, e)
		case orb.Polygon:
			err = marshalPolygonGML(g, e)
		case orb.MultiPoint:
			err = marshalMultiPointGML(g, e)
		case orb.MultiLineString:
			err = marshalMultiLineStringGML(g, e)
		case orb.MultiPolygon:
			err = marshalMultiPolygonGML(g, e)
		}
		if err != nil {
			return err
		}
	}

	if err := e.EncodeToken(multiGeometryStartElement.End()); err != nil {
		return err
	}

	return nil
}
