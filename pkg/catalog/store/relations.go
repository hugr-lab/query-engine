package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// Relation readers. Rows are the PHYSICAL fk legs written by collect (an m2m
// edge is the junction's two legs); the logical projections — FORWARD / BACK /
// M2M — are synthesized in (*Store).Relations (logical.go), matching the
// compiled-provider adapter and the live etalon:
//   - junction object: its legs read as plain FK FORWARD;
//   - endpoint object: a leg from an is_m2m junction becomes M2M FORWARD to the
//     co-endpoint (through = junction, keys reversed into endpoint→junction
//     orientation) and NO FK BACK is shown for it;
//   - any other leg pointing at the object reads as FK BACK (keys stay in the
//     canonical source→destination orientation).

// relationEdge is a relation row read back, with the junction flag of its
// SOURCE object (drives the m2m synthesis on the destination side).
type relationEdge struct {
	relation
	m2mJunction bool
}

// activeMeta is the data-source activity gate used by every read-side query:
// rows of unloaded / disabled / suspended sources are invisible (they stay
// stored — flags flip them back).
func activeMeta(alias, column string) string {
	return ` JOIN core.catalog.data_source_meta ` + alias +
		` ON ` + alias + `.data_source = ` + column +
		` AND ` + alias + `.loaded AND NOT ` + alias + `.disabled AND NOT ` + alias + `.suspended`
}

const relationColumns = `r.source, r.name, r.kind, r.destination, r.m2m_object,
	r.source_keys::JSON::VARCHAR, r.destination_keys::JSON::VARCHAR,
	r.source_field, r.source_field_description, r.destination_field, r.destination_field_description,
	r.field_declared, r.data_source`

// relationsBySource reads the relations DECLARED BY an object (its fk legs),
// memoized per session.
func (g *genContext) relationsBySource(ctx context.Context, source string) ([]*relation, error) {
	if out, ok := g.relSrc[source]; ok {
		return out, nil
	}
	edges, err := g.readRelations(ctx, `SELECT `+relationColumns+`, false
		FROM core.catalog.relations r`+activeMeta("m", "r.data_source")+`
		WHERE r.source = `+lit(source)+` ORDER BY r.name`)
	if err != nil {
		return nil, fmt.Errorf("read relations of %s: %w", source, err)
	}
	out := make([]*relation, 0, len(edges))
	for _, e := range edges {
		out = append(out, &e.relation)
	}
	if g.relSrc == nil {
		g.relSrc = map[string][]*relation{}
	}
	g.relSrc[source] = out
	return out, nil
}

// relationsByDestination reads the relations POINTING AT an object, flagging
// legs whose source is an is_m2m junction; memoized per session.
func (g *genContext) relationsByDestination(ctx context.Context, destination string) ([]*relationEdge, error) {
	if out, ok := g.relDst[destination]; ok {
		return out, nil
	}
	out, err := g.readRelations(ctx, `SELECT `+relationColumns+`,
		COALESCE(json_extract_string(j.properties::JSON, '$.is_m2m') = 'true', false)
		FROM core.catalog.relations r`+activeMeta("m", "r.data_source")+`
		LEFT JOIN core.catalog.data_objects j ON j.name = r.source
		WHERE r.destination = `+lit(destination)+` ORDER BY r.name`)
	if err != nil {
		return nil, fmt.Errorf("read relations at %s: %w", destination, err)
	}
	if g.relDst == nil {
		g.relDst = map[string][]*relationEdge{}
	}
	g.relDst[destination] = out
	return out, nil
}

// readRelations runs one relation query.
func (g *genContext) readRelations(ctx context.Context, query string) ([]*relationEdge, error) {
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*relationEdge
	for rows.Next() {
		var e relationEdge
		var m2mObject, srcKeys, dstKeys, srcField, srcDesc, dstField, dstDesc sql.NullString
		if err := rows.Scan(&e.Source, &e.Name, &e.Kind, &e.Destination, &m2mObject,
			&srcKeys, &dstKeys, &srcField, &srcDesc, &dstField, &dstDesc,
			&e.FieldDeclared, &e.DataSource, &e.m2mJunction); err != nil {
			return nil, err
		}
		e.M2MObject = m2mObject.String
		e.SourceField = srcField.String
		e.SourceFieldDescription = srcDesc.String
		e.DestinationField = dstField.String
		e.DestinationFieldDescription = dstDesc.String
		if srcKeys.Valid {
			_ = json.Unmarshal([]byte(srcKeys.String), &e.SourceKeys)
		}
		if dstKeys.Valid {
			_ = json.Unmarshal([]byte(dstKeys.String), &e.DestinationKeys)
		}
		g.touch(e.DataSource)
		out = append(out, &e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
