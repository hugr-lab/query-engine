-- geom to h3 cast functions
INSTALL h3 FROM community;
LOAD h3;

CREATE OR REPLACE MACRO h3_point_to_cell(geom, res) AS TRY(h3_latlng_to_cell(ST_Y(geom), ST_X(geom), res));

CREATE OR REPLACE MACRO h3_multipoints_to_cells(geom, res) AS (
	CASE
		WHEN ST_GeometryType(geom) = 'POINT' THEN ARRAY[h3_point_to_cell(geom, res)]
		ELSE (
			WITH points AS (
				SELECT unnest(ST_Dump(geom), RECURSIVE=>1)
			)
			SELECT list_distinct(list(h3_point_to_cell(points.geom,res)))
			FROM points
			LIMIT 1
		)
	END
);

CREATE OR REPLACE MACRO h3_linestring_to_cells(geom, res) AS (
	WITH points AS (
		SELECT UNNEST(ST_Dump(ST_Points(geom)), RECURSIVE=>1)
	), pp AS (
		SELECT h3_point_to_cell(points.geom,res) AS p1, lag(p1) OVER(ORDER BY PATH[0]) AS p2
		FROM points
	)
	SELECT list_distinct(flatten(LIST(TRY(h3_grid_path_cells(p1, p2)))))
	FROM pp
	WHERE p2 IS NOT NULL
	LIMIT 1
);

CREATE OR REPLACE MACRO h3_multilinestring_to_cells(geom, res) AS (
	CASE 
		WHEN ST_GeometryType(geom) = 'LINESTRING' THEN h3_linestring_to_cells(geom, res)
		ELSE (
			WITH lines AS (
				SELECT unnest(ST_Dump(geom), RECURSIVE=>1)
			)
			SELECT list_distinct(flatten(list(h3_linestring_to_cells(lines.geom,res))))
			FROM lines
			LIMIT 1
		)
	END
);

CREATE OR REPLACE MACRO h3_polygon_to_cells(geom, res, flags) AS (
	CASE
		WHEN ST_GeometryType(geom) = 'POLYGON' THEN h3_polygon_wkt_to_cells_experimental(ST_AsText(geom), res, flags)
		ELSE (
			WITH polys AS (
				SELECT unnest(ST_Dump(geom), RECURSIVE=>1)
			)
			SELECT list_distinct(flatten(list(h3_polygon_wkt_to_cells_experimental(ST_AsText(polys.geom),res,flags))))
			FROM polys
			LIMIT 1
		)
	END
);

CREATE OR REPLACE MACRO if_geom_is_valid(geom,if_not) AS (
	CASE
		WHEN ST_IsValid(geom) AND NOT ST_IsEmpty(geom) THEN geom
		ELSE if_not
	END
);

CREATE OR REPLACE MACRO hugr_geom_centroid(geom) AS (
    CASE
        WHEN ST_GeometryType(geom) = 'POINT' THEN geom
        WHEN ST_GeometryType(geom) = 'MULTIPOINT' THEN ST_Centroid(geom)
        WHEN ST_GeometryType(geom) = 'LINESTRING' THEN ST_LineInterpolatePoint(geom, 0.5)
        WHEN ST_GeometryType(geom) = 'MULTILINESTRING' THEN ST_Centroid(geom)
        WHEN ST_GeometryType(geom) = 'POLYGON' THEN ST_Centroid(ST_ExteriorRing(geom))
        WHEN ST_GeometryType(geom) = 'MULTIPOLYGON' THEN (
            WITH rings AS (
                SELECT unnest(ST_Dump(geom), RECURSIVE=>1) AS ring
            )
            SELECT ST_Centroid(ST_Union(ST_Centroid(ST_ExteriorRing(ring)))) FROM rings
        )
        ELSE NULL
    END
);

CREATE OR REPLACE MACRO h3_resolution_geom_simplify(geom, res) AS (
	CASE 
		WHEN ST_GeometryType(geom) NOT IN ('MULTIPOLYGON', 'POLYGON', 'LINESTRING', 'MULTILINESTRING') THEN geom
		WHEN res = 0 THEN if_geom_is_valid(ST_Simplify(geom, 0.898), ST_Centroid(geom))
		WHEN res = 1 THEN if_geom_is_valid(ST_Simplify(geom, 0.449), ST_Centroid(geom))
		WHEN res = 2 THEN if_geom_is_valid(ST_Simplify(geom, 0.225), ST_Centroid(geom))
		WHEN res = 3 THEN if_geom_is_valid(ST_Simplify(geom, 0.090), ST_Centroid(geom))
		WHEN res = 4 THEN if_geom_is_valid(ST_Simplify(geom, 0.036), ST_Centroid(geom))
		WHEN res = 5 THEN if_geom_is_valid(ST_Simplify(geom, 0.013), ST_Centroid(geom))
		WHEN res = 6 THEN if_geom_is_valid(ST_Simplify(geom, 0.0054), ST_Centroid(geom))
		WHEN res = 7 THEN if_geom_is_valid(ST_Simplify(geom, 0.0022), ST_Centroid(geom))
		WHEN res = 8 THEN if_geom_is_valid(ST_Simplify(geom, 0.0009), ST_Centroid(geom))
		WHEN res = 9 THEN if_geom_is_valid(ST_Simplify(geom, 0.00027), ST_Centroid(geom))
		ELSE geom
	END
);

CREATE OR REPLACE MACRO h3_geom_exac_to_cells(geom, res) AS (
	CASE
		WHEN ST_GeometryType(geom) = 'POINT' THEN ARRAY[h3_point_to_cell(geom, res)]
		WHEN ST_GeometryType(geom) = 'MULTIPOINT' THEN h3_multipoints_to_cells(geom, res)
		WHEN ST_GeometryType(geom) = 'LINESTRING' THEN h3_linestring_to_cells(geom, res)
		WHEN ST_GeometryType(geom) = 'MULTILINESTRING' THEN h3_multilinestring_to_cells(geom, res)
		WHEN ST_GeometryType(geom) = 'POLYGON' THEN 
			h3_polygon_wkt_to_cells_experimental(ST_AsText(geom),res, 'overlap')
		WHEN ST_GeometryType(geom) = 'MULTIPOLYGON' THEN h3_polygon_to_cells(geom,res, 'overlap')
		WHEN ST_GeometryType(geom) = 'GEOMETRYCOLLECTION' THEN NULL
	END
);

CREATE OR REPLACE MACRO h3_geom_to_cells(geom, res, simplify) AS (
	CASE
		WHEN ST_GeometryType(geom) = 'GEOMETRYCOLLECTION' THEN (
			WITH items AS (
				SELECT unnest(ST_Dump(geom), RECURSIVE=>1)
			)
			SELECT list_distinct(
				flatten(
					list(
						h3_geom_exac_to_cells(
                            CASE
                                WHEN simplify THEN h3_resolution_geom_simplify(items.geom, res)
                                ELSE items.geom
                            END,
							res
						)
					)
				)
			)
			FROM items
			LIMIT 1
		)
		ELSE h3_geom_exac_to_cells(
            CASE
                WHEN simplify THEN h3_resolution_geom_simplify(geom, res)
                ELSE geom
            END, 
            res
        )
	END
);