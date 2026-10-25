BEGIN;
${osm_import_state}
ANALYZE osm_delta_nodes;
ANALYZE osm_delta_ways;
ANALYZE osm_delta_relations;
ANALYZE osm_delta_relation_members;
ANALYZE osm_delta_changes;

INSERT INTO osm_change_log(import_id, object_type, id, operation)
SELECT s.id, c.object_type, c.id, c.operation
FROM osm_delta_changes c CROSS JOIN (SELECT max(id) id FROM osm_import_state) s;

DELETE FROM nodes n USING osm_delta_changes c WHERE c.object_type = 'N' AND c.operation <> 'C' AND n.id = c.id;
INSERT INTO nodes(${node_columns}) SELECT ${node_columns} FROM osm_delta_nodes;

DELETE FROM ways w USING osm_delta_changes c WHERE c.object_type = 'W' AND c.operation <> 'C' AND w.id = c.id;
INSERT INTO ways(${way_columns}) SELECT ${way_columns} FROM osm_delta_ways WHERE h3_3 <> 32767;
CREATE TEMP TABLE osm_delta_ways_h3_3 ON COMMIT DROP AS
SELECT id, array_agg(h3_3 ORDER BY h3_3) h3_3_multi_regions
FROM (SELECT DISTINCT w.id, b.id h3_3 FROM osm_delta_ways w JOIN h3_3_bounds_complex b ON ST_Intersects(w.linestring, b.bounds)
      WHERE w.h3_3 = 32767) a
GROUP BY id;
INSERT INTO ways(${way_columns},h3_3_multi_regions)
SELECT ${way_columns}, CASE WHEN cardinality(h.h3_3_multi_regions) > 1 THEN h.h3_3_multi_regions
    ELSE ARRAY(SELECT DISTINCT unnest((tags -> 'is.h33idxs')::smallint[]) ORDER BY 1)::smallint[] END
FROM osm_delta_ways p JOIN osm_delta_ways_h3_3 h USING (id)
WHERE p.h3_3 = 32767;

DELETE FROM relation_members m USING osm_delta_changes c
WHERE c.object_type = 'R' AND c.operation <> 'C' AND m.relation_id = c.id;
DELETE FROM relations r USING osm_delta_changes c WHERE c.object_type = 'R' AND c.operation <> 'C' AND r.id = c.id;
INSERT INTO relations(${relation_columns}) SELECT ${relation_columns} FROM osm_delta_relations;
INSERT INTO relation_members(relation_id, member_id, sequence_id, member_type, member_role)
SELECT relation_id, member_id, sequence_id, member_type, member_role FROM osm_delta_relation_members;
