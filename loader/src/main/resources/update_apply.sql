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
UPDATE osm_delta_ways p SET h3_3_multi_regions = h.h3_3_multi_regions
FROM (SELECT id, array_agg(h3_3 ORDER BY h3_3) h3_3_multi_regions
      FROM (SELECT w.id, b.id h3_3 FROM osm_delta_ways w JOIN h3_3_bounds_complex b ON ST_Intersects(w.linestring, b.bounds)
            WHERE w.h3_3 = 32767
            UNION
            SELECT w.id, unnest(w.h3_3_multi_regions) FROM osm_delta_ways w WHERE w.h3_3 = 32767) a
      GROUP BY id) h
WHERE p.id = h.id;
INSERT INTO ways(${way_columns}) SELECT ${way_columns} FROM osm_delta_ways;

DELETE FROM relation_members m USING osm_delta_changes c
WHERE c.object_type = 'R' AND c.operation <> 'C' AND m.relation_id = c.id;
DELETE FROM relations r USING osm_delta_changes c WHERE c.object_type = 'R' AND c.operation <> 'C' AND r.id = c.id;
INSERT INTO relations(${relation_columns}) SELECT ${relation_columns} FROM osm_delta_relations;
INSERT INTO relation_members(relation_id, member_id, sequence_id, member_type, member_role)
SELECT relation_id, member_id, sequence_id, member_type, member_role FROM osm_delta_relation_members;
