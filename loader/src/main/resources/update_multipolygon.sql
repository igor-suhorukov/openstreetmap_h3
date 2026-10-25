ANALYZE preimport_multipolygon;
DELETE FROM preimport_multipolygon p WHERE NOT EXISTS (SELECT 1 FROM osm_affected_relations a WHERE a.id = p.id);
CREATE TEMP TABLE osm_delta_multipolygon ON COMMIT DROP AS
SELECT coalesce(p.id, m.id) id, CASE WHEN m.id IS NULL THEN 'C' WHEN p.id IS NULL THEN 'D' ELSE 'U' END operation
FROM osm_affected_relations a
LEFT JOIN preimport_multipolygon p ON p.id = a.id
LEFT JOIN multipolygon m ON m.id = a.id
WHERE (p.id IS NOT NULL OR m.id IS NOT NULL)
  AND (p.id IS NULL OR m.id IS NULL OR m.polygon::text <> p.polygon::text
       OR EXISTS (SELECT 1 FROM osm_delta_changes c WHERE c.object_type = 'R' AND c.id = a.id));
INSERT INTO osm_change_log(import_id, object_type, id, operation)
SELECT s.id, 'M', d.id, d.operation
FROM osm_delta_multipolygon d CROSS JOIN (SELECT max(id) id FROM osm_import_state) s;
DELETE FROM multipolygon m USING osm_delta_multipolygon d WHERE d.operation <> 'C' AND m.id = d.id;
DELETE FROM preimport_multipolygon p
WHERE NOT EXISTS (SELECT 1 FROM osm_delta_multipolygon d WHERE d.id = p.id AND d.operation <> 'D');
\i /input/static/multipolygon_insert.sql
DROP TABLE osm_delta_nodes, osm_delta_ways, osm_delta_relations, osm_delta_relation_members, osm_delta_changes, osm_affected_relations, preimport_multipolygon;
COMMIT;
