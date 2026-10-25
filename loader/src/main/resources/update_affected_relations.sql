CREATE UNLOGGED TABLE osm_affected_relations AS
SELECT id FROM osm_delta_changes WHERE object_type = 'R'
UNION
SELECT m.relation_id FROM relation_members m
JOIN osm_delta_changes c ON c.object_type = 'W' AND c.id = m.member_id
WHERE m.member_type = 'W'
UNION
SELECT m.relation_id FROM relation_members m
JOIN relations r ON r.id = m.relation_id
WHERE m.member_type = 'W' AND r.tags -> 'type' IN ('multipolygon', 'boundary')
  AND NOT EXISTS (SELECT 1 FROM ways w WHERE w.id = m.member_id);
ANALYZE osm_affected_relations;
