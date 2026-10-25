SELECT a.id FROM osm_affected_relations a
JOIN osm_delta_relations d ON d.id = a.id
WHERE d.tags -> 'type' IN ('multipolygon', 'boundary')
UNION
SELECT a.id FROM osm_affected_relations a
JOIN relations r ON r.id = a.id
WHERE r.tags -> 'type' IN ('multipolygon', 'boundary')
  AND NOT EXISTS (SELECT 1 FROM osm_delta_changes c WHERE c.object_type = 'R' AND c.id = a.id)
ORDER BY 1
