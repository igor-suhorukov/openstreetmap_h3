DROP TABLE IF EXISTS osm_delta_nodes, osm_delta_ways, osm_delta_relations, osm_delta_relation_members, osm_delta_changes, osm_affected_relations, preimport_multipolygon;
CREATE UNLOGGED TABLE osm_delta_nodes (LIKE nodes);
CREATE UNLOGGED TABLE osm_delta_ways (LIKE ways);
CREATE UNLOGGED TABLE osm_delta_relations (LIKE relations);
CREATE UNLOGGED TABLE osm_delta_relation_members (LIKE relation_members);
CREATE UNLOGGED TABLE osm_delta_changes (object_type character(1) NOT NULL, id bigint NOT NULL, operation character(1) NOT NULL);
CREATE UNLOGGED TABLE preimport_multipolygon (polygon geometry(MULTIPOLYGON,4326), osm_type text, id bigint, tags hstore);
