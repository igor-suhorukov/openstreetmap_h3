BEGIN;
TRUNCATE osm_delta_nodes, osm_delta_ways, osm_delta_relations, osm_delta_relation_members, osm_delta_changes;
COPY osm_delta_nodes(${node_columns}) FROM '/input/delta/nodes.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV FREEZE;
COPY osm_delta_ways(${way_columns}) FROM '/input/delta/ways.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV FREEZE;
COPY osm_delta_relations(${relation_columns}) FROM '/input/delta/relations.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV FREEZE;
COPY osm_delta_relation_members(relation_id,member_id,member_type,member_role,sequence_id) FROM '/input/delta/relation_members.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV FREEZE;
COPY osm_delta_changes(object_type,id,operation) FROM '/input/delta/changes.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV FREEZE;
COMMIT;
