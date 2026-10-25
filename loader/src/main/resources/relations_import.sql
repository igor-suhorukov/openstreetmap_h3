SET application_name = 'psql sql/relations_import.sql';
\timing on

BEGIN;
TRUNCATE relations, relation_members;
COPY relations(id,tags) FROM '/input/relations/00000.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV FREEZE;
COPY relation_members(relation_id,member_id,member_type,member_role,sequence_id) FROM '/input/relations/00001.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV FREEZE;
ALTER TABLE ONLY relation_members ADD CONSTRAINT pk_relation_members PRIMARY KEY (relation_id, sequence_id);
COMMIT;
ANALYZE relations;
ANALYZE relation_members;
