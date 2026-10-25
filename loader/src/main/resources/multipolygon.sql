\c osmworld
\timing on

create table multipolygon_32767 PARTITION OF multipolygon (check (h3_3=32767)) DEFAULT ${substitute};
CREATE EXTENSION IF NOT EXISTS h3;

\i /input/static/multipolygon_insert.sql

CREATE INDEX idx_multipolygon_id_h3_3 ON multipolygon using btree(id,h3_3);
CREATE INDEX idx_multipolygon_scale ON multipolygon using btree(scale);
CREATE INDEX idx_multipolygon_polygon ON multipolygon USING gist (polygon);
drop table preimport_multipolygon;