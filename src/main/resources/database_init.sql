ALTER SYSTEM set max_parallel_workers_per_gather =6;
ALTER SYSTEM set max_parallel_workers =6;
ALTER SYSTEM set max_parallel_maintenance_workers=6;
SET work_mem = '128 MB';
SET maintenance_work_mem = '420 MB';
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET effective_io_concurrency = 200;
ALTER SYSTEM SET enable_partitionwise_join = ON;
ALTER SYSTEM SET enable_partitionwise_aggregate = ON;
ALTER SYSTEM SET parallel_leader_participation = on;
SELECT pg_reload_conf();

\timing on

create database osmworld encoding 'UTF-8';
\c osmworld

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS hstore;
${substitute}
-- https://github.com/citusdata/citus/blob/main/src/backend/columnar/README.md
BEGIN;

CREATE TABLE nodes (
    h3_3 smallint NOT NULL,
    h3_8 integer NOT NULL,
    tags hstore,
    geom  geometry(Point,4326) NOT NULL,
    id bigint NOT NULL
) PARTITION BY RANGE (h3_3);

CREATE TABLE ways (
    closed boolean NOT NULL,
    building boolean NOT NULL,
    highway boolean NOT NULL,
    h3_3 smallint,
    h3_8 integer NOT NULL,
    scale real NOT NULL,
    tags hstore,
    bbox geometry(POLYGON,4326) NOT NULL,
    centre  geometry(Point,4326) NOT NULL,
    id bigint NOT NULL,
    linestring geometry(LINESTRING,4326) NOT NULL,
    points bigint[],
    h3_8_regions integer[]
) PARTITION BY RANGE (h3_3);
--Alignment Padding: reordered by dba utility advise - closed,h3_3,scale,tags,bbox,centre,p.id,linestring,points

CREATE TABLE relations (
    id bigint NOT NULL,
    tags hstore
);

CREATE TABLE relation_members (
    relation_id bigint NOT NULL,
    member_id bigint NOT NULL,
    sequence_id int NOT NULL,
    member_type character(1) NOT NULL,
    member_role text
);
--todo ${substitute_columnar}

CREATE TABLE multipolygon(
    h3_3 smallint NOT NULL,
    h3_8 integer NOT NULL,
    scale real NOT NULL,
    tags hstore,
    bbox geometry(POLYGON,4326) NOT NULL,
    id bigint NOT NULL,
    centre geometry(Point,4326) NOT NULL,
    polygon geometry(MULTIPOLYGON,4326) NOT NULL,
    h3_3_multi_regions smallint[]
) PARTITION BY RANGE (h3_3);
\i /input/static/multipolygon_tables.sql
CREATE UNLOGGED TABLE preimport_multipolygon(polygon geometry(MULTIPOLYGON,4326), osm_type text, id bigint, tags hstore);


create table h3_3_bounds_complex(id smallint, h3_0 smallint, h3_1 smallint, h3_2 smallint, bbox geometry(Geometry,4326), bounds geometry(Geometry,4326));
copy h3_3_bounds_complex(id,h3_0,h3_1,h3_2,bbox,bounds) from PROGRAM $$ zcat /input/static/h3_poly.tsv.gz$$ DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV;
CREATE INDEX h3_3_bounds_bbox_complex_idx ON h3_3_bounds_complex USING gist (bbox);
CREATE INDEX h3_3_bounds_bbox_complex_bounds_idx ON h3_3_bounds_complex USING gist (bounds);

CREATE FUNCTION to_short_h3_8(bigint) RETURNS integer
    AS $$ select ($1 & 'x000ffffffff00000'::bit(64)::bigint>>20)::bit(32)::int;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION to_full_short_h3_8(integer) RETURNS bigint
    AS $$ select (($1::bigint::bit(64) & 'x00000000ffffffff'::bit(64)) <<20 | 'x08800000000fffff'::bit(64))::bigint;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION to_full_from_short_h3_3(smallint) RETURNS bigint
    AS $$ select (($1::bigint::bit(64) & 'x000000000000ffff'::bit(64))<<36 | 'x0830000fffffffff'::bit(64))::bigint;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION to_short_h3_3(bigint) RETURNS smallint
    AS $$ select ($1 & 'x000ffff000000000'::bit(64)::bigint>>36)::smallint;$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;



CREATE FUNCTION to_ha_arrays_text(text) RETURNS text
    AS $$ select replace($1,',','|');$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE VIEW geometry_global_view AS
  (select id,h3_3,h3_8,'N' as type,geom as centre,geom as geom, tags from nodes)
 union all
  (select id,h3_3,h3_8, 'W' as type,centre,linestring as geom,tags from ways)
 union all
  (select id,h3_3,h3_8,'M' as type, centre,polygon as geom,tags from multipolygon);

COMMIT;