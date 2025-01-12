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

CREATE TYPE table_reference AS ENUM ('nodes','ways','multipolygon');

CREATE OR REPLACE VIEW geometry_global_view AS
  (select id,h3_3,h3_8,0::real scale,'nodes'::table_reference as type,geom as centre,geom as geom, tags from nodes)
 union all
  (select id,h3_3,h3_8, scale, 'ways'::table_reference as type,centre,linestring as geom,tags from ways)
 union all
  (select id,h3_3,h3_8, scale, 'multipolygon'::table_reference as type, centre,polygon as geom,tags from multipolygon);

COMMENT ON TABLE nodes IS 'Table storing OpenStreetMap node data with hierarchical spatial indexing and associated tags';
COMMENT ON COLUMN nodes.h3_3 IS 'H3 index resolution for spatial partitioning, level 3 encoded as smallint. To decode it to H3 bigint value use function: to_full_from_short_h3_3()';
COMMENT ON COLUMN nodes.h3_8 IS 'H3 index resolution for spatial partitioning, level 8 encoded as integer. To decode it to H3 bigint value use function: to_full_short_h3_8()';
COMMENT ON COLUMN nodes.tags IS 'Hstore column storing key-value pairs for node attributes';
COMMENT ON COLUMN nodes.geom IS 'Geometry column storing the geographic location of the node as a Point in WGS 84 coordinate system (SRID 4326)';
COMMENT ON COLUMN nodes.id IS 'Unique identifier for each node, corresponding to the OpenStreetMap node ID';
COMMENT ON TABLE ways IS 'Table storing OpenStreetMap way data with spatial attributes and associated tags';
COMMENT ON COLUMN ways.closed IS 'Boolean indicating if the way forms a closed line where start and end nodes has the same coordinates';
COMMENT ON COLUMN ways.building IS 'Boolean indicating if the way represents a building';
COMMENT ON COLUMN ways.highway IS 'Boolean indicating if the way represents some kind of OSM highway key';
COMMENT ON COLUMN ways.h3_3 IS 'Optional H3 index resolution for spatial partitioning, level 3 encoded as smallint. To decode it to H3 bigint value use function: to_full_from_short_h3_3()';
COMMENT ON COLUMN ways.h3_8 IS 'H3 index resolution for spatial partitioning, level 8 encoded as integer. To decode it to H3 bigint value use function: to_full_short_h3_8()';
COMMENT ON COLUMN ways.scale IS 'Scale factor for the way, representing its relative size';
COMMENT ON COLUMN ways.tags IS 'Hstore column storing key-value pairs for way attributes';
COMMENT ON COLUMN ways.bbox IS 'Geometry column storing the bounding box of the way as a Polygon in WGS 84 coordinate system (SRID 4326)';
COMMENT ON COLUMN ways.centre IS 'Geometry column storing the geographic center of the way as a Point in WGS 84 coordinate system (SRID 4326)';
COMMENT ON COLUMN ways.id IS 'Unique identifier for each way corresponding to the OpenStreetMap way ID';
COMMENT ON COLUMN ways.linestring IS 'Geometry column storing the path of the way as a LineString in WGS 84 coordinate system (SRID 4326)';
COMMENT ON COLUMN ways.points IS 'Array of node IDs that make up the way';
COMMENT ON COLUMN ways.h3_8_regions IS 'Array of H3 index regions at resolution level 8 that the way intersects';
COMMENT ON TABLE relations IS 'Table storing OpenStreetMap relation data with associated tags';
COMMENT ON COLUMN relations.id IS 'Unique identifier for each relation, typically corresponding to the OpenStreetMap relation ID';
COMMENT ON COLUMN relations.tags IS 'Hstore column storing key-value pairs for relation attributes';
COMMENT ON TABLE relation_members IS 'Table storing members of OpenStreetMap relations, including their order and role';
COMMENT ON COLUMN relation_members.relation_id IS 'Identifier for the relation to which the member belongs';
COMMENT ON COLUMN relation_members.member_id IS 'Identifier for the member, which can be a node, way, or another relation';
COMMENT ON COLUMN relation_members.sequence_id IS 'Order of the member within the relation';
COMMENT ON COLUMN relation_members.member_type IS 'Type of the member, represented as a single character (e.g., N for node, W for way, R for relation)';
COMMENT ON COLUMN relation_members.member_role IS 'Role of the member within the relation, providing context for its inclusion';
COMMENT ON TABLE multipolygon IS 'Table storing OpenStreetMap relations of multipolygon data with spatial attributes and associated tags';
COMMENT ON COLUMN multipolygon.h3_3 IS 'H3 index resolution for spatial partitioning, level 3';
COMMENT ON COLUMN multipolygon.h3_8 IS 'H3 index resolution for spatial partitioning, level 8';
COMMENT ON COLUMN multipolygon.scale IS 'Scale factor for the multipolygon, representing its relative size';
COMMENT ON COLUMN multipolygon.tags IS 'Hstore column storing key-value pairs for multipolygon attributes';
COMMENT ON COLUMN multipolygon.bbox IS 'Geometry column storing the bounding box of the multipolygon as a Polygon in WGS 84 coordinate system (SRID 4326)';
COMMENT ON COLUMN multipolygon.id IS 'Unique identifier for each multipolygon corresponding to the OpenStreetMap relation ID';
COMMENT ON COLUMN multipolygon.centre IS 'Geometry column storing the geographic center of the multipolygon as a Point in WGS 84 coordinate system (SRID 4326)';
COMMENT ON COLUMN multipolygon.polygon IS 'Geometry column storing the multipolygon geometry as a MultiPolygon in WGS 84 coordinate system (SRID 4326)';
COMMENT ON COLUMN multipolygon.h3_3_multi_regions IS 'Array of H3 index regions at resolution level 3 that the multipolygon intersects in smallint representation';
COMMENT ON VIEW geometry_global_view IS 'View combining geometry data from nodes, ways, and multipolygons with a unified schema';
COMMENT ON COLUMN geometry_global_view.id IS 'Unique identifier for the geometry, corresponding to the original table (nodes, ways, or multipolygon)';
COMMENT ON COLUMN geometry_global_view.h3_3 IS 'H3 index resolution for spatial partitioning, level 3, from the original table encoded as smallint. To decode it to H3 bigint value use function: to_full_from_short_h3_3()';
COMMENT ON COLUMN geometry_global_view.h3_8 IS 'H3 index resolution for spatial partitioning, level 8, from the original table encoded as integer. To decode it to H3 bigint value use function: to_full_short_h3_8()';
COMMENT ON COLUMN geometry_global_view.scale IS 'Scale factor for the geometry, representing its relative size; set to 0 for nodes';
COMMENT ON COLUMN geometry_global_view.type IS 'Type of the original table from which the geometry is sourced (nodes, ways, or multipolygon)';
COMMENT ON COLUMN geometry_global_view.centre IS 'Geographic center of the geometry, represented as a Point';
COMMENT ON COLUMN geometry_global_view.geom IS 'Geometry column storing the spatial representation (Point, LineString, or MultiPolygon)';
COMMENT ON COLUMN geometry_global_view.tags IS 'Hstore column storing key-value pairs for attributes associated with the geometry';

COMMIT;