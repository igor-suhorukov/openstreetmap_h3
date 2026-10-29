SET application_name = 'psql sql_finalize/admin_units_import.sql';
\timing on

BEGIN;
CREATE SCHEMA admin;

CREATE TABLE admin.unit (
    id bigint PRIMARY KEY,
    admin_level smallint NOT NULL,
    parent_id bigint REFERENCES admin.unit DEFERRABLE INITIALLY DEFERRED,
    parent_source char(1),
    parent_share real,
    path bigint[] NOT NULL,
    depth smallint NOT NULL,
    children integer NOT NULL,
    name text,
    names hstore,
    tags hstore NOT NULL,
    h3_3 smallint[] NOT NULL,
    centre geometry(Point, 4326),
    geom geometry(MultiPolygon, 4326),
    area_m2 double precision,
    own_area_m2 double precision,
    import_id integer
);
COPY admin.unit(id,admin_level,parent_id,parent_source,parent_share,path,depth,children,name,names,tags,h3_3,geom,area_m2,own_area_m2) FROM '/input/admin_units/unit.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV;

CREATE TABLE admin.unit_region (
    unit_id bigint NOT NULL REFERENCES admin.unit DEFERRABLE INITIALLY DEFERRED,
    part_no integer NOT NULL,
    admin_level smallint NOT NULL,
    geom geometry(Polygon, 4326) NOT NULL,
    PRIMARY KEY (unit_id, part_no)
);
COPY admin.unit_region(unit_id,part_no,admin_level,geom) FROM '/input/admin_units/unit_region.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV;

CREATE TABLE admin.state (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation text NOT NULL,
    osm_import_id integer,
    parameters jsonb NOT NULL,
    units integer NOT NULL,
    object_rows bigint NOT NULL,
    finished_at timestamptz NOT NULL DEFAULT now()
);
COPY admin.state(operation,osm_import_id,parameters,units,object_rows) FROM '/input/admin_units/state.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV;

SET CONSTRAINTS ALL IMMEDIATE;
CREATE INDEX unit_parent_idx ON admin.unit (parent_id);
CREATE INDEX unit_path_idx ON admin.unit USING gin (path);
CREATE INDEX unit_geom_idx ON admin.unit USING gist (geom);
CREATE INDEX unit_region_geom_idx ON admin.unit_region USING gist (geom);

COMMENT ON SCHEMA admin IS 'Directory of administrative units built by the loader with -admin_units';
COMMENT ON TABLE admin.unit IS 'Administrative units: boundary=administrative relations with a numeric admin_level, with their hierarchy and names';
COMMENT ON COLUMN admin.unit.id IS 'OpenStreetMap relation id of the unit';
COMMENT ON COLUMN admin.unit.admin_level IS 'admin_level tag of the relation';
COMMENT ON COLUMN admin.unit.parent_id IS 'Parent unit: the deepest lower level unit holding at least -parent_share of the unit area, or the subarea link of the parent where geometry is not enough';
COMMENT ON COLUMN admin.unit.parent_source IS 'G - parent chosen by geometry, S - by the subarea member of the parent relation';
COMMENT ON COLUMN admin.unit.parent_share IS 'Share of the unit area inside the parent; NULL if the unit or the parent has no polygon';
COMMENT ON COLUMN admin.unit.path IS 'Unit ids from the root to the unit itself';
COMMENT ON COLUMN admin.unit.depth IS 'Number of ancestors of the unit';
COMMENT ON COLUMN admin.unit.children IS 'Number of child units';
COMMENT ON COLUMN admin.unit.name IS 'Name in the order of -name_lang, then name, int_name and the first name:* tag';
COMMENT ON COLUMN admin.unit.names IS 'All name, name:*, *_name and *_name:* tags';
COMMENT ON COLUMN admin.unit.tags IS 'All tags of the relation';
COMMENT ON COLUMN admin.unit.h3_3 IS 'Sorted h3_3 cells of the objects bound to the unit or to its descendants, including 32767; filter partitions with h3_3 = ANY (...)';
COMMENT ON COLUMN admin.unit.centre IS 'Centre of the unit, not filled by the loader';
COMMENT ON COLUMN admin.unit.geom IS 'Polygon of the unit assembled by osmium; NULL for relations cut by the extract';
COMMENT ON COLUMN admin.unit.area_m2 IS 'Spherical area of the polygon, square meters';
COMMENT ON COLUMN admin.unit.own_area_m2 IS 'Area of the own territory: the unit minus its descendants, square meters';
COMMENT ON COLUMN admin.unit.import_id IS 'Reserved for -update';
COMMENT ON TABLE admin.unit_region IS 'Own territory of the units (unit minus its descendants) in pieces of at most 256 vertices: a point in a piece belongs to its leaf unit';
COMMENT ON COLUMN admin.unit_region.unit_id IS 'Unit of the piece';
COMMENT ON COLUMN admin.unit_region.part_no IS 'Number of the piece within the unit, from 1';
COMMENT ON COLUMN admin.unit_region.admin_level IS 'admin_level of the unit';
COMMENT ON COLUMN admin.unit_region.geom IS 'Piece of the own territory on the 1e-7 degree grid';
COMMENT ON TABLE admin.state IS 'Parameters, fingerprint and report of the admin unit directory load';
COMMENT ON COLUMN admin.state.operation IS 'load for a full load';
COMMENT ON COLUMN admin.state.osm_import_id IS 'Row of osm_import_state, NULL without -updatable';
COMMENT ON COLUMN admin.state.parameters IS 'Value of the openstreetmap_h3:admin_units key: versions, fingerprint, column names, directory and binding parameters and the report';
COMMENT ON COLUMN admin.state.units IS 'Number of units';
COMMENT ON COLUMN admin.state.object_rows IS 'Number of nodes, ways and multipolygons with admin_units';
COMMENT ON COLUMN admin.state.finished_at IS 'Time of the import';
COMMIT;

ANALYZE admin.unit;
ANALYZE admin.unit_region;
