ALTER TABLE nodes ADD COLUMN version integer NOT NULL;
ALTER TABLE ways ADD COLUMN version integer NOT NULL, ADD COLUMN geom_hash bigint NOT NULL;
ALTER TABLE relations ADD COLUMN version integer NOT NULL;

CREATE TABLE osm_import_state(
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation text NOT NULL,
    source_pbf text NOT NULL,
    replication_timestamp timestamptz,
    replication_sequence bigint,
    replication_url text,
    parameters jsonb NOT NULL,
    imported_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE osm_change_log(
    import_id integer NOT NULL,
    object_type character(1) NOT NULL,
    id bigint NOT NULL,
    operation character(1) NOT NULL
);
CREATE INDEX idx_osm_change_log_import_id ON osm_change_log (import_id);

COMMENT ON COLUMN nodes.version IS 'Version of the OpenStreetMap node, used by the loader in -update mode to skip unchanged nodes';
COMMENT ON COLUMN ways.version IS 'Version of the OpenStreetMap way, used by the loader in -update mode to skip unchanged ways';
COMMENT ON COLUMN ways.geom_hash IS 'FarmHash Fingerprint64 of the way node coordinates in 1e-7 degree units (int32 latitude, int32 longitude, little-endian, in way order): detects moved nodes without a new way version';
COMMENT ON COLUMN relations.version IS 'Version of the OpenStreetMap relation, used by the loader in -update mode to skip unchanged relations';
COMMENT ON TABLE osm_import_state IS 'Loads and updates of this database: source PBF, its replication state and the loader parameters that must stay the same for -update';
COMMENT ON COLUMN osm_import_state.operation IS 'load for the initial load with -updatable, update for -update';
COMMENT ON COLUMN osm_import_state.source_pbf IS 'File name of the source PBF without directory';
COMMENT ON COLUMN osm_import_state.replication_timestamp IS 'osmosis_replication_timestamp from the OSMHeader block of the source PBF';
COMMENT ON COLUMN osm_import_state.replication_sequence IS 'osmosis_replication_sequence_number from the OSMHeader block of the source PBF';
COMMENT ON COLUMN osm_import_state.replication_url IS 'osmosis_replication_base_url from the OSMHeader block of the source PBF';
COMMENT ON COLUMN osm_import_state.parameters IS 'Loader parameters and tag extension fingerprints that define the table content';
COMMENT ON TABLE osm_change_log IS 'Objects created (C), updated (U) and deleted (D) by each -update: N node, W way, R relation, M multipolygon';
