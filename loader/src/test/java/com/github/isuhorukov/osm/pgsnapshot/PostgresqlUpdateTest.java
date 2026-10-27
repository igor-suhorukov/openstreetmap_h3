package com.github.isuhorukov.osm.pgsnapshot;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class PostgresqlUpdateTest {

    private static final Path OSMIUM = Paths.get("/usr/bin/osmium");
    private static final String POSTGIS_IMAGE = "postgis/postgis:19rc1-master";
    private static final String PASSWORD = "osmworld";
    private static final long UPDATE_LOCK_KEY = 0x6f736d5f75706474L;
    private static final String REPLICATION_URL = "https://download.example.org/sample-updates";
    private static final List<String> CONTENT_QUERIES = List.of(
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t.id)) FROM "
                    + "(SELECT id, h3_3, h3_8, tags, geom, version FROM nodes) t",
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t.id)) FROM "
                    + "(SELECT id, closed, building, highway, h3_3, h3_8, scale, tags, bbox, centre, linestring, points, "
                    + "h3_8_regions, h3_3_multi_regions, version, geom_hash FROM ways) t",
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t.id)) FROM "
                    + "(SELECT id, tags, version FROM relations) t",
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t.relation_id, t.sequence_id)) FROM "
                    + "(SELECT relation_id, member_id, sequence_id, member_type, member_role FROM relation_members) t",
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t.id)) FROM "
                    + "(SELECT id, h3_3, h3_8, scale, tags, bbox, centre, polygon, h3_3_multi_regions FROM multipolygon) t",
            "SELECT string_agg((tableoid = 'ways_32767'::regclass) || ' ' || id, ',' ORDER BY id) FROM ways",
            "SELECT string_agg((tableoid = 'multipolygon_32767'::regclass) || ' ' || id, ',' ORDER BY id) FROM multipolygon");

    private static GenericContainer<?> postgis;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void startPostgis() {
        assumeTrue(Files.isExecutable(OSMIUM), OSMIUM + " is required");
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is required");
        postgis = new GenericContainer<>(DockerImageName.parse(POSTGIS_IMAGE))
                .withEnv("POSTGRES_PASSWORD", PASSWORD)
                .withExposedPorts(5432)
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        postgis.start();
    }

    @AfterAll
    static void stopPostgis() {
        if (postgis != null) {
            postgis.stop();
        }
    }

    private static String url(String database) {
        return "jdbc:postgresql://" + postgis.getHost() + ":" + postgis.getMappedPort(5432) + "/" + database
                + "?user=postgres&password=" + PASSWORD;
    }

    private static void execute(String database, String sql) throws SQLException {
        try (Connection connection = DriverManager.getConnection(url(database));
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String query(String database, String sql) throws SQLException {
        try (Connection connection = DriverManager.getConnection(url(database));
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), sql);
            return resultSet.getString(1);
        }
    }

    private static List<String> content(String database) throws SQLException {
        List<String> content = new ArrayList<>();
        for (String contentQuery : CONTENT_QUERIES) {
            content.add(query(database, contentQuery));
        }
        return content;
    }

    private Path pbf(String directory, String osm, long sequence, String timestamp) throws IOException, InterruptedException {
        Path dir = Files.createDirectories(tempDir.resolve(directory));
        Path osmFile = dir.resolve("source.osm");
        Files.writeString(osmFile, osm);
        Path pbf = dir.resolve("sample.osm.pbf");
        Process process = new ProcessBuilder(OSMIUM.toString(), "cat", osmFile.toString(), "-o", pbf.toString(),
                "--output-header=osmosis_replication_sequence_number=" + sequence,
                "--output-header=osmosis_replication_timestamp=" + timestamp,
                "--output-header=osmosis_replication_base_url=" + REPLICATION_URL)
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        assertEquals(0, process.waitFor());
        Files.delete(osmFile);
        return pbf;
    }

    private static String sample(String resource) throws IOException {
        try (InputStream sample = PostgresqlUpdateTest.class.getResourceAsStream(resource)) {
            return new String(Objects.requireNonNull(sample).readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private Path version1(String directory) throws Exception {
        return pbf(directory, sample("/pipeline/sample.osm"), 1, "2025-01-01T00:00:00Z");
    }

    private Path version2(String directory) throws Exception {
        return pbf(directory, sample("/pipeline/sample_v2.osm"), 2, "2025-02-01T00:00:00Z");
    }

    private Path version3(String directory) throws Exception {
        String osm = sample("/pipeline/sample_v2.osm").replace(
                "<node id=\"11\" version=\"2\" timestamp=\"2025-02-01T00:00:00Z\" changeset=\"2\"",
                "<node id=\"11\" version=\"3\" timestamp=\"2025-03-01T00:00:00Z\" changeset=\"3\"")
                .replace("<tag k=\"amenity\" v=\"restaurant\"/>", "<tag k=\"amenity\" v=\"fast_food\"/>");
        return pbf(directory, osm, 3, "2025-03-01T00:00:00Z");
    }

    private static int load(Path pbf, String database, String... options) throws Exception {
        execute("postgres", "CREATE DATABASE " + database);
        return run(pbf, database, options);
    }

    private static int run(Path pbf, String database, String... options) throws Exception {
        List<String> arguments = new ArrayList<>(List.of("-source_pbf", pbf.toString(), "-osmium_docker", "false",
                "-worker_threads", "2", "-pg_script_count", "2", "-pg_jdbc_url", url(database)));
        arguments.addAll(List.of(options));
        return OsmPbfTransformation.run(arguments.toArray(new String[0]));
    }

    private static String lastChangeLog(String database) throws SQLException {
        return query(database, "SELECT string_agg(object_type || id || operation, ' ' ORDER BY object_type, id) "
                + "FROM osm_change_log WHERE import_id = (SELECT max(id) FROM osm_import_state)");
    }

    @Test
    void update_sameContentAsFullLoadOfNewPbfAndChangeLog() throws Exception {
        assertEquals(0, load(version1("old"), "osm_update", "-updatable", "-test_tag_value", "v"));
        assertEquals(0, load(version2("full"), "osm_full", "-updatable", "-test_tag_value", "v"));

        assertEquals(0, run(version2("new"), "osm_update", "-update", "-test_tag_value", "v"));

        assertEquals(content("osm_full"), content("osm_update"));
        assertEquals("M201U N11U N12D N16C R202C W101U W103U W104D W105C", lastChangeLog("osm_update"));
        assertEquals("load sample.osm.pbf 1 2025-01-01 00:00:00+00,update sample.osm.pbf 2 2025-02-01 00:00:00+00",
                query("osm_update", "SELECT string_agg(concat_ws(' ', operation, source_pbf, replication_sequence, "
                        + "replication_timestamp AT TIME ZONE 'UTC' || '+00'), ',' ORDER BY id) FROM osm_import_state"));
        assertEquals(query("osm_update", "SELECT parameters::text FROM osm_import_state WHERE id = 1"),
                query("osm_update", "SELECT parameters::text FROM osm_import_state WHERE id = 2"));
        assertEquals("true", query("osm_update", "SELECT (to_regclass('osm_delta_ways') IS NULL "
                + "AND to_regclass('preimport_multipolygon') IS NULL AND to_regclass('osm_affected_relations') IS NULL)::text"));
        assertEquals("1", query("osm_update", "SELECT count(*)::text FROM multipolygon WHERE tags ? 'is.test'"));
        assertEquals("v-Node", query("osm_update", "SELECT tags -> 'is.test' FROM nodes WHERE id = 16"));
    }

    @Test
    void update_objectsMovedToH3CellEdge_cellsOfNodesAndGeodesicCellBounds() throws Exception {
        assertEquals(0, load(pbf("edge_old", sample("/pipeline/h3_cell_edge_v1.osm"), 1, "2025-01-01T00:00:00Z"),
                "osm_edge_update", "-updatable"));
        assertEquals(0, load(pbf("edge_full", sample("/pipeline/h3_cell_edge.osm"), 2, "2025-02-01T00:00:00Z"),
                "osm_edge_full", "-updatable"));

        assertEquals(0, run(pbf("edge_new", sample("/pipeline/h3_cell_edge.osm"), 2, "2025-02-01T00:00:00Z"),
                "osm_edge_update", "-update"));

        assertEquals(content("osm_edge_full"), content("osm_edge_update"));
        assertEquals("M401C R401C W301U", lastChangeLog("osm_edge_update"));
        assertEquals("301 true {4481,4522},302 false null,311 false null", query("osm_edge_update",
                "SELECT string_agg(id || ' ' || (tableoid = 'ways_32767'::regclass) || ' ' "
                        + "|| coalesce(h3_3_multi_regions::text, 'null'), ',' ORDER BY id) FROM ways"));
        assertEquals("401 4481 null", query("osm_edge_update", "SELECT string_agg(id || ' ' || h3_3 || ' ' "
                + "|| coalesce(h3_3_multi_regions::text, 'null'), ',' ORDER BY id) FROM multipolygon"));
    }

    @Test
    void updatableLoad_versionColumnsIdIndexesAndPartitionsForAllRanges() throws Exception {
        assertEquals(0, load(version1("schema"), "osm_schema", "-updatable"));

        assertEquals("1,1,1", query("osm_schema", "SELECT concat_ws(',', min(version), "
                + "(SELECT min(version) FROM ways), (SELECT min(version) FROM relations)) FROM nodes"));
        assertEquals("0", query("osm_schema", "SELECT count(*)::text FROM ways WHERE geom_hash = 0"));
        assertEquals("idx_nodes_id,idx_ways_id,pk_relations", query("osm_schema",
                "SELECT string_agg(relname, ',' ORDER BY relname) FROM pg_class "
                        + "WHERE relname IN ('idx_nodes_id', 'idx_ways_id', 'pk_relations')"));
        assertEquals("CREATE UNIQUE INDEX idx_multipolygon_id_h3_3 ON ONLY public.multipolygon USING btree (id, h3_3) | "
                        + "CREATE UNIQUE INDEX idx_nodes_id ON ONLY public.nodes USING btree (id, h3_3) INCLUDE (version) | "
                        + "CREATE UNIQUE INDEX idx_ways_id ON ONLY public.ways USING btree (id, h3_3) INCLUDE (version, geom_hash)",
                query("osm_schema", "SELECT string_agg(pg_get_indexdef(indexrelid), ' | ' ORDER BY indexrelid::regclass::text) "
                        + "FROM pg_index WHERE indexrelid::regclass::text IN ('idx_multipolygon_id_h3_3', 'idx_nodes_id', 'idx_ways_id')"));
        String partitionCounts = "SELECT count(*) FILTER (WHERE inhparent = 'nodes'::regclass) || ' ' "
                + "|| count(*) FILTER (WHERE inhparent = 'ways'::regclass AND inhrelid <> 'ways_32767'::regclass) || ' ' "
                + "|| count(*) FILTER (WHERE inhparent = 'multipolygon'::regclass "
                + "AND inhrelid <> 'multipolygon_32767'::regclass) FROM pg_inherits";
        String[] counts = query("osm_schema", partitionCounts).split(" ");
        assertEquals(counts[2], counts[0]);
        assertEquals(counts[2], counts[1]);
        assertEquals("load", query("osm_schema", "SELECT operation FROM osm_import_state"));
        assertEquals("0", query("osm_schema", "SELECT count(*)::text FROM osm_change_log"));
    }

    @Test
    void update_samePbfAgain_exitsWithoutChanges() throws Exception {
        Path pbf = version2("same");
        assertEquals(0, load(version1("same_old"), "osm_same", "-updatable"));
        assertEquals(0, run(pbf, "osm_same", "-update"));
        List<String> content = content("osm_same");

        assertEquals(0, run(pbf, "osm_same", "-update"));

        assertEquals(content, content("osm_same"));
        assertEquals("2", query("osm_same", "SELECT count(*)::text FROM osm_import_state"));
        assertEquals("true", query("osm_same", "SELECT (to_regclass('osm_delta_ways') IS NULL)::text"));
    }

    @Test
    void update_refusedForOlderPbfOtherParametersOtherRulesAndRunningUpdate() throws Exception {
        assertEquals(0, load(version2("refused_new"), "osm_refused", "-updatable", "-test_tag_value", "v"));
        List<String> content = content("osm_refused");

        assertEquals(OsmPbfTransformation.UPDATE_REFUSED_EXIT_CODE,
                run(version1("refused_old"), "osm_refused", "-update", "-test_tag_value", "v"));
        assertEquals(OsmPbfTransformation.UPDATE_REFUSED_EXIT_CODE,
                run(version3("refused_skip"), "osm_refused", "-update", "-test_tag_value", "v", "-skip_buildings"));
        assertEquals(OsmPbfTransformation.UPDATE_REFUSED_EXIT_CODE,
                run(version3("refused_rules"), "osm_refused", "-update", "-test_tag_value", "w"));
        assertEquals(OsmPbfTransformation.UPDATE_REFUSED_EXIT_CODE,
                run(version3("refused_extension"), "osm_refused", "-update"));
        try (Connection lock = DriverManager.getConnection(url("osm_refused"));
             Statement statement = lock.createStatement()) {
            statement.execute("SELECT pg_advisory_lock(" + UPDATE_LOCK_KEY + ")");
            assertEquals(OsmPbfTransformation.UPDATE_REFUSED_EXIT_CODE,
                    run(version3("refused_lock"), "osm_refused", "-update", "-test_tag_value", "v"));
        }

        assertEquals(content, content("osm_refused"));
        assertEquals("1", query("osm_refused", "SELECT count(*)::text FROM osm_import_state"));
    }

    @Test
    void update_foreignKeysToUpdatedTables_refusedWithTheirListBeforeApplying() throws Exception {
        assertEquals(0, load(version2("fk_new"), "osm_fk", "-updatable"));
        List<String> content = content("osm_fk");
        execute("osm_fk", "CREATE TABLE reach_origin (origin_id int PRIMARY KEY, way_id bigint, osm_h3_3 smallint, "
                + "CONSTRAINT origin_way_fk FOREIGN KEY (way_id, osm_h3_3) REFERENCES ways (id, h3_3) DEFERRABLE INITIALLY DEFERRED)");
        execute("osm_fk", "INSERT INTO reach_origin SELECT row_number() OVER (ORDER BY id), id, h3_3 FROM ways");
        execute("osm_fk", "CREATE TABLE reach_target (target_id int PRIMARY KEY, node_id bigint, osm_h3_3 smallint, "
                + "CONSTRAINT target_node_fk FOREIGN KEY (node_id, osm_h3_3) REFERENCES nodes (id, h3_3))");
        Path version3 = version3("fk_v3");
        Logger console = (Logger) LoggerFactory.getLogger("com.github.isuhorukov.osm.pgsnapshot.console");
        ListAppender<ILoggingEvent> messages = new ListAppender<>();
        messages.start();
        console.addAppender(messages);
        try {
            assertEquals(OsmPbfTransformation.UPDATE_REFUSED_EXIT_CODE, run(version3, "osm_fk", "-update"));
        } finally {
            console.detachAppender(messages);
        }

        String printed = messages.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.joining("\n"));
        assertTrue(printed.contains("reach_origin (origin_way_fk) -> ways, reach_target (target_node_fk) -> nodes"), printed);
        assertEquals(content, content("osm_fk"));
        assertEquals("1", query("osm_fk", "SELECT count(*)::text FROM osm_import_state"));
        assertEquals("false", query("osm_fk", "SELECT (to_regclass('osm_delta_ways') IS NOT NULL)::text"));

        execute("osm_fk", "DROP TABLE reach_origin, reach_target");
        assertEquals(0, run(version3, "osm_fk", "-update"));

        assertEquals("N11U", lastChangeLog("osm_fk"));
        assertEquals("load,update", query("osm_fk", "SELECT string_agg(operation, ',' ORDER BY id) FROM osm_import_state"));
    }

    @Test
    void update_databaseLoadedWithoutUpdatable_refused() throws Exception {
        assertEquals(0, load(version1("plain_old"), "osm_plain"));

        assertEquals(OsmPbfTransformation.UPDATE_REFUSED_EXIT_CODE, run(version2("plain_new"), "osm_plain", "-update"));

        assertEquals("false", query("osm_plain", "SELECT (to_regclass('osm_delta_ways') IS NOT NULL)::text"));
    }

    @Test
    void update_failureWhileApplying_databaseUnchangedAndNextRunSucceeds() throws Exception {
        assertEquals(0, load(version2("failure_new"), "osm_failure", "-updatable"));
        List<String> content = content("osm_failure");
        execute("osm_failure", "CREATE FUNCTION reject_node() RETURNS trigger LANGUAGE plpgsql AS "
                + "$$ BEGIN RAISE EXCEPTION 'node rejected by trigger'; END $$");
        execute("osm_failure", "CREATE TRIGGER reject_node BEFORE INSERT ON nodes FOR EACH ROW EXECUTE FUNCTION reject_node()");
        Path version3 = version3("failure_v3");

        assertThrows(IllegalStateException.class, () -> run(version3, "osm_failure", "-update"));
        assertEquals(content, content("osm_failure"));
        assertEquals("1", query("osm_failure", "SELECT count(*)::text FROM osm_import_state"));
        assertEquals("0", query("osm_failure", "SELECT count(*)::text FROM osm_change_log"));

        execute("osm_failure", "DROP TRIGGER reject_node ON nodes");
        assertEquals(0, run(version3, "osm_failure", "-update"));

        assertEquals("N11U", lastChangeLog("osm_failure"));
        assertEquals("load,update", query("osm_failure", "SELECT string_agg(operation, ',' ORDER BY id) FROM osm_import_state"));
        assertEquals("fast_food", query("osm_failure", "SELECT tags -> 'amenity' FROM nodes WHERE id = 11"));
    }
}
