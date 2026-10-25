package com.github.isuhorukov.osm.pgsnapshot;

import com.github.isuhorukov.osm.pgsnapshot.jdbc.ResultDirectoryImport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class PostgresqlJdbcLoadTest {

    private static final Path OSMIUM = Paths.get("/usr/bin/osmium");
    private static final String POSTGIS_IMAGE = "postgis/postgis:19rc1-master";
    private static final String PASSWORD = "osmworld";
    private static final List<String> CONTENT_QUERIES = List.of(
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t.id)) FROM "
                    + "(SELECT id, h3_3, h3_8, tags, geom FROM nodes) t",
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t.id)) FROM "
                    + "(SELECT id, closed, building, highway, h3_3, h3_8, scale, tags, bbox, centre, linestring, points, "
                    + "h3_8_regions, h3_3_multi_regions FROM ways) t",
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t.id)) FROM (SELECT id, tags FROM relations) t",
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t.relation_id, t.sequence_id)) FROM "
                    + "(SELECT relation_id, member_id, sequence_id, member_type, member_role FROM relation_members) t",
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t.id)) FROM "
                    + "(SELECT id, h3_3, h3_8, scale, tags, bbox, centre, polygon, h3_3_multi_regions FROM multipolygon) t",
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t::text)) FROM osm_file_block_content t",
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t::text)) FROM osm_stat_ways_3_3 t",
            "SELECT count(*) || ' ' || md5(string_agg(t::text, E'\\n' ORDER BY t::text)) FROM osm_stat_nodes_3_3 t",
            "SELECT count(*)::text FROM osm_file_statistics",
            "SELECT count(*)::text FROM h3_3_bounds_complex",
            "SELECT string_agg(c.relname, ',' ORDER BY c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    + "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v') AND c.relname !~ '_[0-9]{3}$'");

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

    private static void createDatabase(String database) throws SQLException {
        try (Connection connection = DriverManager.getConnection(url("postgres"));
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + database);
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

    private Path samplePbf(String directory) throws IOException, InterruptedException {
        Path dir = Files.createDirectory(tempDir.resolve(directory));
        Path osm = dir.resolve("sample.osm");
        try (InputStream sample = getClass().getResourceAsStream("/pipeline/sample.osm")) {
            Files.copy(Objects.requireNonNull(sample), osm);
        }
        Path pbf = dir.resolve("sample.osm.pbf");
        Process process = new ProcessBuilder(OSMIUM.toString(), "cat", osm.toString(), "-o", pbf.toString())
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        assertEquals(0, process.waitFor());
        Files.delete(osm);
        return pbf;
    }

    private static List<String> fileNames(Path directory) throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.map(file -> file.getFileName().toString()).sorted().collect(Collectors.toList());
        }
    }

    private static int runJdbc(Path pbf, String database, String... options) throws Exception {
        List<String> arguments = new java.util.ArrayList<>(List.of("-source_pbf", pbf.toString(), "-osmium_docker", "false",
                "-worker_threads", "2", "-pg_script_count", "2", "-pg_jdbc_url", url(database)));
        arguments.addAll(List.of(options));
        return OsmPbfTransformation.run(arguments.toArray(new String[0]));
    }

    private static void assertNoLoaderConnections() throws Exception {
        String loaderSessions = "SELECT 1 FROM pg_stat_activity WHERE application_name LIKE 'osm-to-pgsnapshot%'";
        try (Connection connection = DriverManager.getConnection(url("postgres"));
             Statement statement = connection.createStatement()) {
            statement.execute("DO $$ BEGIN FOR attempt IN 1..100 LOOP EXIT WHEN NOT EXISTS (" + loaderSessions
                    + "); PERFORM pg_sleep(0.1); END LOOP; END $$");
        }
        assertEquals("0", query("postgres", "SELECT count(*)::text FROM (" + loaderSessions + ") s"));
    }

    @Test
    void run_pgJdbcUrl_sameContentAsImportedFilesWithoutWritingFiles() throws Exception {
        createDatabase("osm_jdbc");
        createDatabase("osm_files");
        Path jdbcPbf = samplePbf("jdbc");
        Path filesPbf = samplePbf("files");

        assertEquals(0, runJdbc(jdbcPbf, "osm_jdbc", "-test_tag_value", "v"));
        assertEquals(0, OsmPbfTransformation.run(new String[]{"-source_pbf", filesPbf.toString(), "-osmium_docker", "false",
                "-worker_threads", "2", "-pg_script_count", "2", "-test_tag_value", "v"}));
        try (Connection connection = DriverManager.getConnection(url("osm_files"))) {
            ResultDirectoryImport.importResultDirectory(connection, filesPbf.resolveSibling("sample_loc_ways").toFile());
        }

        assertEquals(List.of("sample.osm.pbf"), fileNames(jdbcPbf.getParent()));
        for (String contentQuery : CONTENT_QUERIES) {
            assertEquals(query("osm_files", contentQuery), query("osm_jdbc", contentQuery), contentQuery);
        }
        assertNotEquals("0", query("osm_jdbc", "SELECT count(*)::text FROM nodes WHERE tags ? 'is.test'"));
        assertNotEquals("0", query("osm_jdbc", "SELECT count(*)::text FROM multipolygon WHERE tags ? 'is.test'"));
        assertNoLoaderConnections();
    }

    @Test
    void run_pgJdbcUrl_frozenPartitionsAndDefaultWaysPartition() throws Exception {
        createDatabase("osm_partitions");

        assertEquals(0, runJdbc(samplePbf("partitions"), "osm_partitions", "-parallel_multipolygon_export", "false"));

        assertEquals("ways_32767", query("osm_partitions", "SELECT c.relname FROM pg_partitioned_table p "
                + "JOIN pg_class c ON c.oid = p.partdefid WHERE p.partrelid = 'ways'::regclass"));
        assertNotEquals("0", query("osm_partitions", "SELECT count(*)::text FROM pg_inherits WHERE inhparent = 'nodes'::regclass"));
        assertNotEquals("0", query("osm_partitions", "SELECT count(*)::text FROM pg_inherits WHERE inhparent = 'multipolygon'::regclass"));
        assertEquals("1", query("osm_partitions", "SELECT count(*)::text FROM multipolygon"));
        assertEquals("true", query("osm_partitions", "SELECT (to_regclass('preimport_multipolygon') IS NULL "
                + "AND to_regclass('preimport_ways_32767') IS NULL)::text"));
        try (Connection connection = DriverManager.getConnection(url("osm_partitions"));
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION pg_visibility");
        }
        assertEquals("true", query("osm_partitions", "SELECT bool_and(v.all_frozen > 0)::text FROM pg_inherits i "
                + "CROSS JOIN LATERAL pg_visibility_map_summary(i.inhrelid) v "
                + "WHERE i.inhparent IN ('nodes'::regclass, 'ways'::regclass) AND i.inhrelid <> 'ways_32767'::regclass"));
        assertNoLoaderConnections();
    }

    @Test
    void run_pgJdbcUrlIntoLoadedDatabase_failsOnSchemaWithoutChanges() throws Exception {
        createDatabase("osm_rerun");
        Path pbf = samplePbf("rerun");
        assertEquals(0, runJdbc(pbf, "osm_rerun"));
        String nodes = query("osm_rerun", "SELECT count(*)::text FROM nodes");

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> runJdbc(pbf, "osm_rerun"));

        assertTrue(exception.getCause().getMessage().contains("already exists"), exception.getCause().getMessage());
        assertEquals(nodes, query("osm_rerun", "SELECT count(*)::text FROM nodes"));
        assertNoLoaderConnections();
    }

    @Test
    void run_pgJdbcUrlBlockFailure_streamedDataRolledBack() throws Exception {
        createDatabase("osm_failure");
        Path pbf = samplePbf("failure");

        IllegalStateException exception = assertTimeoutPreemptively(Duration.ofMinutes(2),
                () -> assertThrows(IllegalStateException.class, () -> runJdbc(pbf, "osm_failure", "-test_tag_value", "fail")));

        Throwable rootCause = exception;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        assertEquals("test extension failure", rootCause.getMessage());
        assertEquals("0", query("osm_failure", "SELECT count(*)::text FROM pg_inherits WHERE inhparent IN "
                + "('nodes'::regclass, 'ways'::regclass)"));
        assertEquals("0", query("osm_failure", "SELECT count(*)::text FROM relations"));
        assertEquals(List.of("sample.osm.pbf"), fileNames(pbf.getParent()));
        assertNoLoaderConnections();
    }
}
