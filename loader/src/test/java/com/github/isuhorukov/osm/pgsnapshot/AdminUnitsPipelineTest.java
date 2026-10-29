package com.github.isuhorukov.osm.pgsnapshot;

import com.github.isuhorukov.osm.pgsnapshot.jdbc.ResultDirectoryImport;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.FileInputStream;
import java.io.IOException;
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
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class AdminUnitsPipelineTest {

    private static final Path OSMIUM = Paths.get("/usr/bin/osmium");
    private static final String POSTGIS_IMAGE = "postgis/postgis:19rc1-master";
    private static final String PASSWORD = "osmworld";
    private static final double LON = 98.9;
    private static final double LAT = 18.7;
    private static final String SUBTREE_WAYS = "SELECT count(*)::text FROM ways w, admin.unit u WHERE u.id = 10 %s"
            + "AND w.admin_units && ARRAY(SELECT id FROM admin.unit WHERE path @> ARRAY[10::bigint])";

    @TempDir
    Path tempDir;

    @BeforeAll
    static void osmiumAvailable() {
        assumeTrue(Files.isExecutable(OSMIUM), OSMIUM + " is required");
    }

    static String fixture() {
        Osm osm = new Osm();
        osm.unit(1, 2, 0, 0, 0.2, 0.2, "r10@subarea", "r11@subarea");
        osm.unit(10, 4, 0, 0, 0.1, 0.2, "r20@subarea");
        osm.unit(11, 4, 0.1, 0, 0.2, 0.2, "r21@subarea");
        osm.unit(20, 6, 0, 0, 0.1, 0.1);
        osm.unit(21, 6, 0.1, 0, 0.2, 0.1);
        osm.ring(0.02, 0.02, 0.03, 0.03, "building=yes");
        osm.ring(0.095, 0.02, 0.105, 0.03, "building=yes");
        osm.node(0.15, 0.05, "amenity=atm");
        osm.node(0.05, 0.15, "place=village", "name=Ban");
        osm.node(0.1, 0.05, "building=yes");
        osm.node(0.3, 0.3, "amenity=atm");
        osm.node(0.05, 0.05, "amenity=bench");
        osm.way(new long[]{osm.node(0.05, 0.051), osm.node(0.15, 0.051)}, "highway=residential");
        osm.multipolygon(100, osm.ring(0.05, 0.03, 0.15, 0.08), "leisure=nature_reserve");
        osm.multipolygon(101, osm.ring(0.12, 0.12, 0.13, 0.13), "building=yes");
        osm.way(new long[]{osm.node(-1.0, 0.05), osm.node(1.5, 0.05)}, "highway=track");
        return osm.xml();
    }

    private Path pbf(String directory) throws IOException, InterruptedException {
        Path dir = Files.createDirectory(tempDir.resolve(directory));
        Path osm = dir.resolve("admin.osm");
        Files.write(osm, fixture().getBytes(StandardCharsets.UTF_8));
        Path pbf = dir.resolve("admin.osm.pbf");
        Process process = new ProcessBuilder(OSMIUM.toString(), "cat", osm.toString(), "-o", pbf.toString())
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        assertEquals(0, process.waitFor());
        Files.delete(osm);
        return pbf;
    }

    private static int run(Path pbf, String... options) throws Exception {
        List<String> arguments = new ArrayList<>(List.of("-source_pbf", pbf.toString(), "-osmium_docker", "false",
                "-worker_threads", "2", "-pg_script_count", "2"));
        arguments.addAll(List.of(options));
        return OsmPbfTransformation.run(arguments.toArray(new String[0]));
    }

    private static List<String> fileNames(Path directory) throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.map(file -> file.getFileName().toString()).sorted().collect(Collectors.toList());
        }
    }

    private static List<Path> files(Path directory) throws IOException {
        try (Stream<Path> files = Files.walk(directory)) {
            return files.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
        }
    }

    private static Map<String, String> lastColumnsById(Path directory, int idColumn) throws IOException {
        Map<String, String> columns = new java.util.TreeMap<>();
        for (Path file : files(directory)) {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                String[] fields = line.split("\t");
                columns.put(fields[idColumn], fields[fields.length - 2] + " " + fields[fields.length - 1]);
            }
        }
        return columns;
    }

    private static String duckdb(String sql) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), sql);
            return resultSet.getString(1);
        }
    }

    @Test
    void run_tsvAndParquet_writesColumnsDirectoryMetadataAndExport() throws Exception {
        Path pbf = pbf("tsv");
        Path export = tempDir.resolve("export");
        Path cache = tempDir.resolve("cache");

        assertEquals(0, run(pbf, "-arrow_format", "PARQUET", "-admin_units", "-admin_unit_shares",
                "-admin_units_export", export.toString(), "-admin_units_cache", cache.toString()));

        Path result = pbf.resolveSibling("admin_loc_ways");
        assertTsvColumns(result);
        assertImportScripts(result);
        assertParquetFiles(result.resolve("arrow"));
        assertEquals(List.of("admin_regions.geojsonl", "admin_regions.parquet", "admin_units.geojsonl",
                "admin_units.jsonl", "admin_units.parquet", "admin_units_state.json"), fileNames(export));
        assertTrue(Files.isRegularFile(cache.resolve("admin_index.bin")));
        assertEquals(5, Files.readAllLines(export.resolve("admin_units.jsonl")).size());
    }

    private static void assertTsvColumns(Path result) throws IOException {
        Map<String, String> nodes = lastColumnsById(result.resolve("nodes"), 2);
        assertEquals("{21} {1.0}", nodes.get("29"));
        assertEquals("{10} {1.0}", nodes.get("30"));
        assertEquals("{20,21} {1.0,1.0}", nodes.get("31"));
        assertEquals("\\N \\N", nodes.get("32"));
        assertEquals("\\N \\N", nodes.get("33"));
        Map<String, String> ways = lastColumnsById(result.resolve("ways"), 2);
        assertEquals("{20} {1.0}", ways.get("6"));
        assertTrue(ways.get("7").startsWith("{20,21} {0.5"), ways.get("7"));
        assertEquals("\\N \\N", ways.get("8"));
        assertEquals("\\N \\N", ways.get("1"));
        String multipolygons = files(result.resolve("multipolygon")).stream().map(AdminUnitsPipelineTest::read)
                .collect(Collectors.joining());
        assertTrue(multipolygons.contains("\t{20,21}\t{0.5"), multipolygons);
        assertTrue(multipolygons.contains("\t{11}\t{1.0}\n"), multipolygons);
    }

    private static void assertImportScripts(Path result) throws IOException {
        assertEquals(List.of("state.tsv", "unit.tsv", "unit_region.tsv"), fileNames(result.resolve("admin_units")));
        assertTrue(fileNames(result.resolve("sql_finalize")).contains("admin_units_import.sql"));
        assertTrue(read(result.resolve("static/database_init.sql")).contains("admin_unit_shares real[]"));
        assertTrue(read(result.resolve("sql/nodes_import_000.sql")).contains(",admin_units,admin_unit_shares)"));
        assertTrue(read(result.resolve("sql/ways_32767_import.sql")).contains("admin_units,admin_unit_shares  from"));
        assertTrue(read(result.resolve("static/multipolygon_insert.sql")).contains("h3_3_multi_regions,admin_units"));
    }

    private static void assertParquetFiles(Path arrow) throws IOException, SQLException {
        assertEquals(List.of("admin_regions.parquet", "admin_units.parquet"), fileNames(arrow.resolve("admin_units")));
        assertEquals("21", duckdb("SELECT admin_units[1]::text FROM read_parquet('" + arrow
                + "/nodes/*.parquet') WHERE id = 29"));
        assertEquals("5 5", duckdb("SELECT count(*) || ' ' || count(geom) FROM read_parquet('" + arrow
                + "/admin_units/admin_units.parquet')"));
        assertEquals("[1, 10, 20]", duckdb("SELECT path::text FROM read_parquet('" + arrow
                + "/admin_units/admin_units.parquet') WHERE id = 20"));
        assertFileMetadata(arrow);
        String units = "'" + arrow + "/admin_units/admin_units.parquet'";
        String subtree = "WITH u AS (SELECT list(id) ids FROM " + units + " WHERE list_contains(path, 10)), "
                + "c AS (SELECT unnest(h3_3) h33 FROM " + units + " WHERE id = 10) SELECT count(*)::text FROM "
                + "read_parquet('" + arrow + "/ways/*.parquet') w, u WHERE %s list_has_any(w.admin_units, u.ids)";
        assertEquals(duckdb(String.format(subtree, "")),
                duckdb(String.format(subtree, "w.h33 IN (SELECT h33 FROM c) AND")));
        assertEquals("2", duckdb(String.format(subtree, "")));
    }

    private static void assertFileMetadata(Path arrow) throws SQLException {
        String files = "'" + arrow + "/*/*.parquet'";
        String fileCount = duckdb("SELECT count(DISTINCT file_name)::text FROM parquet_metadata(" + files + ")");
        assertEquals(fileCount, duckdb("SELECT count(*)::text FROM parquet_kv_metadata(" + files
                + ") WHERE decode(key) = 'openstreetmap_h3:import_state'"));
        assertEquals(Integer.parseInt(fileCount) - 1 + " 1", duckdb("SELECT count(*) || ' ' || "
                + "count(DISTINCT decode(value)::JSON->>'fingerprint') FROM parquet_kv_metadata(" + files
                + ") WHERE decode(key) = 'openstreetmap_h3:admin_units'"));
        assertEquals("0", duckdb("SELECT count(*)::text FROM parquet_kv_metadata('" + arrow
                + "/relations/*.parquet') WHERE decode(key) = 'openstreetmap_h3:admin_units'"));
        assertTrue(duckdb("SELECT decode(value) FROM parquet_kv_metadata('" + arrow
                + "/admin_units/admin_units.parquet') WHERE decode(key) = 'openstreetmap_h3:admin_units'")
                .contains("\"report\":{\"units\":5,"));
    }

    @Test
    void run_twice_writesByteIdenticalFiles() throws Exception {
        Path first = pbf("first");
        Path second = pbf("second");
        assertEquals(0, run(first, "-arrow_format", "PARQUET", "-admin_units"));
        assertEquals(0, run(second, "-arrow_format", "PARQUET", "-admin_units"));
        Path firstResult = first.resolveSibling("admin_loc_ways");
        Path secondResult = second.resolveSibling("admin_loc_ways");
        List<Path> firstFiles = files(firstResult.resolve("arrow"));
        assertFalse(firstFiles.isEmpty());
        firstFiles.addAll(files(firstResult.resolve("admin_units")));
        for (Path file : firstFiles) {
            assertArrayEquals(Files.readAllBytes(file), Files.readAllBytes(secondResult.resolve(firstResult.relativize(file))),
                    file.toString());
        }
    }

    @Test
    void run_arrowIpcWithoutTsv_writesDirectoryAndFooterKeys() throws Exception {
        Path pbf = pbf("ipc");

        assertEquals(0, run(pbf, "-arrow_format", "ARROW_IPC", "-result_in_tsv", "false", "-admin_units"));

        Path arrow = pbf.resolveSibling("admin_loc_ways").resolve("arrow");
        assertEquals(List.of("admin_regions.arrow", "admin_units.arrow"), fileNames(arrow.resolve("admin_units")));
        assertFalse(Files.exists(pbf.resolveSibling("admin_loc_ways").resolve("admin_units")));
        for (Path file : List.of(arrow.resolve("admin_units/admin_units.arrow"), arrow.resolve("nodes/nodes_000.arrow"),
                arrow.resolve("multipolygon/multipolygon_000.arrow"))) {
            Map<String, String> footer = footer(file);
            assertTrue(footer.containsKey("openstreetmap_h3:import_state"), file + " " + footer.keySet());
            assertTrue(footer.containsKey("openstreetmap_h3:admin_units"), file + " " + footer.keySet());
        }
        Map<String, String> relations = footer(arrow.resolve("relations/relations.arrow"));
        assertTrue(relations.containsKey("openstreetmap_h3:import_state"));
        assertFalse(relations.containsKey("openstreetmap_h3:admin_units"));
        assertFalse(footer(arrow.resolve("nodes/nodes_000.arrow")).get("openstreetmap_h3:admin_units")
                .contains("report"));
    }

    @Test
    void run_adminUnitsOnly_exportsDirectoryWithoutLoading() throws Exception {
        Path pbf = pbf("only");
        Path export = tempDir.resolve("only_export");

        assertEquals(0, run(pbf, "-admin_units", "-admin_units_only", "-admin_units_export", export.toString(),
                "-name_lang", "en"));

        assertEquals(List.of("admin.osm.pbf"), fileNames(pbf.getParent()));
        assertEquals(6, fileNames(export).size());
        String state = read(export.resolve("admin_units_state.json"));
        assertTrue(state.contains("\"name_lang\":[\"en\"]"), state);
        assertTrue(state.contains("\"source_pbf\":\"admin.osm.pbf\""), state);
        assertEquals("[]", duckdb("SELECT h3_3::text FROM read_parquet('" + export
                + "/admin_units.parquet') WHERE id = 20"));
    }

    @Test
    void run_tsvImportedIntoPostgresql_hasDirectoryColumnsAndPrunedSubtree() throws Exception {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is required");
        Path pbf = pbf("postgresql");
        assertEquals(0, run(pbf, "-admin_units", "-admin_unit_shares"));
        try (GenericContainer<?> postgis = new GenericContainer<>(DockerImageName.parse(POSTGIS_IMAGE))
                .withEnv("POSTGRES_PASSWORD", PASSWORD)
                .withExposedPorts(5432)
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2)
                        .withStartupTimeout(Duration.ofMinutes(2)))) {
            postgis.start();
            String url = "jdbc:postgresql://" + postgis.getHost() + ":" + postgis.getMappedPort(5432)
                    + "/postgres?user=postgres&password=" + PASSWORD;
            try (Connection connection = DriverManager.getConnection(url)) {
                ResultDirectoryImport.importResultDirectory(connection, pbf.resolveSibling("admin_loc_ways").toFile());
                assertEquals("5 1", query(connection, "SELECT count(*) || ' ' || count(*) FILTER (WHERE parent_id IS NULL) "
                        + "FROM admin.unit"));
                assertNotEquals("0", query(connection, "SELECT count(*)::text FROM admin.unit_region"));
                assertEquals("{1,10,20}", query(connection, "SELECT path::text FROM admin.unit WHERE id = 20"));
                assertEquals("{20,21} {1,1}", query(connection, "SELECT admin_units::text || ' ' || admin_unit_shares::text "
                        + "FROM nodes WHERE id = 31"));
                assertEquals("{11}", query(connection, "SELECT admin_units::text FROM multipolygon WHERE id = 101"));
                assertEquals("2", query(connection, "SELECT count(*)::text FROM multipolygon WHERE admin_units IS NOT NULL"));
                assertEquals(query(connection, String.format(SUBTREE_WAYS, "")),
                        query(connection, String.format(SUBTREE_WAYS, "AND w.h3_3 = ANY (u.h3_3) ")));
                assertEquals("2", query(connection, String.format(SUBTREE_WAYS, "")));
                assertEquals("5", query(connection, "SELECT parameters->'report'->>'units' FROM admin.state"));
                assertEquals("0", query(connection, "SELECT count(*)::text FROM nodes n JOIN LATERAL ("
                        + "SELECT array_agg(DISTINCT r.unit_id ORDER BY r.unit_id) units FROM admin.unit_region r "
                        + "WHERE ST_Intersects(r.geom, n.geom)) h ON true WHERE n.admin_units IS NOT NULL AND n.id <> 31 "
                        + "AND h.units IS DISTINCT FROM (SELECT array_agg(u ORDER BY u) FROM unnest(n.admin_units) u)"));
            }
        }
    }

    private static String query(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), sql);
            return resultSet.getString(1);
        }
    }

    private static Map<String, String> footer(Path file) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             FileInputStream input = new FileInputStream(file.toFile());
             ArrowFileReader reader = new ArrowFileReader(input.getChannel(), allocator,
                     CommonsCompressionFactory.INSTANCE)) {
            reader.getVectorSchemaRoot();
            return reader.getMetaData();
        }
    }

    private static String read(Path file) {
        try {
            return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static final class Osm {
        private final StringBuilder nodes = new StringBuilder();
        private final StringBuilder ways = new StringBuilder();
        private final StringBuilder relations = new StringBuilder();
        private long nodeId;
        private long wayId;

        long node(double x, double y, String... tags) {
            nodeId++;
            nodes.append(String.format(Locale.ROOT, "  <node id=\"%d\" version=\"1\" lat=\"%.7f\" lon=\"%.7f\">%n",
                    nodeId, LAT + y, LON + x)).append(tags(tags)).append("  </node>\n");
            return nodeId;
        }

        long way(long[] wayNodes, String... tags) {
            wayId++;
            ways.append("  <way id=\"").append(wayId).append("\" version=\"1\">\n");
            for (long wayNode : wayNodes) {
                ways.append("    <nd ref=\"").append(wayNode).append("\"/>\n");
            }
            ways.append(tags(tags)).append("  </way>\n");
            return wayId;
        }

        long ring(double minX, double minY, double maxX, double maxY, String... tags) {
            long first = node(minX, minY);
            return way(new long[]{first, node(maxX, minY), node(maxX, maxY), node(minX, maxY), first}, tags);
        }

        void unit(long id, int level, double minX, double minY, double maxX, double maxY, String... subareas) {
            long outer = ring(minX, minY, maxX, maxY, "boundary=administrative");
            List<String> members = new ArrayList<>();
            members.add("way:" + outer + ":outer");
            for (String subarea : subareas) {
                members.add("relation:" + subarea.substring(1, subarea.indexOf('@')) + ":subarea");
            }
            relation(id, members, "type=boundary", "boundary=administrative", "admin_level=" + level,
                    "name=unit" + id);
        }

        void multipolygon(long id, long outer, String... tags) {
            List<String> allTags = new ArrayList<>(List.of("type=multipolygon"));
            allTags.addAll(List.of(tags));
            relation(id, List.of("way:" + outer + ":outer"), allTags.toArray(new String[0]));
        }

        void relation(long id, List<String> members, String... tags) {
            relations.append("  <relation id=\"").append(id).append("\" version=\"1\">\n");
            for (String member : members) {
                String[] parts = member.split(":");
                relations.append("    <member type=\"").append(parts[0]).append("\" ref=\"").append(parts[1])
                        .append("\" role=\"").append(parts[2]).append("\"/>\n");
            }
            relations.append(tags(tags)).append("  </relation>\n");
        }

        private static String tags(String... tags) {
            StringBuilder xml = new StringBuilder();
            for (String tag : tags) {
                int equals = tag.indexOf('=');
                xml.append("    <tag k=\"").append(tag, 0, equals).append("\" v=\"").append(tag.substring(equals + 1))
                        .append("\"/>\n");
            }
            return xml.toString();
        }

        String xml() {
            return "<?xml version='1.0' encoding='UTF-8'?>\n<osm version=\"0.6\" generator=\"test\">\n" + nodes + ways
                    + relations + "</osm>\n";
        }
    }
}
