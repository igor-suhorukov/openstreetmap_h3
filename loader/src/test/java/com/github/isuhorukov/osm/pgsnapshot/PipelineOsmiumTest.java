package com.github.isuhorukov.osm.pgsnapshot;

import com.github.isuhorukov.osm.pgsnapshot.pbfextractor.PbfBlobOffsets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class PipelineOsmiumTest {

    private static final Path OSMIUM = Paths.get("/usr/bin/osmium");

    @TempDir
    Path tempDir;

    @BeforeAll
    static void osmiumAvailable() {
        assumeTrue(Files.isExecutable(OSMIUM), OSMIUM + " is required");
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

    private static String content(Path directory) throws IOException {
        StringBuilder content = new StringBuilder();
        for (String name : fileNames(directory)) {
            content.append(new String(Files.readAllBytes(directory.resolve(name)), StandardCharsets.UTF_8));
        }
        return content.toString();
    }

    @Test
    void run_tsvAndParquetWithTagExtension_writesPostgresqlAndParquetResults() throws Exception {
        Path pbf = samplePbf("tsv");

        assertEquals(0, OsmPbfTransformation.run(new String[]{"-source_pbf", pbf.toString(), "-osmium_docker", "false",
                "-arrow_format", "PARQUET", "-spill_compression", "NONE", "-parquet_compression", "SNAPPY",
                "-worker_threads", "2", "-test_tag_value", "v"}));

        Path result = pbf.resolveSibling("sample_loc_ways");
        assertTrue(Files.isRegularFile(result.resolve("ways/32767.tsv")));
        assertTrue(content(result.resolve("nodes")).contains("is.test\\\"=>\\\"v-Node"));
        assertTrue(content(result.resolve("ways")).contains("Majeedhee Magu"));
        assertTrue(content(result.resolve("relations")).contains("is.test\\\"=>\\\"v-Relation"));
        assertTrue(content(result.resolve("multipolygon")).contains("201"));
        assertTrue(fileNames(result.resolve("sql")).containsAll(List.of("nodes_import_000.sql", "ways_import_000.sql",
                "y_multipoly_aa.sql", "relations_import.sql", "ways_32767_import.sql")));
        assertEquals(List.of("multipolygon_import.sql", "nodes_analyze.sql", "ways_default_partition.sql"),
                fileNames(result.resolve("sql_finalize")));
        assertTrue(content(result.resolve("sql")).contains("CSV FREEZE;"));
        assertTrue(fileNames(result.resolve("static")).containsAll(List.of("database_init.sql",
                "database_after_init.sql", "multipolygon.sql", "multipolygon_tables.sql", "h3_poly.tsv.gz")));
        assertTrue(content(result.resolve("static")).contains("extension_tags from relations"));
        assertEquals(List.of("osm_file_block.tsv", "osm_file_block_content.tsv", "osm_file_statistics.tsv"),
                fileNames(result.resolve("import_related_metadata")));
        assertEquals(List.of("multipolygon_000.parquet"), fileNames(result.resolve("arrow/multipolygon")));
        assertEquals(List.of("nodes_000.parquet"), fileNames(result.resolve("arrow/nodes")));
        assertEquals(List.of("ways_000.parquet", "ways_32767.parquet"), fileNames(result.resolve("arrow/ways")));
        assertEquals(List.of("relations.parquet"), fileNames(result.resolve("arrow/relations")));
        assertFalse(Files.exists(result.resolve("arrow/spill")));
        assertEquals("SNAPPY", parquetCompression(result.resolve("arrow/ways/ways_000.parquet")));
        assertEquals("SNAPPY", parquetCompression(result.resolve("arrow/multipolygon/multipolygon_000.parquet")));
    }

    private static String parquetCompression(Path file) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT string_agg(DISTINCT compression, ',') FROM parquet_metadata('"
                     + file + "')")) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void run_arrowIpcWithoutTsv_writesArrowFilesOnly() throws Exception {
        Path pbf = samplePbf("arrow");

        assertEquals(0, OsmPbfTransformation.run(new String[]{"-source_pbf", pbf.toString(), "-osmium_docker", "false",
                "-arrow_format", "ARROW_IPC", "-result_in_tsv", "false", "-parallel_multipolygon_export", "false"}));

        Path result = pbf.resolveSibling("sample_loc_ways");
        assertEquals(List.of("multipolygon_000.parquet"), fileNames(result.resolve("arrow/multipolygon")));
        assertFalse(Files.exists(result.resolve("arrow/spill")));
        assertTrue(fileNames(result.resolve("arrow/nodes")).stream().allMatch(name -> name.endsWith(".arrow")));
        assertFalse(fileNames(result.resolve("arrow/ways")).isEmpty());
        assertFalse(Files.exists(result.resolve("multipolygon")));
        assertFalse(Files.exists(result.resolve("nodes")));
    }

    private Throwable runFailureRootCause(String testTagValue) throws Exception {
        Path pbf = samplePbf(testTagValue);
        String[] arguments = {"-source_pbf", pbf.toString(), "-osmium_docker", "false",
                "-worker_threads", "2", "-test_tag_value", testTagValue};

        IllegalStateException exception = assertTimeoutPreemptively(Duration.ofMinutes(2),
                () -> assertThrows(IllegalStateException.class, () -> OsmPbfTransformation.run(arguments)));

        Throwable rootCause = exception;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }

    @Test
    void run_blockProcessingThrowsException_runFailsWithCause() throws Exception {
        assertEquals("test extension failure", runFailureRootCause("fail").getMessage());
    }

    @Test
    void run_blockProcessingThrowsError_runFailsWithCause() throws Exception {
        Throwable rootCause = runFailureRootCause("error");

        assertInstanceOf(LinkageError.class, rootCause);
        assertEquals("test extension error", rootCause.getMessage());
    }

    @Test
    void run_collectOnlyStatistics_writesStatisticsWithoutData() throws Exception {
        Path pbf = samplePbf("statistics");

        assertEquals(0, OsmPbfTransformation.run(new String[]{"-source_pbf", pbf.toString(), "-osmium_docker", "false",
                "-collect_only_statistics"}));

        Path result = pbf.resolveSibling("sample_loc_ways");
        assertFalse(content(result.resolve("import_related_metadata")).isEmpty());
        assertTrue(fileNames(result.resolve("nodes")).isEmpty());
        assertTrue(fileNames(result.resolve("sql")).isEmpty());
    }

    @Test
    void splitterAndBlobOffsetsMain_readSamplePbf() throws Exception {
        Path pbf = samplePbf("tools");

        Splitter.main(new String[]{pbf.toString()});
        PbfBlobOffsets.main(new String[]{pbf.toString()});

        assertFalse(fileNames(pbf.resolveSibling("sample.osm_blocks")).isEmpty());
    }

    @Test
    void toolsMain_withoutPbfArgument_throws() {
        String[] noArguments = {};
        assertThrows(IllegalArgumentException.class, () -> Splitter.main(noArguments));
        assertThrows(IllegalArgumentException.class, () -> PbfBlobOffsets.main(noArguments));
    }
}
