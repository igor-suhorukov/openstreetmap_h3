package com.github.isuhorukov.osm.pgsnapshot;

import com.beust.jcommander.JCommander;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class OsmPbfTransformationParseTest {

    @TempDir
    Path tempDir;

    private static int run(StringBuilder stderr, String... args) throws IOException, InterruptedException {
        PrintStream originalErr = System.err;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setErr(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        try {
            return OsmPbfTransformation.run(args);
        } finally {
            System.setErr(originalErr);
            stderr.append(buffer.toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void main_helpFlag_noExceptionThrown() {
        String[] args = {"-help"};
        // -help prints usage and returns without launching the pipeline
        assertDoesNotThrow(() -> OsmPbfTransformation.main(args));
    }

    @Test
    void run_helpFlags_exitCodeZero() throws Exception {
        StringBuilder stderr = new StringBuilder();
        assertEquals(0, run(stderr, "-help"));
        assertEquals(0, run(stderr, "--help"));
        assertEquals("", stderr.toString());
    }

    @Test
    void run_unknownOption_invalidArgumentsExitCodeAndLoaderPathHint() throws Exception {
        StringBuilder stderr = new StringBuilder();
        assertEquals(OsmPbfTransformation.INVALID_ARGUMENTS_EXIT_CODE,
                run(stderr, "-source_pbf", "a.osm.pbf", "-poi_mapping_dir", "/organicmaps"));
        assertTrue(stderr.toString().contains("Unknown option: -poi_mapping_dir"), stderr.toString());
        assertTrue(stderr.toString().contains("-Dloader.path="), stderr.toString());
        assertTrue(stderr.toString().contains("Loaded extensions: test"), stderr.toString());
    }

    @Test
    void run_missingRequiredOption_invalidArgumentsExitCodeWithoutHint() throws Exception {
        StringBuilder stderr = new StringBuilder();
        assertEquals(OsmPbfTransformation.INVALID_ARGUMENTS_EXIT_CODE, run(stderr, "-worker_threads", "2"));
        assertTrue(stderr.toString().contains("-source_pbf"), stderr.toString());
        assertFalse(stderr.toString().contains("loader.path"), stderr.toString());
    }

    @Test
    void run_extensionOptionWithExtensionOnClasspath_accepted() {
        StringBuilder stderr = new StringBuilder();
        String[] args = {"-source_pbf", new File(tempDir.toFile(), "missing.osm.pbf").getAbsolutePath(), "-test_tag_value", "v"};
        assertThrows(IllegalArgumentException.class, () -> run(stderr, args));
        assertEquals("", stderr.toString());
    }

    @Test
    void main_noTsvAndNoArrow_throwsIllegalArgument() throws IOException {
        File pbf = new File(tempDir.toFile(), "test.osm.pbf");
        pbf.createNewFile();
        String[] args = {"-source_pbf", pbf.getAbsolutePath(), "-result_in_tsv", "false"};

        // -result_in_tsv false with no arrow_format means neither output enabled
        assertThrows(IllegalArgumentException.class, () -> OsmPbfTransformation.main(args));
    }

    @Test
    void run_pgJdbcUrlWithArrowOrStatisticsOnly_throwsIllegalArgument() {
        StringBuilder stderr = new StringBuilder();
        String url = "jdbc:postgresql://localhost:5432/osmworld";

        assertThrows(IllegalArgumentException.class,
                () -> run(stderr, "-source_pbf", "a.osm.pbf", "-pg_jdbc_url", url, "-arrow_format", "PARQUET"));
        assertThrows(IllegalArgumentException.class,
                () -> run(stderr, "-source_pbf", "a.osm.pbf", "-pg_jdbc_url", url, "-collect_only_statistics"));
    }

    @Test
    void run_incompatibleUpdateOptions_invalidArgumentsExitCode() throws Exception {
        String url = "jdbc:postgresql://localhost:5432/osmworld";
        StringBuilder columnar = new StringBuilder();
        StringBuilder updateWithoutJdbc = new StringBuilder();
        StringBuilder updateWithArrow = new StringBuilder();
        StringBuilder updateStatisticsOnly = new StringBuilder();

        assertEquals(OsmPbfTransformation.INVALID_ARGUMENTS_EXIT_CODE,
                run(columnar, "-source_pbf", "a.osm.pbf", "-updatable", "-columnar_storage"));
        assertEquals(OsmPbfTransformation.INVALID_ARGUMENTS_EXIT_CODE,
                run(new StringBuilder(), "-source_pbf", "a.osm.pbf", "-update", "-pg_jdbc_url", url, "-columnar_storage"));
        assertEquals(OsmPbfTransformation.INVALID_ARGUMENTS_EXIT_CODE,
                run(updateWithoutJdbc, "-source_pbf", "a.osm.pbf", "-update"));
        assertEquals(OsmPbfTransformation.INVALID_ARGUMENTS_EXIT_CODE,
                run(updateWithArrow, "-source_pbf", "a.osm.pbf", "-update", "-result_in_tsv", "false",
                        "-arrow_format", "PARQUET"));
        assertEquals(OsmPbfTransformation.INVALID_ARGUMENTS_EXIT_CODE,
                run(updateStatisticsOnly, "-source_pbf", "a.osm.pbf", "-update", "-collect_only_statistics"));

        assertTrue(columnar.toString().contains("can't be combined with -columnar_storage"), columnar.toString());
        assertTrue(updateWithoutJdbc.toString().contains("-update requires -pg_jdbc_url"), updateWithoutJdbc.toString());
        assertTrue(updateWithArrow.toString().contains("-update requires -pg_jdbc_url"), updateWithArrow.toString());
        assertTrue(updateStatisticsOnly.toString().contains("-update requires -pg_jdbc_url"), updateStatisticsOnly.toString());
    }

    @Test
    void optionsError_geoParquetVersionWithoutParquet_rejected() {
        CliParameters ipc = new CliParameters();
        JCommander.newBuilder().addObject(ipc).build().parse("-source_pbf", "a.osm.pbf", "-arrow_format", "ARROW_IPC",
                "-geoparquet_version", "V2");
        CliParameters tsv = new CliParameters();
        JCommander.newBuilder().addObject(tsv).build().parse("-source_pbf", "a.osm.pbf", "-geoparquet_version", "BLOB");
        CliParameters parquet = new CliParameters();
        JCommander.newBuilder().addObject(parquet).build().parse("-source_pbf", "a.osm.pbf", "-arrow_format", "PARQUET",
                "-geoparquet_version", "V2");
        CliParameters update = new CliParameters();
        JCommander.newBuilder().addObject(update).build().parse("-source_pbf", "a.osm.pbf", "-update",
                "-geoparquet_version", "V2");

        assertEquals("-geoparquet_version requires -arrow_format PARQUET", OsmPbfTransformation.optionsError(ipc));
        assertEquals("-geoparquet_version requires -arrow_format PARQUET", OsmPbfTransformation.optionsError(tsv));
        assertNull(OsmPbfTransformation.optionsError(parquet));
        assertEquals("-update requires -pg_jdbc_url", OsmPbfTransformation.optionsError(update));
    }

    @Test
    void optionsError_parquetCompression_requiresParquet() {
        assertEquals("-parquet_compression requires -arrow_format PARQUET",
                OsmPbfTransformation.optionsError(parse("-parquet_compression", "SNAPPY")));
        assertEquals("-parquet_compression requires -arrow_format PARQUET",
                OsmPbfTransformation.optionsError(parse("-arrow_format", "ARROW_IPC", "-parquet_compression", "LZ4")));
        assertNull(OsmPbfTransformation.optionsError(parse("-arrow_format", "PARQUET", "-parquet_compression",
                "SNAPPY")));
    }

    @Test
    void optionsError_arrowIpcCompression_requiresArrowIpc() {
        assertEquals("-arrow_ipc_compression requires -arrow_format ARROW_IPC",
                OsmPbfTransformation.optionsError(parse("-arrow_ipc_compression", "ZSTD")));
        assertEquals("-arrow_ipc_compression requires -arrow_format ARROW_IPC",
                OsmPbfTransformation.optionsError(parse("-arrow_format", "PARQUET", "-arrow_ipc_compression", "ZSTD")));
        assertNull(OsmPbfTransformation.optionsError(parse("-arrow_format", "ARROW_IPC", "-arrow_ipc_compression",
                "ZSTD")));
    }

    @Test
    void optionsError_spillOptions_validated() {
        assertEquals("-spill_compression and -spill_zstd_level require -arrow_format",
                OsmPbfTransformation.optionsError(parse("-spill_compression", "NONE")));
        assertEquals("-spill_compression and -spill_zstd_level require -arrow_format",
                OsmPbfTransformation.optionsError(parse("-spill_zstd_level", "3")));
        assertEquals("-spill_zstd_level requires -spill_compression ZSTD", OsmPbfTransformation.optionsError(
                parse("-arrow_format", "PARQUET", "-spill_compression", "NONE", "-spill_zstd_level", "3")));
        assertEquals("-spill_zstd_level must be between -131072 and 22", OsmPbfTransformation.optionsError(
                parse("-arrow_format", "PARQUET", "-spill_zstd_level", "23")));
        assertEquals("-spill_zstd_level must be between -131072 and 22", OsmPbfTransformation.optionsError(
                parse("-arrow_format", "ARROW_IPC", "-spill_zstd_level", "-131073")));
        assertNull(OsmPbfTransformation.optionsError(parse("-arrow_format", "PARQUET", "-spill_compression", "NONE")));
        assertNull(OsmPbfTransformation.optionsError(parse("-arrow_format", "ARROW_IPC", "-spill_zstd_level", "-7")));
        assertNull(OsmPbfTransformation.optionsError(parse("-arrow_format", "PARQUET", "-spill_compression", "ZSTD",
                "-spill_zstd_level", "22")));
        assertNull(OsmPbfTransformation.optionsError(parse()));
    }

    private static CliParameters parse(String... options) {
        CliParameters parameters = new CliParameters();
        String[] args = new String[options.length + 2];
        args[0] = "-source_pbf";
        args[1] = "a.osm.pbf";
        System.arraycopy(options, 0, args, 2, options.length);
        JCommander.newBuilder().addObject(parameters).build().parse(args);
        return parameters;
    }

    @Test
    void run_geoParquetVersionWithArrowIpc_invalidArgumentsExitCode() throws Exception {
        StringBuilder stderr = new StringBuilder();

        assertEquals(OsmPbfTransformation.INVALID_ARGUMENTS_EXIT_CODE, run(stderr, "-source_pbf", "a.osm.pbf",
                "-arrow_format", "ARROW_IPC", "-geoparquet_version", "V1"));

        assertTrue(stderr.toString().contains("-geoparquet_version requires -arrow_format PARQUET"), stderr.toString());
    }

    @Test
    void updateOptionsError_updateWithJdbcAndArrowOrStatistics_rejected() {
        CliParameters arrow = new CliParameters();
        JCommander.newBuilder().addObject(arrow).build().parse("-source_pbf", "a.osm.pbf", "-update",
                "-pg_jdbc_url", "jdbc:postgresql://localhost/osmworld", "-arrow_format", "PARQUET");
        CliParameters statistics = new CliParameters();
        JCommander.newBuilder().addObject(statistics).build().parse("-source_pbf", "a.osm.pbf", "-update",
                "-pg_jdbc_url", "jdbc:postgresql://localhost/osmworld", "-collect_only_statistics");
        CliParameters valid = new CliParameters();
        JCommander.newBuilder().addObject(valid).build().parse("-source_pbf", "a.osm.pbf", "-updatable",
                "-arrow_format", "PARQUET");

        assertEquals("-update can't be combined with -arrow_format or -collect_only_statistics",
                OsmPbfTransformation.updateOptionsError(arrow));
        assertEquals("-update can't be combined with -arrow_format or -collect_only_statistics",
                OsmPbfTransformation.updateOptionsError(statistics));
        assertNull(OsmPbfTransformation.updateOptionsError(valid));
    }

    @Test
    void main_nonExistentSourcePbf_throwsIllegalArgument() {
        File nonExistent = new File(tempDir.toFile(), "missing.osm.pbf");
        String[] args = {"-source_pbf", nonExistent.getAbsolutePath(), "-result_in_tsv", "true"};

        assertThrows(IllegalArgumentException.class, () -> OsmPbfTransformation.main(args));
    }

    @Test
    void main_emptySourcePbf_throwsIllegalArgument() throws IOException {
        File emptyPbf = new File(tempDir.toFile(), "empty.osm.pbf");
        emptyPbf.createNewFile();
        String[] args = {"-source_pbf", emptyPbf.getAbsolutePath(), "-result_in_tsv", "true"};

        assertThrows(IllegalArgumentException.class, () -> OsmPbfTransformation.main(args));
    }
}
