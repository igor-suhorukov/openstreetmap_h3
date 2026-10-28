package com.github.isuhorukov.osm.pgsnapshot;

import com.github.isuhorukov.osm.pgsnapshot.arrow.DuckDbParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ExternalProcessingTest {

    @TempDir
    Path tempDir;

    @Test
    void getIndexType_fileSmallEnough_returnsSparseMemArray() throws IOException {
        File smallFile = Files.createTempFile(tempDir, "small", ".pbf").toFile();
        new FileOutputStream(smallFile).close();

        assertEquals("sparse_mem_array", ExternalProcessing.getIndexType(smallFile));
    }

    @Test
    void getIndexType_fileLargerThanMaxMemory_returnsDenseFileArray() {
        File largeFile = new File(tempDir.toFile(), "large.pbf") {
            @Override
            public long length() {
                return Runtime.getRuntime().maxMemory() + 1;
            }
        };

        assertEquals("dense_file_array", ExternalProcessing.getIndexType(largeFile));
    }

    @Test
    void longestLineLength_lastLineWithoutNewline_counted() throws IOException {
        File file = tempDir.resolve("lines.tsv").toFile();
        Files.writeString(file.toPath(), "ab\nabcd\nabcdef");

        assertEquals(6, ExternalProcessing.longestLineLength(file));
    }

    @Test
    void csvLineSizeOptions_shortLines_duckDbDefaults() throws IOException {
        File file = tempDir.resolve("short.tsv").toFile();
        Files.writeString(file.toPath(), "0103000020E6100000\tway\t7\t\"building\"=>\"yes\"\n");

        assertEquals("max_line_size=2000000,buffer_size=32000000", ExternalProcessing.csvLineSizeOptions(file));
    }

    @Test
    void transformMultipolygonToParquet_lineLongerThanDuckDbDefaultLimit_converted() throws Exception {
        File resultDir = tempDir.toFile();
        new File(resultDir, "multipolygon").mkdirs();
        new File(resultDir, "arrow").mkdirs();
        String bigMultipolygonHex = "AB".repeat(1_500_000);
        Files.writeString(new File(resultDir, "multipolygon/source.tsv").toPath(),
                "0103000020E6100000\tway\t7\t\"building\"=>\"yes\"\n" +
                        bigMultipolygonHex + "\trelation\t42\t\"landuse\"=>\"forest\"\n");

        try (DuckDbParquetWriter parquetWriter = DuckDbParquetWriter.open()) {
            ExternalProcessing.transformMultipolygonToParquet(resultDir, parquetWriter);
        }

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT id, octet_length(wkb), tags_json FROM read_parquet('"
                     + new File(resultDir, "arrow/multipolygon.parquet").getAbsolutePath() + "')")) {
            assertTrue(resultSet.next());
            assertEquals(42, resultSet.getLong(1));
            assertEquals(1_500_000, resultSet.getLong(2));
            assertEquals("{\"landuse\":\"forest\"}", resultSet.getString(3));
            assertFalse(resultSet.next());
        }
    }

    @Test
    void addLocationsToWaysCommand_localOsmium_writesEnrichedPbfToStdout() {
        File sourcePbf = new File(tempDir.toFile(), "test.osm.pbf");

        List<String> command = new ExternalProcessing(false).addLocationsToWaysCommand(sourcePbf, false);

        assertEquals(List.of("/usr/bin/osmium", "add-locations-to-ways", "test.osm.pbf", "-v",
                "--output-format", "pbf,pbf_compression=none", "--keep-member-nodes",
                "-i", "sparse_mem_array", "-o", "-"), command);
    }

    @Test
    void addLocationsToWaysCommand_preserveAllNodes_keepsUntaggedNodes() {
        File sourcePbf = new File(tempDir.toFile(), "test.osm.pbf");

        List<String> command = new ExternalProcessing(false).addLocationsToWaysCommand(sourcePbf, true);

        assertTrue(command.contains("--keep-untagged-nodes"));
        assertFalse(command.contains("--keep-member-nodes"));
    }

    @Test
    void addLocationsToWaysCommand_docker_disablesContainerLogForStreamedOutput() {
        File sourcePbf = new File(tempDir.toFile(), "test.osm.pbf");
        String basePath = tempDir.toFile().getPath();

        List<String> command = new ExternalProcessing(true).addLocationsToWaysCommand(sourcePbf, false);

        assertEquals(List.of("docker", "run", "--rm", "--log-driver", "none", "-w", basePath,
                "-v", basePath + ":" + basePath, "mschilde/osmium-tool", "/usr/bin/osmium"), command.subList(0, 11));
        assertEquals(List.of("-o", "-"), command.subList(command.size() - 2, command.size()));
    }

    @Test
    void multipolygonExportCommand_localOsmium_configFromStdinPolygonsToStdout() {
        File sourcePbf = new File(tempDir.toFile(), "test.osm.pbf");

        List<String> command = new ExternalProcessing(false).multipolygonExportCommand(sourcePbf);

        assertEquals(List.of("/usr/bin/osmium", "export", "-e", "--config=/dev/stdin", "-i", "sparse_mem_array",
                "--geometry-types", "polygon", "-v", "-f", "pg", "-x", "tags_type=hstore", "test.osm.pbf",
                "-o", "-"), command);
    }

    @Test
    void multipolygonExportCommand_docker_interactiveForConfigOnStdin() {
        File sourcePbf = new File(tempDir.toFile(), "test.osm.pbf");
        String basePath = tempDir.toFile().getPath();

        List<String> command = new ExternalProcessing(true).multipolygonExportCommand(sourcePbf);

        assertEquals(List.of("docker", "run", "-i", "--rm", "--log-driver", "none", "-w", basePath,
                "-v", basePath + ":" + basePath, "mschilde/osmium-tool", "/usr/bin/osmium", "export"),
                command.subList(0, 13));
    }

    @Test
    void extractRelationsCommand_localOsmium_idsFromStdinWithReferencedObjects() {
        File sourcePbf = new File(tempDir.toFile(), "test.osm.pbf");
        File output = new File(tempDir.toFile(), "subset/relations.osm.pbf");

        List<String> command = new ExternalProcessing(false).extractRelationsCommand(sourcePbf, output);

        assertEquals(List.of("/usr/bin/osmium", "getid", "-r", "--overwrite", "-i", "-", "-v", "-f", "pbf",
                sourcePbf.getAbsolutePath(), "-o", output.getAbsolutePath()), command);
    }

    @Test
    void extractRelationsCommand_docker_mountsOutputDirectory() {
        File sourcePbf = new File(tempDir.toFile(), "test.osm.pbf");
        File output = new File(tempDir.toFile(), "subset/relations.osm.pbf");
        String basePath = tempDir.toFile().getAbsolutePath();
        String outputPath = output.getParentFile().getAbsolutePath();

        List<String> command = new ExternalProcessing(true).extractRelationsCommand(sourcePbf, output);

        assertEquals(List.of("docker", "run", "-i", "--rm", "--log-driver", "none", "-w", basePath,
                "-v", basePath + ":" + basePath, "-v", outputPath + ":" + outputPath, "mschilde/osmium-tool",
                "/usr/bin/osmium", "getid"), command.subList(0, 15));
    }

    @Test
    void extractRelationsCommand_dockerOutputInSourceDirectory_mountedOnce() {
        File sourcePbf = new File(tempDir.toFile(), "test.osm.pbf");
        File output = new File(tempDir.toFile(), "relations.osm.pbf");

        List<String> command = new ExternalProcessing(true).extractRelationsCommand(sourcePbf, output);

        String mount = tempDir.toFile().getAbsolutePath() + ":" + tempDir.toFile().getAbsolutePath();
        assertEquals(1, command.stream().filter(mount::equals).count());
    }

    @Test
    void extractRelations_missingRelationsOfExtract_acceptedLikeOsmiumExitCodeOne() throws Exception {
        assumeTrue(Files.isExecutable(Paths.get("/usr/bin/osmium")), "/usr/bin/osmium is required");
        File sourceOsm = new File(tempDir.toFile(), "sample.osm");
        try (java.io.InputStream sample = getClass().getResourceAsStream("/pipeline/sample.osm")) {
            Files.copy(java.util.Objects.requireNonNull(sample), sourceOsm.toPath());
        }
        File output = new File(tempDir.toFile(), "relations.osm.pbf");

        new ExternalProcessing(false).extractRelations(sourceOsm, List.of(201L, 999L), output);

        assertTrue(output.length() > 0);
    }

    @Test
    void generateMultipolygonCopyScripts_createsOneSqlFilePerPart() throws IOException {
        File resultDir = tempDir.toFile();
        File multipolygonDir = new File(resultDir, "multipolygon");
        multipolygonDir.mkdirs();
        File sqlDir = new File(resultDir, "sql");
        sqlDir.mkdirs();

        new File(multipolygonDir, "multipolygon_aa").createNewFile();
        new File(multipolygonDir, "multipolygon_ab").createNewFile();

        ExternalProcessing.generateMultipolygonCopyScripts(resultDir.getAbsolutePath());

        File[] sqlFiles = sqlDir.listFiles((d, name) -> name.startsWith("y_multipoly_"));
        assertNotNull(sqlFiles);
        assertEquals(2, sqlFiles.length);
        for (File sqlFile : sqlFiles) {
            String content = Files.readString(sqlFile.toPath());
            assertTrue(content.contains("copy preimport_multipolygon FROM"));
            assertTrue(content.contains("\\timing on"));
        }
    }

    @Test
    void splitMultipolygonsAndWriteScripts_relationsSplitIntoPartsWithCopyScripts() throws Exception {
        File resultDir = tempDir.toFile();
        new File(resultDir, "multipolygon").mkdirs();
        new File(resultDir, "sql").mkdirs();
        File source = new File(resultDir, "multipolygon/source.tsv");
        Files.writeString(source.toPath(), "01\tway\t1\t\\N\n02\trelation\t2\t\\N\n03\trelation\t3\t\\N\n04\trelation\t4\t\\N\n");

        long splitTime = new ExternalProcessing(false).splitMultipolygonsAndWriteScripts(resultDir, 2, 3);

        assertTrue(splitTime >= 0);
        assertFalse(source.exists());
        assertEquals("02\trelation\t2\t\\N\n03\trelation\t3\t\\N\n",
                Files.readString(new File(resultDir, "multipolygon/multipolygon_aa").toPath()));
        assertEquals("04\trelation\t4\t\\N\n",
                Files.readString(new File(resultDir, "multipolygon/multipolygon_ab").toPath()));
        assertTrue(new File(resultDir, "sql/y_multipoly_aa.sql").exists());
        assertTrue(new File(resultDir, "sql/y_multipoly_ab.sql").exists());
    }

    @Test
    void runProcess_interrupted_destroysProcess() throws Exception {
        Process process = new ProcessBuilder("sleep", "60").start();
        AtomicReference<Exception> thrown = new AtomicReference<>();
        Thread runner = new Thread(() -> {
            try {
                ExternalProcessing.runProcess(process, "sleep 60");
            } catch (IOException | InterruptedException e) {
                thrown.set(e);
            }
        });
        runner.start();

        runner.interrupt();
        runner.join(10_000);

        assertFalse(runner.isAlive());
        assertInstanceOf(InterruptedException.class, thrown.get());
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
    }

    @Test
    void generateMultipolygonCopyScripts_sqlContentIsCorrect() throws IOException {
        File resultDir = tempDir.toFile();
        new File(resultDir, "multipolygon").mkdirs();
        new File(resultDir, "sql").mkdirs();
        new File(resultDir, "multipolygon/multipolygon_aa").createNewFile();

        ExternalProcessing.generateMultipolygonCopyScripts(resultDir.getAbsolutePath());

        File script = new File(resultDir, "sql/y_multipoly_aa.sql");
        assertTrue(script.exists());
        String content = Files.readString(script.toPath());
        assertTrue(content.contains("/input/multipolygon/multipolygon_aa"));
    }
}
