package com.github.isuhorukov.osm.pgsnapshot;

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
