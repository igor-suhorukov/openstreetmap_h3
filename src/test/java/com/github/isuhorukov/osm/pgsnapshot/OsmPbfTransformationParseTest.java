package com.github.isuhorukov.osm.pgsnapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class OsmPbfTransformationParseTest {

    @TempDir
    Path tempDir;

    @Test
    void main_helpFlag_noExceptionThrown() throws Exception {
        // -help prints usage and returns without launching the pipeline
        OsmPbfTransformation.main(new String[]{"-help"});
    }

    @Test
    void main_invalidArgument_noExceptionThrown() throws Exception {
        // Unknown arg triggers usage output and returns null from parseCliArguments
        OsmPbfTransformation.main(new String[]{"-unknown_arg"});
    }

    @Test
    void main_noTsvAndNoArrow_throwsIllegalArgument() throws Exception {
        File pbf = new File(tempDir.toFile(), "test.osm.pbf");
        pbf.createNewFile();

        // -result_in_tsv false with no arrow_format means neither output enabled
        assertThrows(IllegalArgumentException.class, () ->
                OsmPbfTransformation.main(new String[]{
                        "-source_pbf", pbf.getAbsolutePath(),
                        "-result_in_tsv", "false"
                }));
    }

    @Test
    void main_nonExistentSourcePbf_throwsIllegalArgument() throws Exception {
        File nonExistent = new File(tempDir.toFile(), "missing.osm.pbf");

        assertThrows(IllegalArgumentException.class, () ->
                OsmPbfTransformation.main(new String[]{
                        "-source_pbf", nonExistent.getAbsolutePath(),
                        "-result_in_tsv", "true"
                }));
    }

    @Test
    void main_emptySourcePbf_throwsIllegalArgument() throws Exception {
        File emptyPbf = new File(tempDir.toFile(), "empty.osm.pbf");
        emptyPbf.createNewFile();

        assertThrows(IllegalArgumentException.class, () ->
                OsmPbfTransformation.main(new String[]{
                        "-source_pbf", emptyPbf.getAbsolutePath(),
                        "-result_in_tsv", "true"
                }));
    }
}
