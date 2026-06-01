package com.github.isuhorukov.osm.pgsnapshot.output;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ResultLayoutTest {

    @TempDir
    Path tempDir;

    @Test
    void blockResultDirectory_nodes() {
        File dir = tempDir.toFile();
        File result = ResultLayout.blockResultDirectory(1, 0, 0, dir);
        assertEquals(new File(dir, "nodes"), result);
    }

    @Test
    void blockResultDirectory_ways() {
        File dir = tempDir.toFile();
        File result = ResultLayout.blockResultDirectory(0, 5, 0, dir);
        assertEquals(new File(dir, "ways"), result);
    }

    @Test
    void blockResultDirectory_relations() {
        File dir = tempDir.toFile();
        File result = ResultLayout.blockResultDirectory(0, 0, 3, dir);
        assertEquals(new File(dir, "relations"), result);
    }

    @Test
    void blockResultDirectory_allZero_throws() {
        File dir = tempDir.toFile();
        assertThrows(IllegalArgumentException.class,
                () -> ResultLayout.blockResultDirectory(0, 0, 0, dir));
    }

    @Test
    void prepareResultDirectories_noTsvNoArrow_createsBaseAndMetaAndStatic() {
        File resultDir = new File(tempDir.toFile(), "out");
        ResultLayout.prepareResultDirectories(resultDir, false, false);

        assertTrue(resultDir.exists());
        assertTrue(new File(resultDir, ResultLayout.IMPORT_RELATED_METADATA_DIR).exists());
        assertTrue(new File(resultDir, ResultLayout.STATIC_DIR).exists());
        assertFalse(new File(resultDir, ResultLayout.NODES_DIR).exists());
        assertFalse(new File(resultDir, ResultLayout.ARROW_DIR).exists());
    }

    @Test
    void prepareResultDirectories_withTsv_createsTsvDirs() {
        File resultDir = new File(tempDir.toFile(), "out");
        ResultLayout.prepareResultDirectories(resultDir, true, false);

        assertTrue(new File(resultDir, ResultLayout.NODES_DIR).exists());
        assertTrue(new File(resultDir, ResultLayout.WAYS_DIR).exists());
        assertTrue(new File(resultDir, ResultLayout.RELATIONS_DIR).exists());
        assertTrue(new File(resultDir, ResultLayout.MULTIPOLYGON_DIR).exists());
        assertTrue(new File(resultDir, ResultLayout.SQL_DIR).exists());
    }

    @Test
    void prepareResultDirectories_withArrow_createsArrowSubDirs() {
        File resultDir = new File(tempDir.toFile(), "out");
        ResultLayout.prepareResultDirectories(resultDir, false, true);

        File arrowDir = new File(resultDir, ResultLayout.ARROW_DIR);
        assertTrue(new File(arrowDir, ResultLayout.NODES_DIR).exists());
        assertTrue(new File(arrowDir, ResultLayout.WAYS_DIR).exists());
        assertTrue(new File(arrowDir, ResultLayout.RELATIONS_DIR).exists());
    }

    @Test
    void prepareResultDirectories_nonEmptyNodesDir_throws() throws IOException {
        File resultDir = new File(tempDir.toFile(), "out");
        resultDir.mkdir();
        File nodesDir = new File(resultDir, ResultLayout.NODES_DIR);
        nodesDir.mkdir();
        new File(nodesDir, "existing.tsv").createNewFile();

        assertThrows(IllegalArgumentException.class,
                () -> ResultLayout.prepareResultDirectories(resultDir, true, false));
    }

    @Test
    void prepareResultDirectories_nonEmptyWaysDir_throws() throws IOException {
        File resultDir = new File(tempDir.toFile(), "out");
        resultDir.mkdir();
        File waysDir = new File(resultDir, ResultLayout.WAYS_DIR);
        waysDir.mkdir();
        new File(waysDir, "existing.tsv").createNewFile();

        assertThrows(IllegalArgumentException.class,
                () -> ResultLayout.prepareResultDirectories(resultDir, true, false));
    }

    @Test
    void prepareResultDirectories_nonEmptyRelationsDir_throws() throws IOException {
        File resultDir = new File(tempDir.toFile(), "out");
        resultDir.mkdir();
        File relDir = new File(resultDir, ResultLayout.RELATIONS_DIR);
        relDir.mkdir();
        new File(relDir, "existing.tsv").createNewFile();

        assertThrows(IllegalArgumentException.class,
                () -> ResultLayout.prepareResultDirectories(resultDir, true, false));
    }

    @Test
    void checkAndMakeMultipolygonDirectory_nonEmpty_throws() throws IOException {
        File resultDir = new File(tempDir.toFile(), "out");
        resultDir.mkdir();
        File mpDir = new File(resultDir, ResultLayout.MULTIPOLYGON_DIR);
        mpDir.mkdir();
        new File(mpDir, "existing.tsv").createNewFile();

        assertThrows(IllegalArgumentException.class,
                () -> ResultLayout.checkAndMakeMultipolygonDirectory(resultDir));
    }

    @Test
    void checkAndMakeMultipolygonDirectory_notExisting_createsDir() {
        File resultDir = new File(tempDir.toFile(), "out");
        resultDir.mkdir();
        File mp = ResultLayout.checkAndMakeMultipolygonDirectory(resultDir);
        assertTrue(mp.exists());
        assertEquals(ResultLayout.MULTIPOLYGON_DIR, mp.getName());
    }

    @Test
    void resultDirectoryNameFromSource_stripsBlocksSuffix() {
        File f = new File("/some/path/maldives_blocks");
        assertEquals("maldives", ResultLayout.resultDirectoryNameFromSource(f));
    }

    @Test
    void resultDirectoryNameFromSource_noSuffix_unchanged() {
        File f = new File("/some/path/maldives");
        assertEquals("maldives", ResultLayout.resultDirectoryNameFromSource(f));
    }
}
