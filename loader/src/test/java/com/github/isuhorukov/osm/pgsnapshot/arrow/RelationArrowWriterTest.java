package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.ArrowFormat;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelationMember;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RelationArrowWriterTest {

    @TempDir
    Path tempDir;

    private File prepareArrowDir() {
        File resultDir = tempDir.toFile();
        ResultLayout.prepareResultDirectories(resultDir, false, true);
        return new File(resultDir, ResultLayout.ARROW_DIR);
    }

    @Test
    void write_parquetFormat_createsParquetFile() {
        File arrowDir = prepareArrowDir();

        ArrowRelation relation = new ArrowRelation(1L, Map.of("type", "multipolygon"));
        relation.getRelationMembers().add(new ArrowRelationMember(10L, 'W', "outer"));

        new RelationArrowWriter(arrowDir, ArrowFormat.PARQUET).write(List.of(relation), 0L);

        File relationsDir = new File(arrowDir, ResultLayout.RELATIONS_DIR);
        File[] files = relationsDir.listFiles((d, name) -> name.endsWith(".parquet"));
        assertNotNull(files);
        assertEquals(1, files.length);
        assertTrue(files[0].length() > 0);
    }

    @Test
    void write_arrowIpcFormat_createsArrowFile() {
        File arrowDir = prepareArrowDir();

        ArrowRelation relation = new ArrowRelation(2L, Map.of("natural", "water"));
        relation.getRelationMembers().add(new ArrowRelationMember(5L, 'W', "outer"));

        new RelationArrowWriter(arrowDir, ArrowFormat.ARROW_IPC).write(List.of(relation), 0L);

        File relationsDir = new File(arrowDir, ResultLayout.RELATIONS_DIR);
        File[] files = relationsDir.listFiles((d, name) -> name.endsWith(".arrow"));
        assertNotNull(files);
        assertEquals(1, files.length);
        assertTrue(files[0].length() > 0);
    }

    @Test
    void write_relationWithNoMembers_nullableVectorsUsed() {
        File arrowDir = prepareArrowDir();

        // Empty members list covers the else branch in RelationArrowWriter
        ArrowRelation relation = new ArrowRelation(3L, Map.of("type", "route"));

        assertDoesNotThrow(() ->
                new RelationArrowWriter(arrowDir, ArrowFormat.PARQUET).write(List.of(relation), 1L));

        File relationsDir = new File(arrowDir, ResultLayout.RELATIONS_DIR);
        File[] files = relationsDir.listFiles((d, name) -> name.endsWith(".parquet"));
        assertNotNull(files);
        assertEquals(1, files.length);
    }

    @Test
    void write_relationWithEmptyTags_writesNullTagMap() {
        File arrowDir = prepareArrowDir();

        ArrowRelation relation = new ArrowRelation(4L, Collections.emptyMap());
        relation.getRelationMembers().add(new ArrowRelationMember(1L, 'N', "label"));

        assertDoesNotThrow(() ->
                new RelationArrowWriter(arrowDir, ArrowFormat.PARQUET).write(List.of(relation), 2L));
    }

    @Test
    void write_emptyList_noFileCreated() {
        File arrowDir = prepareArrowDir();

        new RelationArrowWriter(arrowDir, ArrowFormat.PARQUET).write(Collections.emptyList(), 0L);

        File relationsDir = new File(arrowDir, ResultLayout.RELATIONS_DIR);
        File[] files = relationsDir.listFiles();
        assertTrue(files == null || files.length == 0);
    }
}
