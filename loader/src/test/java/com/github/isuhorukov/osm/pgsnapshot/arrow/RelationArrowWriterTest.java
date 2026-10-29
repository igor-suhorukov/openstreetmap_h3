package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelationMember;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
    void write_relation_createsArrowFile() {
        File arrowDir = prepareArrowDir();

        ArrowRelation relation = new ArrowRelation(2L, Map.of("natural", "water"));
        relation.getRelationMembers().add(new ArrowRelationMember(5L, 'W', "outer"));

        new RelationArrowWriter(arrowDir).write(List.of(relation), 0L);

        File relationsDir = new File(arrowDir, ResultLayout.RELATIONS_DIR);
        File[] files = relationsDir.listFiles((d, name) -> name.endsWith(".arrow"));
        assertNotNull(files);
        assertEquals(1, files.length);
        assertTrue(files[0].length() > 0);
    }

    @Test
    void write_relationWithNoMembers_nullableVectorsUsed() {
        File arrowDir = prepareArrowDir();

        ArrowRelation relation = new ArrowRelation(3L, Map.of("type", "route"));

        assertDoesNotThrow(() -> new RelationArrowWriter(arrowDir).write(List.of(relation), 1L));

        File relationsDir = new File(arrowDir, ResultLayout.RELATIONS_DIR);
        File[] files = relationsDir.listFiles((d, name) -> name.endsWith(".arrow"));
        assertNotNull(files);
        assertEquals(1, files.length);
    }

    @Test
    void write_relationWithEmptyTags_writesNullTagMap() {
        File arrowDir = prepareArrowDir();

        ArrowRelation relation = new ArrowRelation(4L, Collections.emptyMap());
        relation.getRelationMembers().add(new ArrowRelationMember(1L, 'N', "label"));

        assertDoesNotThrow(() -> new RelationArrowWriter(arrowDir).write(List.of(relation), 2L));
    }

    @Test
    void write_emptyList_noFileCreated() {
        File arrowDir = prepareArrowDir();

        new RelationArrowWriter(arrowDir).write(Collections.emptyList(), 0L);

        File relationsDir = new File(arrowDir, ResultLayout.RELATIONS_DIR);
        File[] files = relationsDir.listFiles();
        assertTrue(files == null || files.length == 0);
    }

    @Test
    void write_missingDirectory_throwsWithBlockNumber() throws IOException {
        File arrowDir = prepareArrowDir();
        Files.delete(new File(arrowDir, ResultLayout.RELATIONS_DIR).toPath());
        RelationArrowWriter writer = new RelationArrowWriter(arrowDir);
        List<ArrowRelation> relations = List.of(new ArrowRelation(6L, Map.of("type", "route")));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> writer.write(relations, 7L));

        assertEquals("Writing Arrow block 7 failed", exception.getMessage());
        assertInstanceOf(IOException.class, exception.getCause());
    }
}
