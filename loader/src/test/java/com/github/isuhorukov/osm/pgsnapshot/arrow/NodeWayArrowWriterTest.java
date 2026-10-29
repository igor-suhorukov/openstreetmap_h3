package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NodeWayArrowWriterTest {

    @TempDir
    Path tempDir;

    private File prepareArrowDir() {
        File resultDir = tempDir.toFile();
        ResultLayout.prepareResultDirectories(resultDir, false, true);
        return new File(resultDir, ResultLayout.ARROW_DIR);
    }

    @Test
    void write_node_createsArrowFile() {
        File arrowDir = prepareArrowDir();

        ArrowNodeOrWay node = new ArrowNodeOrWay(1L, (short) 100, 200, 4.0, 73.0,
                List.of(new Tag("amenity", "restaurant")));
        new NodeWayArrowWriter(arrowDir).write(List.of(node), 0L);

        File nodesDir = new File(arrowDir, ResultLayout.NODES_DIR);
        File[] files = nodesDir.listFiles((d, name) -> name.endsWith(".arrow"));
        assertNotNull(files);
        assertEquals(1, files.length);
        assertTrue(files[0].length() > 0);
    }

    @Test
    void write_nodeWithEmptyTags_writesNullTagMap() {
        File arrowDir = prepareArrowDir();

        ArrowNodeOrWay node = new ArrowNodeOrWay(2L, (short) 100, 200, 4.0, 73.0, Collections.emptyList());
        assertDoesNotThrow(() -> new NodeWayArrowWriter(arrowDir).write(List.of(node), 1L));
    }

    @Test
    void write_wayWithNullH38Indexes_handledAsNull() {
        File arrowDir = prepareArrowDir();

        ArrowNodeOrWay way = new ArrowNodeOrWay.Builder(3L, (short) 100, 200, 4.0, 73.0,
                List.of(new Tag("highway", "primary")))
                .pointIdxs(new long[]{1L, 2L})
                .lineStringWkb(new byte[]{0x01, 0x02})
                .bboxWkb(new byte[]{0x03, 0x04})
                .h38Indexes(null)
                .build();

        assertDoesNotThrow(() -> new NodeWayArrowWriter(arrowDir).write(List.of(way), 2L));
        assertTrue(new File(arrowDir, ResultLayout.WAYS_DIR + "/00000002.arrow").isFile());
    }

    @Test
    void write_emptyList_noFileCreated() {
        File arrowDir = prepareArrowDir();

        new NodeWayArrowWriter(arrowDir).write(Collections.emptyList(), 0L);

        File nodesDir = new File(arrowDir, ResultLayout.NODES_DIR);
        File[] files = nodesDir.listFiles();
        assertTrue(files == null || files.length == 0);
    }

    @Test
    void write_nullList_noFileCreated() {
        File arrowDir = prepareArrowDir();

        new NodeWayArrowWriter(arrowDir).write(null, 0L);

        File nodesDir = new File(arrowDir, ResultLayout.NODES_DIR);
        File[] files = nodesDir.listFiles();
        assertTrue(files == null || files.length == 0);
    }

    @Test
    void write_missingDirectory_throwsWithBlockNumber() throws IOException {
        File arrowDir = prepareArrowDir();
        Files.delete(new File(arrowDir, ResultLayout.NODES_DIR).toPath());
        NodeWayArrowWriter writer = new NodeWayArrowWriter(arrowDir);
        List<ArrowNodeOrWay> nodes = List.of(new ArrowNodeOrWay(1L, (short) 100, 200, 4.0, 73.0,
                Collections.emptyList()));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> writer.write(nodes, 5L));

        assertEquals("Writing Arrow block 5 failed", exception.getMessage());
        assertInstanceOf(IOException.class, exception.getCause());
    }
}
