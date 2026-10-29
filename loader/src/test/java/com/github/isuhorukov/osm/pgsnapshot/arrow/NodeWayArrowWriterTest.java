package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.ArrowFormat;
import com.github.isuhorukov.osm.pgsnapshot.GeoParquetVersion;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class NodeWayArrowWriterTest {

    @TempDir
    Path tempDir;

    private DuckDbParquetWriter parquetWriter;

    @BeforeEach
    void openParquetWriter() throws IOException {
        parquetWriter = DuckDbParquetWriter.open(GeoParquetVersion.V1, Set.of(ArrowSchemas.LINE_STRING_WKB));
    }

    @AfterEach
    void closeParquetWriter() throws IOException {
        parquetWriter.close();
    }

    private File prepareArrowDir() {
        File resultDir = tempDir.toFile();
        ResultLayout.prepareResultDirectories(resultDir, false, true);
        return new File(resultDir, ResultLayout.ARROW_DIR);
    }

    @Test
    void write_arrowIpcFormat_createsArrowFile() {
        File arrowDir = prepareArrowDir();

        ArrowNodeOrWay node = new ArrowNodeOrWay(1L, (short) 100, 200, 4.0, 73.0,
                List.of(new Tag("amenity", "restaurant")));
        new NodeWayArrowWriter(arrowDir, ArrowFormat.ARROW_IPC, parquetWriter).write(List.of(node), 0L);

        File nodesDir = new File(arrowDir, ResultLayout.NODES_DIR);
        File[] files = nodesDir.listFiles((d, name) -> name.endsWith(".arrow"));
        assertNotNull(files);
        assertEquals(1, files.length);
        assertTrue(files[0].length() > 0);
    }

    @Test
    void write_parquetFormat_createsParquetFile() {
        File arrowDir = prepareArrowDir();

        ArrowNodeOrWay node = new ArrowNodeOrWay(1L, (short) 100, 200, 4.0, 73.0, Collections.emptyList());
        new NodeWayArrowWriter(arrowDir, ArrowFormat.PARQUET, parquetWriter).write(List.of(node), 0L);

        File nodesDir = new File(arrowDir, ResultLayout.NODES_DIR);
        File[] files = nodesDir.listFiles((d, name) -> name.endsWith(".parquet"));
        assertNotNull(files);
        assertEquals(1, files.length);
        assertTrue(files[0].length() > 0);
    }

    @Test
    void write_nodeWithEmptyTags_writesNullTagMap() {
        File arrowDir = prepareArrowDir();

        ArrowNodeOrWay node = new ArrowNodeOrWay(2L, (short) 100, 200, 4.0, 73.0, Collections.emptyList());
        assertDoesNotThrow(() ->
                new NodeWayArrowWriter(arrowDir, ArrowFormat.ARROW_IPC, parquetWriter).write(List.of(node), 1L));
    }

    @Test
    void write_wayWithNullH38Indexes_handledAsNull() {
        File arrowDir = prepareArrowDir();

        // way: pointIdxs != null, h38Indexes = null
        ArrowNodeOrWay way = new ArrowNodeOrWay.Builder(3L, (short) 100, 200, 4.0, 73.0,
                List.of(new Tag("highway", "primary")))
                .pointIdxs(new long[]{1L, 2L})
                .lineStringWkb(new byte[]{0x01, 0x02})
                .bboxWkb(new byte[]{0x03, 0x04})
                .h38Indexes(null)
                .build();

        assertDoesNotThrow(() ->
                new NodeWayArrowWriter(arrowDir, ArrowFormat.ARROW_IPC, parquetWriter).write(List.of(way), 2L));
    }

    @Test
    void write_emptyList_noFileCreated() {
        File arrowDir = prepareArrowDir();

        new NodeWayArrowWriter(arrowDir, ArrowFormat.PARQUET, parquetWriter).write(Collections.emptyList(), 0L);

        File nodesDir = new File(arrowDir, ResultLayout.NODES_DIR);
        File[] files = nodesDir.listFiles();
        assertTrue(files == null || files.length == 0);
    }

    @Test
    void write_nullList_noFileCreated() {
        File arrowDir = prepareArrowDir();

        new NodeWayArrowWriter(arrowDir, ArrowFormat.PARQUET, parquetWriter).write(null, 0L);

        File nodesDir = new File(arrowDir, ResultLayout.NODES_DIR);
        File[] files = nodesDir.listFiles();
        assertTrue(files == null || files.length == 0);
    }

    @Test
    void write_parquetWriterFails_throwsWithBlockNumber() throws IOException {
        File arrowDir = prepareArrowDir();
        parquetWriter.close();
        NodeWayArrowWriter writer = new NodeWayArrowWriter(arrowDir, ArrowFormat.PARQUET, parquetWriter);
        List<ArrowNodeOrWay> nodes = List.of(new ArrowNodeOrWay(1L, (short) 100, 200, 4.0, 73.0,
                Collections.emptyList()));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> writer.write(nodes, 5L));

        assertEquals("Writing Arrow block 5 failed", exception.getMessage());
        assertInstanceOf(IOException.class, exception.getCause());
    }
}
