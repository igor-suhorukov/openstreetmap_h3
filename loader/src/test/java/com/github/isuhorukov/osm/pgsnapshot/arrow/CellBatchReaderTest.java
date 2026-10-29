package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.IpcCompression;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CellBatchReaderTest {

    private static final ArrowSpill SPILL = ArrowSpill.of(IpcCompression.ZSTD, 1);

    @TempDir
    Path tempDir;

    @Test
    void loadNextBatch_chunksOfCells_coalescedUpToLimitWithinCell() throws IOException {
        File first = cell("first", 7, new long[][]{{1, 2}, {3, 4}, {5}});
        File empty = tempDir.resolve("empty.arrows").toFile();
        Files.write(empty.toPath(), new byte[0]);
        File second = cell("second", 9, new long[][]{{6}});

        List<String> batches = batches(List.of(first, empty, second), 3);

        assertEquals(List.of("7:1,2", "7:3,4,5", "9:6"), batches);
    }

    @Test
    void loadNextBatch_largeLimit_onePerCell() throws IOException {
        File first = cell("first", 7, new long[][]{{1, 2}, {3, 4}, {5}});
        File second = cell("second", 9, new long[][]{{6}, {8}});

        assertEquals(List.of("7:1,2,3,4,5", "9:6,8"), batches(List.of(first, second), CellBatchReader.MAX_BATCH_ROWS));
    }

    @Test
    void loadNextBatch_noFiles_noBatches() throws IOException {
        assertEquals(List.of(), batches(List.of(), 3));
    }

    @Test
    void close_midCell_releasesMemory() throws IOException {
        File first = cell("first", 7, new long[][]{{1, 2}, {3, 4}});
        try (BufferAllocator allocator = new RootAllocator()) {
            CellBatchReader reader = new CellBatchReader(allocator, ArrowSchemas.getNodesOrWaysSchema(false),
                    List.of(first), 2);
            assertTrue(reader.loadNextBatch());
            assertEquals(first.length(), reader.bytesRead());
            reader.close();
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    private File cell(String name, int h33, long[][] chunks) throws IOException {
        File file = tempDir.resolve(name + ".arrows").toFile();
        for (long[] chunk : chunks) {
            List<ArrowNodeOrWay> nodes = new ArrayList<>();
            for (long id : chunk) {
                nodes.add(new ArrowNodeOrWay(id, (short) h33, 200, 4.0, 73.0, List.of(new Tag("amenity", "cafe"))));
            }
            Files.write(file.toPath(), SPILL.encode(nodes, List.of(), 0).get((short) h33).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        return file;
    }

    private static List<String> batches(List<File> files, int maxRows) throws IOException {
        List<String> batches = new ArrayList<>();
        try (BufferAllocator allocator = new RootAllocator();
             CellBatchReader reader = new CellBatchReader(allocator, ArrowSchemas.getNodesOrWaysSchema(false), files,
                     maxRows)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                SmallIntVector h33 = (SmallIntVector) root.getVector(ArrowSchemas.H33);
                BigIntVector ids = (BigIntVector) root.getVector(ArrowSchemas.ID);
                List<String> values = new ArrayList<>();
                for (int row = 0; row < root.getRowCount(); row++) {
                    assertEquals(h33.get(0), h33.get(row));
                    values.add(String.valueOf(ids.get(row)));
                }
                batches.add(h33.get(0) + ":" + String.join(",", values));
            }
        }
        return batches;
    }
}
