package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.IpcCompression;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ArrowIpcWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void write_noneAndZstd_sameRowsSmallerZstdFile() throws IOException {
        File spill = spill(2000);
        File plain = tempDir.resolve("plain.arrow").toFile();
        File zstd = tempDir.resolve("zstd.arrow").toFile();

        write(new ArrowIpcWriter(IpcCompression.NONE, Set.of()), spill, plain);
        write(new ArrowIpcWriter(IpcCompression.ZSTD, Set.of()), spill, zstd);

        assertEquals(ids(plain), ids(zstd));
        assertEquals(2000, ids(plain).size());
        assertTrue(zstd.length() * 4 < plain.length(), zstd.length() + " vs " + plain.length());
        assertEquals(".arrow", new ArrowIpcWriter(IpcCompression.NONE, Set.of()).fileExtension());
    }

    @Test
    void write_wkbGeometryColumn_geoArrowWkbWithCrs84() throws IOException {
        ArrowNodeOrWay way = new ArrowNodeOrWay.Builder(1L, (short) 5, 200, 4.0, 73.0, List.of(new Tag("highway", "primary")))
                .pointIdxs(new long[]{1L, 2L}).lineStringWkb(new byte[]{1, 2}).bboxWkb(new byte[]{3}).build();
        File spill = tempDir.resolve("way.arrows").toFile();
        Files.write(spill.toPath(), ArrowSpill.of(IpcCompression.NONE, 1).encode(List.of(way), List.of(), 0)
                .get((short) 5).getBytes());
        File target = tempDir.resolve("ways.arrow").toFile();

        try (BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = new CellBatchReader(allocator, ArrowSchemas.getNodesOrWaysSchema(true),
                     List.of(spill), CellBatchReader.MAX_BATCH_ROWS)) {
            new ArrowIpcWriter(IpcCompression.NONE, Set.of(ArrowSchemas.LINE_STRING_WKB, "id"))
                    .write(allocator, reader, target);
            assertTrue(reader.getVectorSchemaRoot().getSchema().findField(ArrowSchemas.LINE_STRING_WKB)
                    .getMetadata().isEmpty());
        }

        try (BufferAllocator allocator = new RootAllocator();
             FileInputStream input = new FileInputStream(target);
             ArrowFileReader reader = new ArrowFileReader(input.getChannel(), allocator)) {
            Field geometry = reader.getVectorSchemaRoot().getSchema().findField(ArrowSchemas.LINE_STRING_WKB);
            assertEquals(Map.of("ARROW:extension:name", "geoarrow.wkb",
                    "ARROW:extension:metadata", "{\"crs\":\"OGC:CRS84\",\"crs_type\":\"authority_code\"}"),
                    geometry.getMetadata());
            assertTrue(reader.getVectorSchemaRoot().getSchema().findField("bboxWkb").getMetadata().isEmpty());
            assertTrue(reader.getVectorSchemaRoot().getSchema().findField("id").getMetadata().isEmpty());
            assertTrue(reader.loadNextBatch());
            assertArrayEquals(new byte[]{1, 2},
                    (byte[]) reader.getVectorSchemaRoot().getVector(ArrowSchemas.LINE_STRING_WKB).getObject(0));
        }
    }

    @Test
    void write_batches_statisticsInFooterMetadata() throws IOException {
        File spill = spill(5);
        File target = tempDir.resolve("statistics.arrow").toFile();
        try (BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = new CellBatchReader(allocator, ArrowSchemas.getNodesOrWaysSchema(false),
                     List.of(spill), 3)) {
            new ArrowIpcWriter(IpcCompression.ZSTD, Set.of()).write(allocator, reader, target);
        }

        try (BufferAllocator allocator = new RootAllocator();
             FileInputStream input = new FileInputStream(target);
             ArrowFileReader reader = new ArrowFileReader(input.getChannel(), allocator,
                     CommonsCompressionFactory.INSTANCE)) {
            reader.getVectorSchemaRoot();
            assertEquals(Map.of(BatchStatistics.METADATA_KEY, "{\"version\":1,\"batches\":["
                    + "{\"rows\":5,\"id\":[1,5],\"h33\":[5,5],\"h38\":[200,200]}]}"), reader.getMetaData());
        }
    }

    @Test
    void write_orderBy_rejected() throws IOException {
        File spill = spill(1);
        File target = tempDir.resolve("ordered.arrow").toFile();
        ArrowIpcWriter writer = new ArrowIpcWriter(IpcCompression.NONE, Set.of());
        List<String> orderBy = List.of("id");

        try (BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = ArrowSpill.reader(allocator, ArrowSchemas.getNodesOrWaysSchema(false), List.of(spill))) {
            assertThrows(IllegalArgumentException.class, () -> writer.write(allocator, reader, target, orderBy));
        }
    }

    private File spill(int rows) throws IOException {
        List<ArrowNodeOrWay> nodes = new ArrayList<>();
        for (int id = 1; id <= rows; id++) {
            nodes.add(new ArrowNodeOrWay(id, (short) 5, 200, 4.0, 73.0, List.of(new Tag("amenity", "cafe"))));
        }
        File file = tempDir.resolve("cell.arrows").toFile();
        Files.write(file.toPath(), ArrowSpill.of(IpcCompression.NONE, 1).encode(nodes, List.of(), 0)
                .get((short) 5).getBytes());
        return file;
    }

    private static void write(ArrowIpcWriter writer, File spill, File target) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = new CellBatchReader(allocator, ArrowSchemas.getNodesOrWaysSchema(false),
                     List.of(spill), CellBatchReader.MAX_BATCH_ROWS)) {
            writer.write(allocator, reader, target);
        }
    }

    private static List<Long> ids(File file) throws IOException {
        List<Long> ids = new ArrayList<>();
        try (BufferAllocator allocator = new RootAllocator();
             FileInputStream input = new FileInputStream(file);
             ArrowFileReader reader = new ArrowFileReader(input.getChannel(), allocator,
                     CommonsCompressionFactory.INSTANCE)) {
            while (reader.loadNextBatch()) {
                BigIntVector id = (BigIntVector) reader.getVectorSchemaRoot().getVector("id");
                for (int row = 0; row < id.getValueCount(); row++) {
                    ids.add(id.get(row));
                }
            }
        }
        return ids;
    }
}
