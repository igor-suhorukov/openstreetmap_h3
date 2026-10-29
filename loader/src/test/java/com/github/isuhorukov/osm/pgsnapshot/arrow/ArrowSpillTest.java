package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.IpcCompression;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelationMember;
import com.github.isuhorukov.osm.pgsnapshot.output.SpillChunk;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ArrowSpillTest {

    private static final ArrowSpill ZSTD = ArrowSpill.of(IpcCompression.ZSTD, 1);
    private static final ArrowSpill NONE = ArrowSpill.of(IpcCompression.NONE, 1);

    @TempDir
    Path tempDir;

    @Test
    void encode_nodesInTwoCells_chunkPerCellWithIdRange() throws IOException {
        List<ArrowNodeOrWay> nodes = List.of(node(1, 5), node(2, -7), node(3, 5), node(8, -7), node(9, 5));

        Map<Short, SpillChunk> chunks = ZSTD.encode(nodes, Collections.emptyList(), 0);

        assertEquals(List.of((short) -7, (short) 5), new ArrayList<>(chunks.keySet()));
        assertEquals(2, chunks.get((short) -7).getFirstId());
        assertEquals(8, chunks.get((short) -7).getLastId());
        assertEquals(1, chunks.get((short) 5).getFirstId());
        assertEquals(9, chunks.get((short) 5).getLastId());
        assertEquals(List.of("1 5", "3 5", "9 5"), rows(List.of(spillFile("a", chunks.get((short) 5))), false));
    }

    @Test
    void reader_chunksOfSeveralFiles_rowsInFileAndChunkOrder() throws IOException {
        File first = spillFile("first", ZSTD.encode(List.of(node(1, 5), node(2, 5)), List.of(), 0).get((short) 5));
        Files.write(first.toPath(), ZSTD.encode(List.of(node(3, 5)), List.of(), 1).get((short) 5).getBytes(),
                java.nio.file.StandardOpenOption.APPEND);
        File second = spillFile("second", ZSTD.encode(List.of(node(4, 6)), List.of(), 2).get((short) 6));

        assertEquals(List.of("1 5", "2 5", "3 5", "4 6"), rows(List.of(first, second), false));
    }

    @Test
    void reader_emptySpillFileBetweenFiles_skipped() throws IOException {
        File first = spillFile("one", ZSTD.encode(List.of(node(1, 5)), List.of(), 0).get((short) 5));
        File empty = tempDir.resolve("empty.arrows").toFile();
        Files.write(empty.toPath(), new byte[0]);
        File last = spillFile("two", ZSTD.encode(List.of(node(2, 6)), List.of(), 1).get((short) 6));

        assertEquals(List.of("1 5", "2 6"), rows(List.of(first, empty, last), false));
        assertEquals(List.of(), rows(List.of(empty), false));
    }

    @Test
    void encode_ways_waySchema() throws IOException {
        ArrowNodeOrWay way = new ArrowNodeOrWay.Builder(11L, (short) 32767, 200, 4.0, 73.0,
                List.of(new Tag("highway", "primary")))
                .pointIdxs(new long[]{1L, 2L}).lineStringWkb(new byte[]{1}).bboxWkb(new byte[]{2}).build();

        Map<Short, SpillChunk> chunks = ZSTD.encode(List.of(way), Collections.emptyList(), 3);

        assertEquals(List.of("11 32767"), rows(List.of(spillFile("w", chunks.get((short) 32767))), true));
    }

    @Test
    void recordBatch_zstd_needsCompressionCodec() throws IOException {
        SpillChunk chunk = ZSTD.encode(List.of(node(1, 5), node(2, 5)), List.of(), 0).get((short) 5);

        try (BufferAllocator allocator = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(withSchema(chunk)), allocator)) {
            assertThrows(IllegalArgumentException.class, reader::loadNextBatch);
        }
    }

    @Test
    void recordBatch_none_readableWithoutCodecAndLarger() throws IOException {
        List<ArrowNodeOrWay> nodes = new ArrayList<>();
        for (int id = 1; id <= 1000; id++) {
            nodes.add(node(id, 5));
        }
        SpillChunk plain = NONE.encode(nodes, List.of(), 0).get((short) 5);
        SpillChunk compressed = ZSTD.encode(nodes, List.of(), 0).get((short) 5);

        try (BufferAllocator allocator = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(withSchema(plain)), allocator)) {
            assertTrue(reader.loadNextBatch());
            assertEquals(1000, reader.getVectorSchemaRoot().getRowCount());
        }
        assertTrue(compressed.getBytes().length * 4 < plain.getBytes().length,
                compressed.getBytes().length + " vs " + plain.getBytes().length);
        assertEquals(1000, rows(List.of(spillFile("plain", plain)), false).size());
    }

    @Test
    void of_zstdLevels_sameRows() throws IOException {
        List<ArrowNodeOrWay> nodes = List.of(node(1, 5), node(2, 5), node(3, 5));
        List<String> expected = List.of("1 5", "2 5", "3 5");

        for (int level : new int[]{-5, 1, 19}) {
            SpillChunk chunk = ArrowSpill.of(IpcCompression.ZSTD, level).encode(nodes, List.of(), 0).get((short) 5);
            assertEquals(expected, rows(List.of(spillFile("level" + level, chunk)), false));
        }
    }

    private static byte[] withSchema(SpillChunk chunk) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(ArrowSpill.schemaMessage(ArrowSchemas.getNodesOrWaysSchema(false)));
        stream.write(chunk.getBytes());
        return stream.toByteArray();
    }

    @Test
    void encode_idsDecreaseInCell_throwsUnsortedPbf() {
        List<ArrowNodeOrWay> nodes = List.of(node(5, 1), node(3, 2), node(4, 1));
        List<ArrowRelation> relations = List.of();

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> ZSTD.encode(nodes, relations, 0));

        assertEquals("nodes 4 in h3_3 cell 1 follows id 5: the Parquet export needs a PBF sorted by type and id "
                + "(osmium sort)", exception.getMessage());
    }

    @Test
    void encode_nodesAndWays_throwsMixedBlock() {
        ArrowNodeOrWay way = new ArrowNodeOrWay.Builder(2L, (short) 1, 200, 4.0, 73.0, List.of())
                .pointIdxs(new long[]{1L}).lineStringWkb(new byte[]{1}).bboxWkb(new byte[]{2}).build();
        List<ArrowNodeOrWay> items = List.of(node(1, 1), way);
        List<ArrowRelation> relations = List.of();

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> ZSTD.encode(items, relations, 4));

        assertEquals("Block 4 mixes nodes and ways", exception.getMessage());
    }

    @Test
    void encode_relations_singleChunkInCellZero() {
        ArrowRelation first = new ArrowRelation(10L, Map.of("type", "route"));
        first.getRelationMembers().add(new ArrowRelationMember(1L, 'W', "outer"));
        ArrowRelation second = new ArrowRelation(12L, Map.of());

        Map<Short, SpillChunk> chunks = ZSTD.encode(List.of(), List.of(first, second), 5);

        assertEquals(List.of((short) 0), new ArrayList<>(chunks.keySet()));
        assertEquals(10, chunks.get((short) 0).getFirstId());
        assertEquals(12, chunks.get((short) 0).getLastId());
    }

    @Test
    void encode_relationIdsDecrease_throwsUnsortedPbf() {
        List<ArrowRelation> relations = List.of(new ArrowRelation(12L, Map.of()), new ArrowRelation(10L, Map.of()));
        List<ArrowNodeOrWay> nodes = List.of();

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> ZSTD.encode(nodes, relations, 5));

        assertTrue(exception.getMessage().startsWith("relations 10 in h3_3 cell 0 follows id 12"));
    }

    @Test
    void encode_relationsAndNodes_throwsMixedBlock() {
        List<ArrowNodeOrWay> nodes = List.of(node(1, 1));
        List<ArrowRelation> relations = List.of(new ArrowRelation(1L, Map.of()));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> ZSTD.encode(nodes, relations, 6));

        assertEquals("Block 6 mixes relations with nodes or ways", exception.getMessage());
    }

    @Test
    void encode_empty_noChunks() {
        assertTrue(ZSTD.encode(List.of(), List.of(), 7).isEmpty());
    }

    private static ArrowNodeOrWay node(long id, int h33) {
        return new ArrowNodeOrWay(id, (short) h33, 200, 4.0, 73.0, List.of(new Tag("amenity", "cafe")));
    }

    private File spillFile(String name, SpillChunk chunk) throws IOException {
        File file = tempDir.resolve(name + ".arrows").toFile();
        Files.write(file.toPath(), chunk.getBytes());
        return file;
    }

    private static List<String> rows(List<File> files, boolean ways) throws IOException {
        List<String> rows = new ArrayList<>();
        try (BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = ArrowSpill.reader(allocator, ArrowSchemas.getNodesOrWaysSchema(ways), files)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                BigIntVector id = (BigIntVector) root.getVector("id");
                SmallIntVector h33 = (SmallIntVector) root.getVector("h33");
                for (int row = 0; row < root.getRowCount(); row++) {
                    rows.add(id.get(row) + " " + h33.get(row));
                }
            }
        }
        return rows;
    }
}
