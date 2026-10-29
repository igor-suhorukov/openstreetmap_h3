package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.GeoParquetVersion;
import com.github.isuhorukov.osm.pgsnapshot.SpillCompression;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import com.github.isuhorukov.osm.pgsnapshot.output.SpillChunk;
import com.github.isuhorukov.osm.pgsnapshot.util.CompactH3;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;
import com.uber.h3core.H3Core;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ParquetPartitionsTest {

    private static final List<Partition> PARTITIONS = List.of(
            new Partition(0, (short) PartitionSplitter.LOWER_H3_3_BOUND, (short) 0, 0),
            new Partition(1, (short) 0, (short) PartitionSplitter.HIGH_H3_3_BOUND, 0));
    private static final Schema DUCKDB_MULTIPOLYGON_SCHEMA = new Schema(List.of(
            Field.nullable(ArrowSchemas.ID, new ArrowType.Int(64, true)),
            Field.nullable(ArrowSchemas.WKB, new ArrowType.Binary()),
            Field.nullable(ArrowSchemas.TAGS_JSON, new ArrowType.Utf8())));
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final ArrowSpill SPILL = ArrowSpill.of(SpillCompression.ZSTD, 1);

    @TempDir
    Path tempDir;

    private File arrowDir;
    private DuckDbParquetWriter parquetWriter;
    private ExecutorService executor;

    @BeforeEach
    void prepare() throws IOException {
        ResultLayout.prepareResultDirectories(tempDir.toFile(), false, true);
        arrowDir = new File(tempDir.toFile(), ResultLayout.ARROW_DIR);
        parquetWriter = DuckDbParquetWriter.open(GeoParquetVersion.V1, Set.of(ArrowSchemas.LINE_STRING_WKB));
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void close() throws IOException {
        executor.shutdownNow();
        parquetWriter.close();
    }

    @Test
    void writeBlocks_spilledCells_filePerPartitionInH33AndIdOrder() throws Exception {
        spill(ResultLayout.NODES_DIR, SPILL.encode(List.of(node(1, 7), node(2, -9), node(3, 7), node(4, 25764)),
                List.of(), 0));
        spill(ResultLayout.NODES_DIR, SPILL.encode(List.of(node(5, -9), node(6, 7), node(7, -30000)),
                List.of(), 1));
        spill(ResultLayout.WAYS_DIR, SPILL.encode(List.of(way(10, 32767), way(11, -9), way(12, 32767)),
                List.of(), 2));
        spill(ResultLayout.RELATIONS_DIR, SPILL.encode(List.of(), List.of(new ArrowRelation(20L, Map.of()),
                new ArrowRelation(21L, Map.of("type", "route"))), 3));
        spill(ResultLayout.RELATIONS_DIR, SPILL.encode(List.of(), List.of(new ArrowRelation(22L, Map.of())), 4));

        new ParquetPartitions(arrowDir, PARTITIONS, parquetWriter, SPILL).writeBlocks(executor);

        assertEquals(List.of("nodes_000.parquet", "nodes_001.parquet"), fileNames(ResultLayout.NODES_DIR));
        assertEquals(List.of("-30000 7", "-9 2", "-9 5"), rows("nodes/nodes_000.parquet", "h33, id"));
        assertEquals(List.of("7 1", "7 3", "7 6", "25764 4"), rows("nodes/nodes_001.parquet", "h33, id"));
        assertEquals(List.of("ways_000.parquet", "ways_32767.parquet"), fileNames(ResultLayout.WAYS_DIR));
        assertEquals(List.of("-9 11"), rows("ways/ways_000.parquet", "h33, id"));
        assertEquals(List.of("32767 10", "32767 12"), rows("ways/ways_32767.parquet", "h33, id"));
        assertEquals(List.of("20 null", "21 {type=route}", "22 null"), rows("relations/relations.parquet", "id, tags"));
        for (String entityDir : ResultLayout.SPILL_ENTITY_DIRS) {
            assertEquals(List.of(), spillFileNames(entityDir));
        }
    }

    @Test
    void writeBlocks_nothingSpilled_noFiles() throws Exception {
        new ParquetPartitions(arrowDir, PARTITIONS, parquetWriter, SPILL).writeBlocks(executor);

        assertEquals(List.of(), fileNames(ResultLayout.NODES_DIR));
        assertEquals(List.of(), fileNames(ResultLayout.WAYS_DIR));
        assertEquals(List.of(), fileNames(ResultLayout.RELATIONS_DIR));
    }

    @Test
    void writeBlocks_cellOutsidePartitions_throws() throws Exception {
        spill(ResultLayout.NODES_DIR, SPILL.encode(List.of(node(1, 7), node(2, 32767)), List.of(), 0));
        ParquetPartitions parquetPartitions = new ParquetPartitions(arrowDir, PARTITIONS, parquetWriter, SPILL);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> parquetPartitions.writeBlocks(executor));

        assertEquals("No partition for nodes with h3_3 [32767]", exception.getMessage());
    }

    @Test
    void writeBlocks_writerFails_throwsIOException() throws Exception {
        spill(ResultLayout.NODES_DIR, SPILL.encode(List.of(node(1, 7)), List.of(), 0));
        ParquetPartitions parquetPartitions = new ParquetPartitions(arrowDir, PARTITIONS, parquetWriter, SPILL);
        parquetWriter.close();

        assertThrows(IOException.class, () -> parquetPartitions.writeBlocks(executor));
    }

    @Test
    void spillMultipolygons_unsortedRows_partitionFilesSortedByH33AndId() throws Exception {
        H3Core h3Core = H3Core.newInstance();
        short phuket = CompactH3.serialize3(h3Core.latLngToCell(7.88, 98.30, 3));
        short bangkok = CompactH3.serialize3(h3Core.latLngToCell(13.75, 100.50, 3));
        ParquetPartitions parquetPartitions = new ParquetPartitions(arrowDir, List.of(
                new Partition(0, (short) PartitionSplitter.LOWER_H3_3_BOUND, (short) PartitionSplitter.HIGH_H3_3_BOUND,
                        0)), parquetWriter, SPILL);

        multipolygonBatch(parquetPartitions, new long[]{30, 10, 20},
                new byte[][]{square(98.30, 7.88, 0.001), square(100.50, 13.75, 0.001), square(98.0, 7.5, 5.0)});
        multipolygonBatch(parquetPartitions, new long[]{5, 40},
                new byte[][]{square(98.30, 7.88, 0.001), square(98.0, 7.5, 5.0)});
        parquetPartitions.writeMultipolygons();

        assertEquals(List.of("multipolygon_000.parquet", "multipolygon_32767.parquet"),
                fileNames(ResultLayout.MULTIPOLYGON_DIR));
        List<String> expected = Stream.of(phuket + " 5", phuket + " 30", bangkok + " 10")
                .sorted((a, b) -> Short.compare(Short.parseShort(a.split(" ")[0]), Short.parseShort(b.split(" ")[0])))
                .collect(Collectors.toList());
        assertEquals(expected, rows("multipolygon/multipolygon_000.parquet", "h33, id"));
        assertEquals(List.of("32767 20", "32767 40"), rows("multipolygon/multipolygon_32767.parquet", "h33, id"));
        assertEquals(List.of("{\"id\":\"30\"}"), rows("multipolygon/multipolygon_000.parquet", "tags_json",
                "id = 30"));
        assertEquals(List.of(), spillFileNames(ResultLayout.MULTIPOLYGON_DIR));
    }

    @Test
    void spillMultipolygons_invalidWkb_throwsWithId() throws Exception {
        ParquetPartitions parquetPartitions = new ParquetPartitions(arrowDir, PARTITIONS, parquetWriter, SPILL);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> multipolygonBatch(parquetPartitions, new long[]{77}, new byte[][]{{1, 2, 3}}));

        assertEquals("Invalid WKB of multipolygon 77", exception.getMessage());
    }

    @Test
    void deleteSpill_leftoverFiles_removesSpillDirectory() throws Exception {
        spill(ResultLayout.WAYS_DIR, SPILL.encode(List.of(way(1, 5)), List.of(), 0));

        new ParquetPartitions(arrowDir, PARTITIONS, parquetWriter, SPILL).deleteSpill();

        assertFalse(new File(arrowDir, ResultLayout.SPILL_DIR).exists());
    }

    private void spill(String entityDir, Map<Short, SpillChunk> chunks) throws IOException {
        File directory = new File(arrowDir, ResultLayout.SPILL_DIR + "/" + entityDir);
        for (Map.Entry<Short, SpillChunk> chunk : chunks.entrySet()) {
            Files.write(new File(directory, ResultLayout.spillFileName(chunk.getKey())).toPath(),
                    chunk.getValue().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    private static void multipolygonBatch(ParquetPartitions parquetPartitions, long[] ids, byte[][] wkbs)
            throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(DUCKDB_MULTIPOLYGON_SCHEMA, allocator)) {
            BigIntVector id = (BigIntVector) root.getVector(ArrowSchemas.ID);
            VarBinaryVector wkb = (VarBinaryVector) root.getVector(ArrowSchemas.WKB);
            VarCharVector tags = (VarCharVector) root.getVector(ArrowSchemas.TAGS_JSON);
            root.allocateNew();
            for (int row = 0; row < ids.length; row++) {
                id.setSafe(row, ids[row]);
                wkb.setSafe(row, wkbs[row]);
                tags.setSafe(row, ("{\"id\":\"" + ids[row] + "\"}").getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(ids.length);
            parquetPartitions.spillMultipolygons(root);
        }
    }

    private static byte[] square(double longitude, double latitude, double size) {
        return new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN).write(GEOMETRY_FACTORY.createPolygon(new Coordinate[]{
                new Coordinate(longitude, latitude), new Coordinate(longitude + size, latitude),
                new Coordinate(longitude + size, latitude + size), new Coordinate(longitude, latitude + size),
                new Coordinate(longitude, latitude)}));
    }

    private static ArrowNodeOrWay node(long id, int h33) {
        return new ArrowNodeOrWay(id, (short) h33, 200, 4.0, 73.0, List.of(new Tag("amenity", "cafe")));
    }

    private static ArrowNodeOrWay way(long id, int h33) {
        return new ArrowNodeOrWay.Builder(id, (short) h33, 200, 4.0, 73.0, List.of(new Tag("highway", "primary")))
                .pointIdxs(new long[]{1L, 2L})
                .lineStringWkb(new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN).write(GEOMETRY_FACTORY.createLineString(
                        new Coordinate[]{new Coordinate(73.0, 4.0), new Coordinate(73.1, 4.1)})))
                .bboxWkb(new byte[]{2})
                .build();
    }

    private List<String> fileNames(String entityDir) {
        String[] names = new File(arrowDir, entityDir).list();
        return names == null ? List.of() : Arrays.stream(names).sorted().collect(Collectors.toList());
    }

    private List<String> spillFileNames(String entityDir) {
        String[] names = new File(arrowDir, ResultLayout.SPILL_DIR + "/" + entityDir).list();
        return names == null ? List.of() : Arrays.stream(names).sorted().collect(Collectors.toList());
    }

    private List<String> rows(String file, String columns) throws SQLException {
        return rows(file, columns, "true");
    }

    private List<String> rows(String file, String columns, String where) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT " + columns + " FROM read_parquet('"
                     + new File(arrowDir, file).getAbsolutePath() + "', file_row_number = true) WHERE " + where
                     + " ORDER BY file_row_number")) {
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> values = new ArrayList<>();
                for (int column = 1; column <= columnCount; column++) {
                    values.add(String.valueOf(rs.getObject(column)));
                }
                rows.add(String.join(" ", values));
            }
        }
        return rows;
    }
}
