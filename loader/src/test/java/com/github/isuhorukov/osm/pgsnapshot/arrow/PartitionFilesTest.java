package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.GeoParquetVersion;
import com.github.isuhorukov.osm.pgsnapshot.IpcCompression;
import com.github.isuhorukov.osm.pgsnapshot.ParquetCompression;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowMultipolygon;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import com.github.isuhorukov.osm.pgsnapshot.output.SpillChunk;
import com.github.isuhorukov.osm.pgsnapshot.util.CompactH3;
import com.github.isuhorukov.osm.pgsnapshot.util.GeometryH33;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;
import com.uber.h3core.H3Core;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Field;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
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

class PartitionFilesTest {

    private static final List<Partition> PARTITIONS = List.of(
            new Partition(0, (short) PartitionSplitter.LOWER_H3_3_BOUND, (short) 0, 0),
            new Partition(1, (short) 0, (short) PartitionSplitter.HIGH_H3_3_BOUND, 0));
    private static final short PHUKET = h33(7.88, 98.30);
    private static final short BANGKOK = h33(13.75, 100.50);
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final ArrowSpill SPILL = ArrowSpill.of(IpcCompression.ZSTD, 1);

    @TempDir
    Path tempDir;

    private File arrowDir;
    private DuckDbParquetWriter parquetWriter;
    private ExecutorService executor;

    @BeforeEach
    void prepare() throws IOException {
        ResultLayout.prepareResultDirectories(tempDir.toFile(), false, true);
        arrowDir = new File(tempDir.toFile(), ResultLayout.ARROW_DIR);
        parquetWriter = DuckDbParquetWriter.open(GeoParquetVersion.V1, ParquetCompression.ZSTD,
                Set.of(ArrowSchemas.LINE_STRING_WKB, ArrowSchemas.WKB));
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

        new PartitionFiles(arrowDir, PARTITIONS, SPILL, parquetWriter, parquetWriter).writeBlocks(executor);

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
    void writeBlocks_arrowIpc_cellAlignedBatchesInH33AndIdOrder() throws Exception {
        spill(ResultLayout.NODES_DIR, SPILL.encode(List.of(node(1, 7), node(2, 9), node(3, 7)), List.of(), 0));
        spill(ResultLayout.NODES_DIR, SPILL.encode(List.of(node(5, 9), node(6, 7)), List.of(), 1));
        spill(ResultLayout.RELATIONS_DIR, SPILL.encode(List.of(), List.of(new ArrowRelation(20L, Map.of())), 2));
        ArrowIpcWriter ipcWriter = new ArrowIpcWriter(IpcCompression.NONE, Set.of(ArrowSchemas.LINE_STRING_WKB));

        new PartitionFiles(arrowDir, PARTITIONS, SPILL, ipcWriter, ipcWriter).writeBlocks(executor);

        assertEquals(List.of("nodes_001.arrow"), fileNames(ResultLayout.NODES_DIR));
        assertEquals(List.of("relations.arrow"), fileNames(ResultLayout.RELATIONS_DIR));
        assertEquals(List.of("batch", "7 1", "7 3", "7 6", "batch", "9 2", "9 5"),
                ipcBatches(new File(arrowDir, "nodes/nodes_001.arrow")));
        assertEquals("{\"version\":1,\"batches\":[{\"rows\":3,\"id\":[1,6],\"h33\":[7,7],\"h38\":[200,200]},"
                + "{\"rows\":2,\"id\":[2,5],\"h33\":[9,9],\"h38\":[200,200]}]}",
                footerMetadata(new File(arrowDir, "nodes/nodes_001.arrow")).get(BatchStatistics.METADATA_KEY));
    }

    @Test
    void writeMultipolygons_arrowIpcByDuckDb_sortedByH33AndIdWithZstd() throws Exception {
        PartitionFiles partitionFiles = new PartitionFiles(arrowDir, List.of(
                new Partition(0, (short) PartitionSplitter.LOWER_H3_3_BOUND, (short) PartitionSplitter.HIGH_H3_3_BOUND,
                        0)), SPILL, new ArrowIpcWriter(IpcCompression.ZSTD, Set.of(ArrowSchemas.LINE_STRING_WKB)),
                parquetWriter.arrowIpcWriter(IpcCompression.ZSTD));

        partitionFiles.appendMultipolygons(SPILL.encodeMultipolygons(List.of(multipolygon(30, PHUKET),
                multipolygon(10, PHUKET), multipolygon(20, GeometryH33.MULTIPLE_CELLS))));
        partitionFiles.writeMultipolygons();

        assertEquals(List.of("multipolygon_000.arrow", "multipolygon_32767.arrow"),
                fileNames(ResultLayout.MULTIPOLYGON_DIR));
        File phuketFile = new File(arrowDir, "multipolygon/multipolygon_000.arrow");
        assertEquals(List.of("batch", PHUKET + " 10", PHUKET + " 30"), ipcBatches(phuketFile));
        assertEquals(List.of("batch", "32767 20"), ipcBatches(new File(arrowDir, "multipolygon/multipolygon_32767.arrow")));
        assertEquals("{\"version\":1,\"batches\":[{\"rows\":2,\"id\":[10,30],\"h33\":[" + PHUKET + "," + PHUKET + "]}]}",
                footerMetadata(phuketFile).get(BatchStatistics.METADATA_KEY));
        assertEquals(ArrowIpcWriter.GEOARROW_WKB,
                ipcField(phuketFile, ArrowSchemas.WKB).getMetadata().get(ArrowIpcWriter.EXTENSION_NAME));
        assertEquals("tags: Map(false)<entries: Struct<key: Utf8 not null, value: Utf8> not null>",
                ipcField(phuketFile, ArrowSchemas.TAGS).toString());
    }

    @Test
    void writeBlocks_nothingSpilled_noFiles() throws Exception {
        new PartitionFiles(arrowDir, PARTITIONS, SPILL, parquetWriter, parquetWriter).writeBlocks(executor);

        assertEquals(List.of(), fileNames(ResultLayout.NODES_DIR));
        assertEquals(List.of(), fileNames(ResultLayout.WAYS_DIR));
        assertEquals(List.of(), fileNames(ResultLayout.RELATIONS_DIR));
    }

    @Test
    void writeBlocks_cellOutsidePartitions_throws() throws Exception {
        spill(ResultLayout.NODES_DIR, SPILL.encode(List.of(node(1, 7), node(2, 32767)), List.of(), 0));
        PartitionFiles partitionFiles = new PartitionFiles(arrowDir, PARTITIONS, SPILL, parquetWriter, parquetWriter);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> partitionFiles.writeBlocks(executor));

        assertEquals("No partition for nodes with h3_3 [32767]", exception.getMessage());
    }

    @Test
    void writeBlocks_writerFails_throwsIOException() throws Exception {
        spill(ResultLayout.NODES_DIR, SPILL.encode(List.of(node(1, 7)), List.of(), 0));
        PartitionFiles partitionFiles = new PartitionFiles(arrowDir, PARTITIONS, SPILL, parquetWriter, parquetWriter);
        parquetWriter.close();

        assertThrows(IOException.class, () -> partitionFiles.writeBlocks(executor));
    }

    @Test
    void appendMultipolygons_unsortedChunks_partitionFilesSortedByH33AndIdWithGeoParquetAndTags() throws Exception {
        PartitionFiles partitionFiles = new PartitionFiles(arrowDir, List.of(
                new Partition(0, (short) PartitionSplitter.LOWER_H3_3_BOUND, (short) PartitionSplitter.HIGH_H3_3_BOUND,
                        0)), SPILL, parquetWriter, parquetWriter);

        partitionFiles.appendMultipolygons(SPILL.encodeMultipolygons(List.of(multipolygon(30, PHUKET),
                multipolygon(10, BANGKOK), multipolygon(20, GeometryH33.MULTIPLE_CELLS))));
        partitionFiles.appendMultipolygons(SPILL.encodeMultipolygons(List.of(multipolygon(5, PHUKET),
                multipolygon(40, GeometryH33.MULTIPLE_CELLS))));
        partitionFiles.writeMultipolygons();

        assertEquals(List.of("multipolygon_000.parquet", "multipolygon_32767.parquet"),
                fileNames(ResultLayout.MULTIPOLYGON_DIR));
        List<String> expected = Stream.of(PHUKET + " 5", PHUKET + " 30", BANGKOK + " 10")
                .sorted((a, b) -> Short.compare(Short.parseShort(a.split(" ")[0]), Short.parseShort(b.split(" ")[0])))
                .collect(Collectors.toList());
        assertEquals(expected, rows("multipolygon/multipolygon_000.parquet", "h33, id"));
        assertEquals(List.of("32767 20", "32767 40"), rows("multipolygon/multipolygon_32767.parquet", "h33, id"));
        assertEquals(List.of("{id=30, name=multipolygon 30}"), rows("multipolygon/multipolygon_000.parquet", "tags",
                "id = 30"));
        assertEquals(List.of("POLYGON ((98.3 7.88, 98.301 7.88, 98.301 7.881, 98.3 7.881, 98.3 7.88))"),
                rows("multipolygon/multipolygon_000.parquet", "ST_AsText(wkb)", "id = 30"));
        assertTrue(geoMetadata("multipolygon/multipolygon_000.parquet").contains(
                "\"primary_column\":\"wkb\",\"columns\":{\"wkb\":{\"encoding\":\"WKB\",\"geometry_types\":[\"Polygon\"]"));
        assertEquals(List.of(), spillFileNames(ResultLayout.MULTIPOLYGON_DIR));
    }

    @Test
    void deleteSpill_leftoverFiles_removesSpillDirectory() throws Exception {
        spill(ResultLayout.WAYS_DIR, SPILL.encode(List.of(way(1, 5)), List.of(), 0));

        new PartitionFiles(arrowDir, PARTITIONS, SPILL, parquetWriter, parquetWriter).deleteSpill();

        assertFalse(new File(arrowDir, ResultLayout.SPILL_DIR).exists());
    }

    private static Map<String, String> footerMetadata(File file) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             FileInputStream input = new FileInputStream(file);
             ArrowFileReader reader = new ArrowFileReader(input.getChannel(), allocator,
                     CommonsCompressionFactory.INSTANCE)) {
            reader.getVectorSchemaRoot();
            return reader.getMetaData();
        }
    }

    private static List<String> ipcBatches(File file) throws IOException {
        List<String> rows = new ArrayList<>();
        try (BufferAllocator allocator = new RootAllocator();
             FileInputStream input = new FileInputStream(file);
             ArrowFileReader reader = new ArrowFileReader(input.getChannel(), allocator,
                     CommonsCompressionFactory.INSTANCE)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                rows.add("batch");
                BigIntVector ids = (BigIntVector) root.getVector(ArrowSchemas.ID);
                SmallIntVector h33 = (SmallIntVector) root.getVector(ArrowSchemas.H33);
                for (int row = 0; row < root.getRowCount(); row++) {
                    rows.add(h33.get(row) + " " + ids.get(row));
                }
            }
        }
        return rows;
    }

    private void spill(String entityDir, Map<Short, SpillChunk> chunks) throws IOException {
        File directory = new File(arrowDir, ResultLayout.SPILL_DIR + "/" + entityDir);
        for (Map.Entry<Short, SpillChunk> chunk : chunks.entrySet()) {
            Files.write(new File(directory, ResultLayout.spillFileName(chunk.getKey())).toPath(),
                    chunk.getValue().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    private static short h33(double latitude, double longitude) {
        try {
            return CompactH3.serialize3(H3Core.newInstance().latLngToCell(latitude, longitude, 3));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ArrowMultipolygon multipolygon(long id, short h33) {
        return new ArrowMultipolygon(id, h33, square(98.30, 7.88, 0.001),
                List.of(new Tag("name", "multipolygon " + id), new Tag("id", String.valueOf(id))));
    }

    private static Field ipcField(File file, String name) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             FileInputStream input = new FileInputStream(file);
             ArrowFileReader reader = new ArrowFileReader(input.getChannel(), allocator,
                     CommonsCompressionFactory.INSTANCE)) {
            return reader.getVectorSchemaRoot().getSchema().findField(name);
        }
    }

    private String geoMetadata(String file) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT decode(value) FROM parquet_kv_metadata('"
                     + new File(arrowDir, file).getAbsolutePath() + "') WHERE decode(key) = 'geo'")) {
            return rs.next() ? rs.getString(1) : null;
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
