package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.GeoParquetVersion;
import com.github.isuhorukov.osm.pgsnapshot.IpcCompression;
import com.github.isuhorukov.osm.pgsnapshot.ParquetCompression;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.RootArrowReader;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DuckDbParquetWriterTest {

    private static final Schema SCHEMA = new Schema(List.of(
            Field.notNullable("id", new ArrowType.Int(64, true)),
            Field.nullable("name", new ArrowType.Utf8())));
    private static final String GEOMETRY = "geometry";
    private static final String OTHER = "other";
    private static final Schema GEOMETRY_SCHEMA = new Schema(List.of(
            Field.notNullable("id", new ArrowType.Int(64, true)),
            Field.notNullable(GEOMETRY, new ArrowType.Binary()),
            Field.notNullable(OTHER, new ArrowType.Binary())));
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final Geometry LINE = GEOMETRY_FACTORY.createLineString(new Coordinate[]{
            new Coordinate(73.509, 4.175), new Coordinate(73.5095, 4.175)});
    private static final Geometry POINT = GEOMETRY_FACTORY.createPoint(new Coordinate(73.505, 4.173));
    private static final String GEO_COLUMNS = "\"primary_column\":\"geometry\",\"columns\":{\"geometry\":"
            + "{\"encoding\":\"WKB\",\"geometry_types\":[\"Point\",\"LineString\"],"
            + "\"bbox\":[73.505,4.173,73.5095,4.175]}}}";
    private static final Map<GeoParquetVersion, String> GEO = Map.of(
            GeoParquetVersion.V1, "{\"version\":\"1.0.0\"," + GEO_COLUMNS,
            GeoParquetVersion.BOTH, "{\"version\":\"1.0.0\"," + GEO_COLUMNS,
            GeoParquetVersion.V2, "{\"version\":\"2.0.0\"," + GEO_COLUMNS);
    private static final String GEOMETRY_LOGICAL_TYPE = "GeometryType(crs=<null>)";

    @TempDir
    Path tempDir;

    private DuckDbParquetWriter parquetWriter;

    @BeforeEach
    void openParquetWriter() throws IOException {
        parquetWriter = DuckDbParquetWriter.open(GeoParquetVersion.V1, ParquetCompression.ZSTD, Set.of(GEOMETRY));
    }

    @AfterEach
    void closeParquetWriter() throws IOException {
        parquetWriter.close();
    }

    @Test
    void write_arrowStream_writesRowsInOrderWithZstd() throws Exception {
        File target = tempDir.resolve("block.parquet").toFile();

        writeRows(target, 10, 3);

        assertEquals(List.of("10 name-10", "11 name-11", "12 name-12"), rows(target));
        assertEquals("ZSTD", compression(target));
    }

    @ParameterizedTest
    @EnumSource(ParquetCompression.class)
    void write_compression_parquetCodec(ParquetCompression compression) throws Exception {
        File target = tempDir.resolve(compression + ".parquet").toFile();

        try (DuckDbParquetWriter writer = DuckDbParquetWriter.open(GeoParquetVersion.V1, compression,
                Set.of(GEOMETRY));
             BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(GEOMETRY_SCHEMA, allocator)) {
            fillGeometries(root, new byte[][]{wkb(LINE, ByteOrderValues.LITTLE_ENDIAN)});
            write(writer, allocator, root, target);
        }

        assertEquals(compression == ParquetCompression.LZ4 ? "LZ4_RAW" : compression.name(), compression(target));
        assertEquals(".parquet", parquetWriter.fileExtension());
        assertArrayEquals(wkb(LINE, ByteOrderValues.LITTLE_ENDIAN), wkb(target, GEOMETRY, true).get(0));
    }

    @Test
    void write_concurrentStreams_eachFileGetsItsOwnRows() throws Exception {
        int fileCount = 32;
        int rowCount = 500;

        IntStream.range(0, fileCount).parallel().forEach(file -> {
            try {
                writeRows(tempDir.resolve(file + ".parquet").toFile(), file * 1000, rowCount);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        for (int file = 0; file < fileCount; file++) {
            List<String> rows = rows(tempDir.resolve(file + ".parquet").toFile());
            assertEquals(rowCount, rows.size());
            assertEquals(file * 1000 + " name-" + file * 1000, rows.get(0));
            int last = file * 1000 + rowCount - 1;
            assertEquals(last + " name-" + last, rows.get(rowCount - 1));
        }
    }

    @Test
    void write_orderBy_writesSortedRows() throws Exception {
        File target = tempDir.resolve("it's.parquet").toFile();

        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
            BigIntVector id = (BigIntVector) root.getVector("id");
            VarCharVector name = (VarCharVector) root.getVector("name");
            long[] ids = {3, 1, 2};
            id.allocateNew(ids.length);
            name.allocateNew(ids.length);
            for (int row = 0; row < ids.length; row++) {
                id.set(row, ids[row]);
                name.setSafe(row, ("name-" + ids[row]).getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(ids.length);
            try (ArrowReader reader = RootArrowReader.fromRoot(allocator, root)) {
                parquetWriter.write(allocator, reader, target, List.of("id"));
            }
        }

        assertEquals(List.of("1 name-1", "2 name-2", "3 name-3"), rows(target));
        assertEquals("ZSTD", compression(target));
    }

    @Test
    void write_missingDirectory_throwsIOException() {
        File target = tempDir.resolve("missing/block.parquet").toFile();

        IOException exception = assertThrows(IOException.class, () -> writeRows(target, 0, 1));

        assertEquals("Writing " + target + " failed", exception.getMessage());
    }

    @Test
    void write_afterClose_throwsIOException() throws IOException {
        File target = tempDir.resolve("closed.parquet").toFile();
        parquetWriter.close();

        assertThrows(IOException.class, () -> writeRows(target, 0, 1));
        assertFalse(target.exists());
    }

    @ParameterizedTest
    @EnumSource(GeoParquetVersion.class)
    void write_wkbGeometryColumn_writesGeoParquetVersion(GeoParquetVersion version) throws Exception {
        File target = tempDir.resolve(version + ".parquet").toFile();

        try (DuckDbParquetWriter writer = DuckDbParquetWriter.open(version, ParquetCompression.ZSTD, Set.of(GEOMETRY))) {
            writeGeometries(writer, target, ByteOrderValues.BIG_ENDIAN);
        }

        assertEquals(GEO.get(version), geoMetadata(target));
        assertEquals(version == GeoParquetVersion.BOTH || version == GeoParquetVersion.V2
                ? GEOMETRY_LOGICAL_TYPE : null, logicalType(target, GEOMETRY));
        assertNull(logicalType(target, OTHER));
        int geometryByteOrder = version.isGeometry() ? ByteOrderValues.LITTLE_ENDIAN : ByteOrderValues.BIG_ENDIAN;
        List<byte[]> geometries = wkb(target, GEOMETRY, version.isGeometry());
        assertArrayEquals(wkb(LINE, geometryByteOrder), geometries.get(0));
        assertArrayEquals(wkb(POINT, geometryByteOrder), geometries.get(1));
        List<byte[]> others = wkb(target, OTHER, false);
        assertArrayEquals(wkb(LINE, ByteOrderValues.BIG_ENDIAN), others.get(0));
        assertArrayEquals(wkb(POINT, ByteOrderValues.BIG_ENDIAN), others.get(1));
    }

    @Test
    void write_littleEndianWkb_writtenAsIs() throws Exception {
        File target = tempDir.resolve("little_endian.parquet").toFile();

        writeGeometries(parquetWriter, target, ByteOrderValues.LITTLE_ENDIAN);

        List<byte[]> geometries = wkb(target, GEOMETRY, true);
        assertArrayEquals(wkb(LINE, ByteOrderValues.LITTLE_ENDIAN), geometries.get(0));
        assertArrayEquals(wkb(POINT, ByteOrderValues.LITTLE_ENDIAN), geometries.get(1));
    }

    @Test
    void write_noWkbGeometryColumn_noGeoParquetMetadata() throws Exception {
        File target = tempDir.resolve("nodes.parquet").toFile();

        writeRows(target, 0, 2);

        assertNull(geoMetadata(target));
        assertEquals(List.of("0 name-0", "1 name-1"), rows(target));
    }

    @Test
    void arrowIpcWriter_wkbGeometryColumn_geoArrowWkbWithCrs84() throws Exception {
        File target = tempDir.resolve("multipolygon.arrow").toFile();

        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(GEOMETRY_SCHEMA, allocator)) {
            fillGeometries(root, new byte[][]{wkb(LINE, ByteOrderValues.LITTLE_ENDIAN)});
            try (ArrowReader reader = RootArrowReader.fromRoot(allocator, root)) {
                parquetWriter.arrowIpcWriter(IpcCompression.NONE).write(allocator, reader, target);
            }
        }

        try (BufferAllocator allocator = new RootAllocator();
             FileInputStream input = new FileInputStream(target);
             ArrowFileReader reader = new ArrowFileReader(input.getChannel(), allocator)) {
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            Map<String, String> geometry = schema.findField(GEOMETRY).getMetadata();
            assertEquals(ArrowIpcWriter.GEOARROW_WKB, geometry.get(ArrowIpcWriter.EXTENSION_NAME));
            assertTrue(geometry.get(ArrowIpcWriter.EXTENSION_METADATA).contains("\"crs_type\":\"projjson\""));
            assertTrue(geometry.get(ArrowIpcWriter.EXTENSION_METADATA)
                    .contains("\"id\":{\"authority\":\"OGC\",\"code\":\"CRS84\"}"));
            assertEquals(Map.of(), schema.findField(OTHER).getMetadata());
            assertTrue(reader.loadNextBatch());
            assertArrayEquals(wkb(LINE, ByteOrderValues.LITTLE_ENDIAN),
                    ((VarBinaryVector) reader.getVectorSchemaRoot().getVector(GEOMETRY)).get(0));
        }
    }

    @Test
    void write_invalidWkb_throwsIOException() {
        File target = tempDir.resolve("invalid.parquet").toFile();

        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(GEOMETRY_SCHEMA, allocator)) {
            fillGeometries(root, new byte[][]{{0x01, 0x02}});
            IOException exception = assertThrows(IOException.class, () -> write(parquetWriter, allocator, root, target));
            assertEquals("Writing " + target + " failed", exception.getMessage());
        }
    }

    private static void writeGeometries(DuckDbParquetWriter writer, File target, int byteOrder) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(GEOMETRY_SCHEMA, allocator)) {
            fillGeometries(root, new byte[][]{wkb(LINE, byteOrder), wkb(POINT, byteOrder)});
            write(writer, allocator, root, target);
        }
    }

    private static void fillGeometries(VectorSchemaRoot root, byte[][] geometries) {
        BigIntVector id = (BigIntVector) root.getVector("id");
        VarBinaryVector geometry = (VarBinaryVector) root.getVector(GEOMETRY);
        VarBinaryVector other = (VarBinaryVector) root.getVector(OTHER);
        id.allocateNew(geometries.length);
        geometry.allocateNew(geometries.length);
        other.allocateNew(geometries.length);
        for (int row = 0; row < geometries.length; row++) {
            id.set(row, row);
            geometry.setSafe(row, geometries[row]);
            other.setSafe(row, wkb(row == 0 ? LINE : POINT, ByteOrderValues.BIG_ENDIAN));
        }
        root.setRowCount(geometries.length);
    }

    private static void write(DuckDbParquetWriter writer, BufferAllocator allocator, VectorSchemaRoot root,
                              File target) throws IOException {
        try (ArrowReader reader = RootArrowReader.fromRoot(allocator, root)) {
            writer.write(allocator, reader, target);
        }
    }

    private static byte[] wkb(Geometry geometry, int byteOrder) {
        return new WKBWriter(2, byteOrder).write(geometry);
    }

    private static String geoMetadata(File file) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT decode(value) FROM parquet_kv_metadata(" + literal(file)
                     + ") WHERE decode(key) = 'geo'")) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static String logicalType(File file, String column) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT logical_type FROM parquet_schema(" + literal(file)
                     + ") WHERE name = '" + column + "'")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static List<byte[]> wkb(File file, String column, boolean geometry) throws SQLException {
        List<byte[]> values = new ArrayList<>();
        String value = geometry ? "ST_AsWKB(\"" + column + "\")" : "\"" + column + "\"";
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT " + value + " FROM read_parquet(" + literal(file)
                     + ", file_row_number = true) ORDER BY file_row_number")) {
            while (rs.next()) {
                values.add(rs.getBytes(1));
            }
        }
        return values;
    }

    private void writeRows(File target, int first, int count) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
            BigIntVector id = (BigIntVector) root.getVector("id");
            VarCharVector name = (VarCharVector) root.getVector("name");
            id.allocateNew(count);
            name.allocateNew(count);
            for (int row = 0; row < count; row++) {
                id.set(row, first + row);
                name.setSafe(row, ("name-" + (first + row)).getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(count);
            try (ArrowReader reader = RootArrowReader.fromRoot(allocator, root)) {
                parquetWriter.write(allocator, reader, target);
            }
        }
    }

    private static List<String> rows(File file) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT id, name FROM read_parquet(" + literal(file)
                     + ", file_row_number = true) ORDER BY file_row_number")) {
            while (rs.next()) {
                rows.add(rs.getLong(1) + " " + rs.getString(2));
            }
        }
        return rows;
    }

    private static String compression(File file) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT string_agg(DISTINCT compression, ',') FROM parquet_metadata("
                     + literal(file) + ")")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String literal(File file) {
        return "'" + file.getAbsolutePath().replace("'", "''") + "'";
    }

    @Test
    void kvMetadata_sortedKeysAndQuotedValues() {
        assertEquals("", DuckDbParquetWriter.kvMetadata(Map.of()));
        assertEquals(", KV_METADATA {'a:b': '{\"x\":\"it''s\"}', 'c': 'd'}",
                DuckDbParquetWriter.kvMetadata(Map.of("c", "d", "a:b", "{\"x\":\"it's\"}")));
    }
}
