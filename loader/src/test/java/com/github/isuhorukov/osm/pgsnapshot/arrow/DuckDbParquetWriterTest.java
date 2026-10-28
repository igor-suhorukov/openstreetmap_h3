package com.github.isuhorukov.osm.pgsnapshot.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.RootArrowReader;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
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
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DuckDbParquetWriterTest {

    private static final Schema SCHEMA = new Schema(List.of(
            Field.notNullable("id", new ArrowType.Int(64, true)),
            Field.nullable("name", new ArrowType.Utf8())));

    @TempDir
    Path tempDir;

    private DuckDbParquetWriter parquetWriter;

    @BeforeEach
    void openParquetWriter() throws IOException {
        parquetWriter = DuckDbParquetWriter.open();
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
    void writeQuery_select_writesResult() throws Exception {
        File target = tempDir.resolve("it's.parquet").toFile();

        parquetWriter.writeQuery("SELECT 42::BIGINT AS id, 'answer' AS name", target);

        assertEquals(List.of("42 answer"), rows(target));
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
}
