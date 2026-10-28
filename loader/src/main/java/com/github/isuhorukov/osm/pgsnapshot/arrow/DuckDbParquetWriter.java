package com.github.isuhorukov.osm.pgsnapshot.arrow;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public final class DuckDbParquetWriter implements ParquetWriter {

    private static final String ARROW_STREAM = "arrow_stream";

    private final DuckDBConnection connection;

    private DuckDbParquetWriter(DuckDBConnection connection) {
        this.connection = connection;
    }

    public static DuckDbParquetWriter open() throws IOException {
        try {
            return new DuckDbParquetWriter((DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:"));
        } catch (SQLException e) {
            throw new IOException("Opening DuckDB failed", e);
        }
    }

    @Override
    public void write(BufferAllocator allocator, ArrowReader reader, File target) throws IOException {
        try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection writer = connection.duplicate()) {
            Data.exportArrayStream(allocator, reader, stream);
            writer.registerArrowStream(ARROW_STREAM, stream);
            copy(writer, "SELECT * FROM " + ARROW_STREAM, target);
        } catch (SQLException e) {
            throw new IOException("Writing " + target + " failed", e);
        }
    }

    public void writeQuery(String query, File target) throws IOException {
        try (DuckDBConnection writer = connection.duplicate()) {
            copy(writer, query, target);
        } catch (SQLException e) {
            throw new IOException("Writing " + target + " failed", e);
        }
    }

    @SuppressWarnings("java:S2077")
    private static void copy(DuckDBConnection writer, String query, File target) throws SQLException {
        try (Statement statement = writer.createStatement()) {
            statement.executeUpdate("COPY (" + query + ") TO '" + target.getAbsolutePath().replace("'", "''")
                    + "' (FORMAT 'PARQUET', CODEC 'ZSTD')");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IOException("Closing DuckDB failed", e);
        }
    }
}
