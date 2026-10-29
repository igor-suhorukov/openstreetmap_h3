package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.GeoParquetVersion;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class DuckDbParquetWriter implements ParquetWriter {

    private static final String ARROW_STREAM = "arrow_stream";

    private final DuckDBConnection connection;
    private final GeoParquetVersion geoParquetVersion;
    private final Set<String> wkbGeometryColumns;

    private DuckDbParquetWriter(DuckDBConnection connection, GeoParquetVersion geoParquetVersion,
                                Set<String> wkbGeometryColumns) {
        this.connection = connection;
        this.geoParquetVersion = geoParquetVersion;
        this.wkbGeometryColumns = Set.copyOf(wkbGeometryColumns);
    }

    public static DuckDbParquetWriter open(GeoParquetVersion geoParquetVersion, Set<String> wkbGeometryColumns)
            throws IOException {
        try {
            return new DuckDbParquetWriter((DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:"),
                    geoParquetVersion, wkbGeometryColumns);
        } catch (SQLException e) {
            throw new IOException("Opening DuckDB failed", e);
        }
    }

    @Override
    public void write(BufferAllocator allocator, ArrowReader reader, File target) throws IOException {
        try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection writer = connection.duplicate()) {
            String query = select(reader.getVectorSchemaRoot().getSchema());
            Data.exportArrayStream(allocator, reader, stream);
            writer.registerArrowStream(ARROW_STREAM, stream);
            copy(writer, query, target);
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

    private String select(Schema schema) {
        List<String> geometries = new ArrayList<>();
        if (geoParquetVersion.isGeometry()) {
            for (Field field : schema.getFields()) {
                if (wkbGeometryColumns.contains(field.getName()) && field.getType() instanceof ArrowType.Binary) {
                    String column = '"' + field.getName().replace("\"", "\"\"") + '"';
                    geometries.add("ST_GeomFromWKB(" + column + ") AS " + column);
                }
            }
        }
        if (geometries.isEmpty()) {
            return "SELECT * FROM " + ARROW_STREAM;
        }
        return "SELECT * REPLACE (" + String.join(", ", geometries) + ") FROM " + ARROW_STREAM;
    }

    @SuppressWarnings("java:S2077")
    private void copy(DuckDBConnection writer, String query, File target) throws SQLException {
        String geoParquet = geoParquetVersion.isGeometry()
                ? ", GEOPARQUET_VERSION '" + geoParquetVersion.name() + "'" : "";
        try (Statement statement = writer.createStatement()) {
            statement.executeUpdate("COPY (" + query + ") TO '" + target.getAbsolutePath().replace("'", "''")
                    + "' (FORMAT 'PARQUET', CODEC 'ZSTD'" + geoParquet + ")");
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
