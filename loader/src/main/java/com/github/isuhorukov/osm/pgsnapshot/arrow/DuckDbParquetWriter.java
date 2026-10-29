package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.GeoParquetVersion;
import com.github.isuhorukov.osm.pgsnapshot.ParquetCompression;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class DuckDbParquetWriter implements ParquetWriter {

    private static final String ARROW_STREAM = "arrow_stream";
    private static final long SCAN_BATCH_ROWS = 8192;

    private final DuckDBConnection connection;
    private final GeoParquetVersion geoParquetVersion;
    private final ParquetCompression compression;
    private final Set<String> wkbGeometryColumns;

    private DuckDbParquetWriter(DuckDBConnection connection, GeoParquetVersion geoParquetVersion,
                                ParquetCompression compression, Set<String> wkbGeometryColumns) {
        this.connection = connection;
        this.geoParquetVersion = geoParquetVersion;
        this.compression = compression;
        this.wkbGeometryColumns = Set.copyOf(wkbGeometryColumns);
    }

    public static DuckDbParquetWriter open(GeoParquetVersion geoParquetVersion, ParquetCompression compression,
                                           Set<String> wkbGeometryColumns) throws IOException {
        try {
            return new DuckDbParquetWriter((DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:"),
                    geoParquetVersion, compression, wkbGeometryColumns);
        } catch (SQLException e) {
            throw new IOException("Opening DuckDB failed", e);
        }
    }

    @Override
    public void write(BufferAllocator allocator, ArrowReader reader, File target, List<String> orderBy)
            throws IOException {
        try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection writer = connection.duplicate()) {
            String query = select(reader.getVectorSchemaRoot().getSchema(), orderBy);
            Data.exportArrayStream(allocator, reader, stream);
            writer.registerArrowStream(ARROW_STREAM, stream);
            copy(writer, query, target);
        } catch (SQLException e) {
            throw new IOException("Writing " + target + " failed", e);
        }
    }

    @SuppressWarnings("java:S2077")
    public void scan(String query, ArrowBatchConsumer consumer) throws IOException {
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection reader = connection.duplicate();
             Statement statement = reader.createStatement();
             DuckDBResultSet resultSet = (DuckDBResultSet) statement.executeQuery(query);
             ArrowReader batches = (ArrowReader) resultSet.arrowExportStream(allocator, SCAN_BATCH_ROWS)) {
            while (batches.loadNextBatch()) {
                consumer.accept(batches.getVectorSchemaRoot());
            }
        } catch (SQLException e) {
            throw new IOException("Reading " + query + " failed", e);
        }
    }

    private String select(Schema schema, List<String> orderBy) {
        List<String> geometries = new ArrayList<>();
        if (geoParquetVersion.isGeometry()) {
            for (Field field : schema.getFields()) {
                if (wkbGeometryColumns.contains(field.getName()) && field.getType() instanceof ArrowType.Binary) {
                    String column = quote(field.getName());
                    geometries.add("ST_GeomFromWKB(" + column + ") AS " + column);
                }
            }
        }
        String select = geometries.isEmpty() ? "SELECT * FROM " + ARROW_STREAM
                : "SELECT * REPLACE (" + String.join(", ", geometries) + ") FROM " + ARROW_STREAM;
        if (orderBy.isEmpty()) {
            return select;
        }
        return select + " ORDER BY " + orderBy.stream().map(DuckDbParquetWriter::quote).collect(Collectors.joining(", "));
    }

    private static String quote(String column) {
        return '"' + column.replace("\"", "\"\"") + '"';
    }

    @SuppressWarnings("java:S2077")
    private void copy(DuckDBConnection writer, String query, File target) throws SQLException {
        String geoParquet = geoParquetVersion.isGeometry()
                ? ", GEOPARQUET_VERSION '" + geoParquetVersion.name() + "'" : "";
        try (Statement statement = writer.createStatement()) {
            statement.executeUpdate("COPY (" + query + ") TO '" + target.getAbsolutePath().replace("'", "''")
                    + "' (FORMAT 'PARQUET', CODEC '" + compression.name() + "'" + geoParquet + ")");
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
