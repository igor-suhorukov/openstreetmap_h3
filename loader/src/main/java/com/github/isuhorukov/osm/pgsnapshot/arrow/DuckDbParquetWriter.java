package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.GeoParquetVersion;
import com.github.isuhorukov.osm.pgsnapshot.IpcCompression;
import com.github.isuhorukov.osm.pgsnapshot.ParquetCompression;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class DuckDbParquetWriter implements PartitionWriter, Closeable {

    public static final String FILE_EXTENSION = ".parquet";
    static final String ARROW_IPC_EXTENSION = "nanoarrow";
    private static final String ARROW_STREAM = "arrow_stream";
    private static final String PARQUET_GEOMETRY = "ST_GeomFromWKB(%s)";
    private static final String ARROW_IPC_GEOMETRY = "ST_GeomFromWKB(%s)::GEOMETRY('OGC:CRS84')";

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
    public String fileExtension() {
        return FILE_EXTENSION;
    }

    @Override
    public void write(BufferAllocator allocator, ArrowReader reader, File target, List<String> orderBy,
                      Map<String, String> metadata) throws IOException {
        String geoParquet = geoParquetVersion.isGeometry()
                ? ", GEOPARQUET_VERSION '" + geoParquetVersion.name() + "'" : "";
        copy(allocator, reader, target, select(reader.getVectorSchemaRoot().getSchema(),
                geoParquetVersion.isGeometry() ? PARQUET_GEOMETRY : null, orderBy),
                "FORMAT 'PARQUET', CODEC '" + compression.name() + "'" + geoParquet + kvMetadata(metadata));
    }

    static String kvMetadata(Map<String, String> metadata) {
        if (metadata.isEmpty()) {
            return "";
        }
        StringBuilder options = new StringBuilder(", KV_METADATA {");
        boolean first = true;
        for (Map.Entry<String, String> entry : new TreeMap<>(metadata).entrySet()) {
            options.append(first ? "" : ", ").append(literal(entry.getKey())).append(": ")
                    .append(literal(entry.getValue()));
            first = false;
        }
        return options.append('}').toString();
    }

    private static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    public PartitionWriter arrowIpcWriter(IpcCompression ipcCompression) throws IOException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSTALL " + ARROW_IPC_EXTENSION + " FROM community");
            statement.execute("LOAD " + ARROW_IPC_EXTENSION);
        } catch (SQLException e) {
            throw new IOException("Arrow IPC files of DuckDB need the " + ARROW_IPC_EXTENSION
                    + " extension: INSTALL " + ARROW_IPC_EXTENSION + " FROM community failed", e);
        }
        String options = "FORMAT 'ARROW', BATCH_SIZE " + CellBatchReader.MAX_BATCH_ROWS
                + (ipcCompression == IpcCompression.ZSTD ? ", COMPRESSION 'zstd'" : "");
        return new PartitionWriter() {
            @Override
            public String fileExtension() {
                return ArrowIpcWriter.FILE_EXTENSION;
            }

            @Override
            public void write(BufferAllocator allocator, ArrowReader reader, File target, List<String> orderBy,
                              Map<String, String> metadata) throws IOException {
                copy(allocator, reader, target, select(reader.getVectorSchemaRoot().getSchema(), ARROW_IPC_GEOMETRY,
                        orderBy), options);
                BatchStatistics.addToFooter(allocator, target, metadata);
            }
        };
    }

    private String select(Schema schema, String geometryExpression, List<String> orderBy) {
        List<String> geometries = new ArrayList<>();
        if (geometryExpression != null) {
            for (Field field : schema.getFields()) {
                if (wkbGeometryColumns.contains(field.getName()) && field.getType() instanceof ArrowType.Binary) {
                    String column = quote(field.getName());
                    geometries.add(String.format(geometryExpression, column) + " AS " + column);
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
    private void copy(BufferAllocator allocator, ArrowReader reader, File target, String query, String options)
            throws IOException {
        try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
             DuckDBConnection writer = connection.duplicate();
             Statement statement = writer.createStatement()) {
            Data.exportArrayStream(allocator, reader, stream);
            writer.registerArrowStream(ARROW_STREAM, stream);
            statement.executeUpdate("COPY (" + query + ") TO '" + target.getAbsolutePath().replace("'", "''")
                    + "' (" + options + ")");
        } catch (SQLException e) {
            throw new IOException("Writing " + target + " failed", e);
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
