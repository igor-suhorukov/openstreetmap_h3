package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.ArrowFormat;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.RootArrowReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public abstract class ArrowBatchWriter {

    protected static final Logger log = LoggerFactory.getLogger(ArrowBatchWriter.class);

    protected final File resultDir;
    protected final ArrowFormat arrowFormat;

    protected ArrowBatchWriter(File resultDir, ArrowFormat arrowFormat) {
        this.resultDir = resultDir;
        this.arrowFormat = arrowFormat;
    }

    protected static void writeTagsToArrow(BufferAllocator allocator, UnionMapWriter mapWriter, int idx, Map<String, String> tags) {
        mapWriter.setPosition(idx);
        if (tags != null && !tags.isEmpty()) {
            mapWriter.startMap();
            tags.forEach((key, value) -> {
                if (value.isEmpty()) {
                    value = "-";
                }
                if (key.isEmpty()) {
                    key = "-";
                }
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                try (
                        ArrowBuf keyBuf = allocator.buffer(keyBytes.length);
                        ArrowBuf valueBuf = allocator.buffer(valueBytes.length)
                ) {
                    mapWriter.startEntry();
                    keyBuf.writeBytes(keyBytes);
                    valueBuf.writeBytes(valueBytes);
                    mapWriter.key().varChar().writeVarChar(0, keyBytes.length, keyBuf);
                    mapWriter.value().varChar().writeVarChar(0, valueBytes.length, valueBuf);
                    mapWriter.endEntry();
                }
            });
            mapWriter.endMap();
        } else {
            mapWriter.writeNull();
        }
    }

    // Output path is derived from server-controlled resultDirectory, not from external user input.
    @SuppressWarnings("java:S2077")
    protected static void saveParquet(File resultDirectory, BufferAllocator allocator, VectorSchemaRoot vectorSchemaRoot, String fileName) throws IOException, SQLException {
        try (ArrowReader reader = RootArrowReader.fromRoot(allocator, vectorSchemaRoot);
             var arrowArrayStream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, arrowArrayStream);
            String filePath = resultDirectory.getAbsolutePath() + "/" + fileName;
            try (DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                connection.registerArrowStream("arrow_stream", arrowArrayStream);
                try (Statement preparedStatement = connection.createStatement()) {
                    preparedStatement.executeUpdate("COPY (SELECT * FROM arrow_stream) TO '" + filePath + ".parquet' (FORMAT 'PARQUET', CODEC 'ZSTD')");
                }
            }
        }
    }

    protected static void saveArrowIPC(File resultDirectory, VectorSchemaRoot vectorSchemaRoot, String fileName) throws IOException {
        try (
                FileOutputStream fileOutputStream = new FileOutputStream(new File(resultDirectory, fileName + ".arrow"));
                ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel())
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
    }

    @FunctionalInterface
    protected interface BlockWriter {
        void write(BufferAllocator allocator, VectorSchemaRoot root) throws Exception;
    }

    protected void writeBlock(Long blockNumber, Schema schema, String fileName, BlockWriter writer) {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            writer.write(allocator, root);
            dispatch(allocator, root, fileName, blockNumber);
        } catch (Exception e) {
            log.error("block {}", blockNumber, e);
            System.exit(-1);
        }
    }

    protected void dispatch(BufferAllocator allocator, VectorSchemaRoot vectorSchemaRoot, String fileName, Long blockNumber) {
        try {
            switch (arrowFormat) {
                case PARQUET:
                    saveParquet(resultDir, allocator, vectorSchemaRoot, fileName);
                    break;
                case ARROW_IPC:
                    saveArrowIPC(resultDir, vectorSchemaRoot, fileName);
                    break;
                default:
                    throw new IllegalArgumentException(arrowFormat.name());
            }
        } catch (Exception e) {
            log.error("block {}", blockNumber, e);
            System.exit(-1);
        }
    }
}
