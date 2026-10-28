package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.ArrowFormat;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.RootArrowReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class ArrowBatchWriter {

    protected final File resultDir;
    protected final ArrowFormat arrowFormat;
    protected final ParquetWriter parquetWriter;

    protected ArrowBatchWriter(File resultDir, ArrowFormat arrowFormat, ParquetWriter parquetWriter) {
        this.resultDir = resultDir;
        this.arrowFormat = arrowFormat;
        this.parquetWriter = parquetWriter;
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

    private void saveParquet(BufferAllocator allocator, VectorSchemaRoot vectorSchemaRoot, String fileName) throws IOException {
        try (ArrowReader reader = RootArrowReader.fromRoot(allocator, vectorSchemaRoot)) {
            parquetWriter.write(allocator, reader, new File(resultDir, fileName + ".parquet"));
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
        void write(BufferAllocator allocator, VectorSchemaRoot root);
    }

    protected void writeBlock(Long blockNumber, Schema schema, String fileName, BlockWriter writer) {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            writer.write(allocator, root);
            dispatch(allocator, root, fileName);
        } catch (Exception e) {
            throw new IllegalStateException("Writing Arrow block " + blockNumber + " failed", e);
        }
    }

    protected void dispatch(BufferAllocator allocator, VectorSchemaRoot vectorSchemaRoot, String fileName) throws IOException {
        switch (arrowFormat) {
            case PARQUET:
                saveParquet(allocator, vectorSchemaRoot, fileName);
                break;
            case ARROW_IPC:
                saveArrowIPC(resultDir, vectorSchemaRoot, fileName);
                break;
            default:
                throw new IllegalArgumentException(arrowFormat.name());
        }
    }
}
