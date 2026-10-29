package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.IpcCompression;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ArrowIpcWriter implements PartitionWriter {

    public static final String FILE_EXTENSION = ".arrow";
    static final String EXTENSION_NAME = "ARROW:extension:name";
    static final String EXTENSION_METADATA = "ARROW:extension:metadata";
    static final String GEOARROW_WKB = "geoarrow.wkb";
    static final String GEOARROW_CRS84 = "{\"crs\":\"OGC:CRS84\",\"crs_type\":\"authority_code\"}";

    private final IpcCompression compression;
    private final Set<String> wkbGeometryColumns;

    public ArrowIpcWriter(IpcCompression compression, Set<String> wkbGeometryColumns) {
        this.compression = compression;
        this.wkbGeometryColumns = Set.copyOf(wkbGeometryColumns);
    }

    @Override
    public String fileExtension() {
        return FILE_EXTENSION;
    }

    @Override
    public void write(BufferAllocator allocator, ArrowReader reader, File target, List<String> orderBy)
            throws IOException {
        if (!orderBy.isEmpty()) {
            throw new IllegalArgumentException("Arrow IPC files are written in the reader order, not by " + orderBy);
        }
        VectorSchemaRoot batches = reader.getVectorSchemaRoot();
        Map<String, String> footerMetadata = new HashMap<>();
        BatchStatistics statistics = new BatchStatistics();
        try (VectorSchemaRoot root = new VectorSchemaRoot(geoArrowSchema(batches.getSchema()),
                batches.getFieldVectors(), 0);
             FileOutputStream output = new FileOutputStream(target);
             ArrowFileWriter writer = compression == IpcCompression.ZSTD
                     ? new ArrowFileWriter(root, null, output.getChannel(), footerMetadata, IpcOption.DEFAULT,
                     CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.ZSTD)
                     : new ArrowFileWriter(root, null, output.getChannel(), footerMetadata, IpcOption.DEFAULT)) {
            writer.start();
            while (reader.loadNextBatch()) {
                root.setRowCount(batches.getRowCount());
                statistics.add(root);
                writer.writeBatch();
            }
            footerMetadata.put(BatchStatistics.METADATA_KEY, statistics.toJson());
            writer.end();
        }
    }

    private Schema geoArrowSchema(Schema schema) {
        List<Field> fields = new ArrayList<>();
        for (Field field : schema.getFields()) {
            if (wkbGeometryColumns.contains(field.getName()) && field.getType() instanceof ArrowType.Binary) {
                Map<String, String> metadata = new HashMap<>(field.getMetadata());
                metadata.put(EXTENSION_NAME, GEOARROW_WKB);
                metadata.put(EXTENSION_METADATA, GEOARROW_CRS84);
                fields.add(new Field(field.getName(), new FieldType(field.isNullable(), field.getType(),
                        field.getDictionary(), metadata), field.getChildren()));
            } else {
                fields.add(field);
            }
        }
        return new Schema(fields, schema.getCustomMetadata());
    }
}
