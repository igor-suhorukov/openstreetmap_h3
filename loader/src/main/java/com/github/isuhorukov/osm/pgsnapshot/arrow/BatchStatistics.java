package com.github.isuhorukov.osm.pgsnapshot.arrow;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowFooter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class BatchStatistics {

    public static final String METADATA_KEY = "openstreetmap_h3:batch_statistics";
    static final List<String> COLUMNS = List.of(ArrowSchemas.ID, ArrowSchemas.H33, "h38");
    private static final byte[] MAGIC = "ARROW1".getBytes(StandardCharsets.US_ASCII);
    private static final int FOOTER_TAIL = Integer.BYTES + MAGIC.length;

    private final List<String> batches = new ArrayList<>();

    public void add(VectorSchemaRoot batch) {
        StringBuilder json = new StringBuilder("{\"rows\":").append(batch.getRowCount());
        for (String column : COLUMNS) {
            FieldVector vector = batch.getVector(column);
            if (vector instanceof BaseIntVector) {
                json.append(",\"").append(column).append("\":").append(range((BaseIntVector) vector,
                        batch.getRowCount()));
            }
        }
        batches.add(json.append('}').toString());
    }

    public String toJson() {
        return "{\"version\":1,\"batches\":[" + String.join(",", batches) + "]}";
    }

    private static String range(BaseIntVector vector, int rows) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        boolean found = false;
        for (int row = 0; row < rows; row++) {
            if (!vector.isNull(row)) {
                long value = vector.getValueAsLong(row);
                min = Math.min(min, value);
                max = Math.max(max, value);
                found = true;
            }
        }
        return found ? "[" + min + "," + max + "]" : "null";
    }

    public static void addToFooter(BufferAllocator allocator, File file) throws IOException {
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long footerStart = footerStart(channel);
            BatchStatistics statistics = new BatchStatistics();
            try (ArrowFileReader reader = new ArrowFileReader(channel, allocator, CommonsCompressionFactory.INSTANCE)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                List<ArrowBlock> recordBlocks = reader.getRecordBlocks();
                for (ArrowBlock block : recordBlocks) {
                    reader.loadRecordBatch(block);
                    statistics.add(root);
                }
                Map<String, String> metadata = new HashMap<>(reader.getMetaData());
                metadata.put(METADATA_KEY, statistics.toJson());
                ArrowFooter footer = new ArrowFooter(root.getSchema(), reader.getDictionaryBlocks(), recordBlocks,
                        metadata);
                channel.truncate(footerStart);
                channel.position(footerStart);
                WriteChannel out = new WriteChannel(channel);
                out.write(footer, false);
                out.writeIntLittleEndian((int) out.getCurrentPosition());
                out.write(MAGIC);
            }
        }
    }

    private static long footerStart(FileChannel channel) throws IOException {
        ByteBuffer tail = ByteBuffer.allocate(FOOTER_TAIL).order(ByteOrder.LITTLE_ENDIAN);
        channel.read(tail, channel.size() - FOOTER_TAIL);
        tail.flip();
        int footerLength = tail.getInt();
        return channel.size() - FOOTER_TAIL - footerLength;
    }
}
