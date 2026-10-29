package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.IpcCompression;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowMultipolygon;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import com.github.isuhorukov.osm.pgsnapshot.output.SpillChunk;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class ArrowSpill {

    private static final int READ_BUFFER_SIZE = 1 << 20;
    private static final short RELATIONS_CELL = 0;

    private final CompressionCodec codec;

    public ArrowSpill(CompressionCodec codec) {
        this.codec = codec;
    }

    public static ArrowSpill of(IpcCompression compression, int zstdLevel) {
        return new ArrowSpill(compression == IpcCompression.ZSTD
                ? CommonsCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.ZSTD, zstdLevel)
                : NoCompressionCodec.INSTANCE);
    }

    public Map<Short, SpillChunk> encode(List<ArrowNodeOrWay> arrowNodeOrWays, List<ArrowRelation> arrowRelations,
                                                long blockNumber) {
        if (!arrowNodeOrWays.isEmpty() && !arrowRelations.isEmpty()) {
            throw new IllegalStateException("Block " + blockNumber + " mixes relations with nodes or ways");
        }
        if (!arrowRelations.isEmpty()) {
            return encodeRelations(arrowRelations);
        }
        return encodeNodesOrWays(arrowNodeOrWays, blockNumber);
    }

    Map<Short, SpillChunk> encodeNodesOrWays(List<ArrowNodeOrWay> arrowNodeOrWays, long blockNumber) {
        Map<Short, SpillChunk> chunks = new TreeMap<>();
        if (arrowNodeOrWays.isEmpty()) {
            return chunks;
        }
        boolean isWays = arrowNodeOrWays.get(0).getPointIdxs() != null;
        String entity = isWays ? ResultLayout.WAYS_DIR : ResultLayout.NODES_DIR;
        Map<Short, List<ArrowNodeOrWay>> byCell = new TreeMap<>();
        for (ArrowNodeOrWay item : arrowNodeOrWays) {
            if ((item.getPointIdxs() != null) != isWays) {
                throw new IllegalStateException("Block " + blockNumber + " mixes nodes and ways");
            }
            List<ArrowNodeOrWay> cellItems = byCell.computeIfAbsent(item.getH33(), h33 -> new ArrayList<>());
            if (!cellItems.isEmpty() && item.getId() <= cellItems.get(cellItems.size() - 1).getId()) {
                throw SpillChunk.outOfOrder(entity, item.getH33(), item.getId(),
                        cellItems.get(cellItems.size() - 1).getId());
            }
            cellItems.add(item);
        }
        Schema schema = ArrowSchemas.getNodesOrWaysSchema(isWays);
        try (BufferAllocator allocator = new RootAllocator()) {
            for (Map.Entry<Short, List<ArrowNodeOrWay>> cell : byCell.entrySet()) {
                List<ArrowNodeOrWay> cellItems = cell.getValue();
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    NodeWayArrowWriter.fill(allocator, root, cellItems, isWays);
                    chunks.put(cell.getKey(), new SpillChunk(recordBatch(root), cellItems.get(0).getId(),
                            cellItems.get(cellItems.size() - 1).getId()));
                }
            }
        }
        return chunks;
    }

    Map<Short, SpillChunk> encodeRelations(List<ArrowRelation> arrowRelations) {
        for (int i = 1; i < arrowRelations.size(); i++) {
            if (arrowRelations.get(i).getId() <= arrowRelations.get(i - 1).getId()) {
                throw SpillChunk.outOfOrder(ResultLayout.RELATIONS_DIR, RELATIONS_CELL, arrowRelations.get(i).getId(),
                        arrowRelations.get(i - 1).getId());
            }
        }
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(ArrowSchemas.getRelationSchema(), allocator)) {
            RelationArrowWriter.fill(allocator, root, arrowRelations);
            return Map.of(RELATIONS_CELL, new SpillChunk(recordBatch(root), arrowRelations.get(0).getId(),
                    arrowRelations.get(arrowRelations.size() - 1).getId()));
        }
    }

    public Map<Short, byte[]> encodeMultipolygons(List<ArrowMultipolygon> multipolygons) {
        Map<Short, List<ArrowMultipolygon>> byCell = new TreeMap<>();
        for (ArrowMultipolygon multipolygon : multipolygons) {
            byCell.computeIfAbsent(multipolygon.getH33(), h33 -> new ArrayList<>()).add(multipolygon);
        }
        Map<Short, byte[]> chunks = new TreeMap<>();
        try (BufferAllocator allocator = new RootAllocator()) {
            for (Map.Entry<Short, List<ArrowMultipolygon>> cell : byCell.entrySet()) {
                try (VectorSchemaRoot root = VectorSchemaRoot.create(ArrowSchemas.getMultipolygonSchema(), allocator)) {
                    MultipolygonArrowWriter.fill(allocator, root, cell.getValue());
                    chunks.put(cell.getKey(), recordBatch(root));
                }
            }
        }
        return chunks;
    }

    public byte[] recordBatch(VectorSchemaRoot root) {
        try (ArrowRecordBatch batch = new VectorUnloader(root, true, codec, true).getRecordBatch()) {
            ByteArrayOutputStream message = new ByteArrayOutputStream();
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(message)), batch, IpcOption.DEFAULT);
            return message.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static byte[] schemaMessage(Schema schema) {
        try {
            ByteArrayOutputStream message = new ByteArrayOutputStream();
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(message)), schema, IpcOption.DEFAULT);
            return message.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ArrowReader reader(BufferAllocator allocator, Schema schema, List<File> spillFiles) {
        InputStream messages = new SpillInputStream(schemaMessage(schema), spillFiles);
        return new ArrowStreamReader(new BufferedInputStream(messages, READ_BUFFER_SIZE), allocator,
                CommonsCompressionFactory.INSTANCE);
    }

    private static final class SpillInputStream extends InputStream {
        private final Iterator<File> files;
        private InputStream current;

        private SpillInputStream(byte[] schemaMessage, List<File> files) {
            this.current = new ByteArrayInputStream(schemaMessage);
            this.files = files.iterator();
        }

        @Override
        public int read() throws IOException {
            byte[] single = new byte[1];
            return read(single, 0, 1) < 0 ? -1 : single[0] & 0xFF;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            while (current != null) {
                int read = current.read(buffer, offset, length);
                if (read > 0) {
                    return read;
                }
                current.close();
                current = files.hasNext() ? new FileInputStream(files.next()) : null;
            }
            return -1;
        }

        @Override
        public void close() throws IOException {
            if (current != null) {
                current.close();
                current = null;
            }
        }
    }
}
