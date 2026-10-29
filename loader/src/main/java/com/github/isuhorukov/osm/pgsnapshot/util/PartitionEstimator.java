package com.github.isuhorukov.osm.pgsnapshot.util;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.google.protobuf.InvalidProtocolBufferException;
import com.uber.h3core.H3Core;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.osmbinary.Fileformat;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoder;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoderListener;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class PartitionEstimator {

    private static final Logger log = LoggerFactory.getLogger(PartitionEstimator.class);

    private static final String OSM_DATA = "OSMData";
    private static final byte[] OSM_DATA_BLOB_HEADER = ("\n\u0007" + OSM_DATA + "\u0018").getBytes(StandardCharsets.ISO_8859_1);
    private static final byte[] NO_BLOB = new byte[0];
    private static final int MIN_SAMPLES = 64;
    private static final int MAX_SAMPLES = 2048;
    private static final long BYTES_PER_SAMPLE = 1L << 20;
    private static final int SEARCH_WINDOW = 1 << 18;
    private static final int MAX_BLOB_HEADER_SIZE = 1 << 16;
    private static final int MAX_BLOB_SIZE = 1 << 25;
    private static final int DECODE_BATCH_SIZE = 256;

    private PartitionEstimator() {
    }

    public static List<Partition> estimatePartitions(File pbfFile, double thresholdPercentFromMaxPartition) throws IOException {
        long start = System.currentTimeMillis();
        Map<Short, Long> nodeCounts = sampleNodeCounts(pbfFile);
        List<Partition> partitions = partitions(nodeCounts, thresholdPercentFromMaxPartition);
        log.info("{} partitions estimated from {} sampled nodes in {} ms", partitions.size(),
                nodeCounts.values().stream().mapToLong(Long::longValue).sum(), System.currentTimeMillis() - start);
        return partitions;
    }

    static List<Partition> partitions(Map<Short, Long> nodeCounts, double thresholdPercentFromMaxPartition) {
        if (nodeCounts.isEmpty()) {
            Partition partition = new Partition(0, (short) PartitionSplitter.LOWER_H3_3_BOUND,
                    (short) PartitionSplitter.HIGH_H3_3_BOUND, 0);
            partition.setH33RegionsInside(new ArrayList<>());
            return List.of(partition);
        }
        return PartitionSplitter.distributeH33ByPartitionsForWays(nodeCounts, thresholdPercentFromMaxPartition);
    }

    static int sampleCount(long fileSize) {
        return (int) Math.max(MIN_SAMPLES, Math.min(MAX_SAMPLES, fileSize / BYTES_PER_SAMPLE));
    }

    static Map<Short, Long> sampleNodeCounts(File pbfFile) throws IOException {
        H3Core h3Core = H3Core.newInstance();
        Map<Short, Long> nodeCounts = new ConcurrentHashMap<>();
        List<byte[]> blobs = new ArrayList<>();
        try (RandomAccessFile file = new RandomAccessFile(pbfFile, "r")) {
            long fileSize = file.length();
            int samples = sampleCount(fileSize);
            Set<Long> sampledHeaders = new HashSet<>();
            for (int sample = 0; sample < samples; sample++) {
                byte[] blob = readBlobAfter(file, fileSize * sample / samples, sampledHeaders);
                if (blob.length > 0) {
                    blobs.add(blob);
                }
                if (blobs.size() == DECODE_BATCH_SIZE) {
                    countNodes(blobs, h3Core, nodeCounts);
                    blobs.clear();
                }
            }
        }
        countNodes(blobs, h3Core, nodeCounts);
        return nodeCounts;
    }

    static byte[] readBlobAfter(RandomAccessFile file, long offset, Set<Long> sampledHeaders) throws IOException {
        byte[] window = new byte[SEARCH_WINDOW];
        file.seek(offset);
        int length = 0;
        int read;
        while (length < window.length && (read = file.read(window, length, window.length - length)) > 0) {
            length += read;
        }
        for (int position = indexOf(window, length, 0); position >= 0; position = indexOf(window, length, position + 1)) {
            long headerOffset = offset + position;
            if (sampledHeaders.contains(headerOffset)) {
                return NO_BLOB;
            }
            byte[] blob = readBlob(file, headerOffset);
            if (blob.length > 0) {
                sampledHeaders.add(headerOffset);
                return blob;
            }
        }
        return NO_BLOB;
    }

    private static byte[] readBlob(RandomAccessFile file, long headerOffset) throws IOException {
        if (headerOffset < Integer.BYTES) {
            return NO_BLOB;
        }
        file.seek(headerOffset - Integer.BYTES);
        int headerLength = file.readInt();
        if (headerLength < OSM_DATA_BLOB_HEADER.length + 1 || headerLength > MAX_BLOB_HEADER_SIZE
                || headerOffset + headerLength > file.length()) {
            return NO_BLOB;
        }
        byte[] header = new byte[headerLength];
        file.readFully(header);
        Fileformat.BlobHeader blobHeader;
        try {
            blobHeader = Fileformat.BlobHeader.parseFrom(header);
        } catch (InvalidProtocolBufferException e) {
            return NO_BLOB;
        }
        long dataSize = blobHeader.getDatasize();
        if (!OSM_DATA.equals(blobHeader.getType()) || dataSize <= 0 || dataSize > MAX_BLOB_SIZE
                || file.getFilePointer() + dataSize > file.length()) {
            return NO_BLOB;
        }
        byte[] blob = new byte[(int) dataSize];
        file.readFully(blob);
        return blob;
    }

    private static int indexOf(byte[] window, int length, int from) {
        for (int i = from; i + OSM_DATA_BLOB_HEADER.length <= length; i++) {
            if (matchesBlobHeader(window, i)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean matchesBlobHeader(byte[] window, int position) {
        for (int j = 0; j < OSM_DATA_BLOB_HEADER.length; j++) {
            if (window[position + j] != OSM_DATA_BLOB_HEADER[j]) {
                return false;
            }
        }
        return true;
    }

    private static void countNodes(List<byte[]> blobs, H3Core h3Core, Map<Short, Long> nodeCounts) {
        blobs.parallelStream().forEach(blob -> countNodes(blob, h3Core, nodeCounts));
    }

    private static void countNodes(byte[] blob, H3Core h3Core, Map<Short, Long> nodeCounts) {
        Map<Short, Long> blobCounts = new HashMap<>();
        new PbfBlobDecoder(new RawBlob(OSM_DATA, blob), new PbfBlobDecoderListener() {
            @Override
            public void complete(List<EntityContainer> decodedEntities) {
                for (EntityContainer entityContainer : decodedEntities) {
                    if (entityContainer instanceof NodeContainer) {
                        Node node = ((NodeContainer) entityContainer).getEntity();
                        short h33 = CompactH3.serialize3(h3Core.latLngToCell(node.getLatitude(), node.getLongitude(), 3));
                        blobCounts.merge(h33, 1L, Long::sum);
                    }
                }
            }

            @Override
            public void error() {
                blobCounts.clear();
            }
        }).run();
        blobCounts.forEach((h33, count) -> nodeCounts.merge(h33, count, Long::sum));
    }
}
