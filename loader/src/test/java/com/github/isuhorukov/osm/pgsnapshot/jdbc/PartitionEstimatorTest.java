package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openstreetmap.osmosis.osmbinary.Fileformat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class PartitionEstimatorTest {

    private static final Path OSMIUM = Paths.get("/usr/bin/osmium");

    @TempDir
    Path tempDir;

    @Test
    void sampleCount_boundedByFileSize() {
        assertEquals(64, PartitionEstimator.sampleCount(0));
        assertEquals(300, PartitionEstimator.sampleCount(300L << 20));
        assertEquals(2048, PartitionEstimator.sampleCount(90L << 30));
    }

    @Test
    void partitions_noSampledNodes_singlePartitionForAllCells() {
        List<Partition> partitions = PartitionEstimator.partitions(Map.of(), 0.48);

        assertEquals(1, partitions.size());
        assertEquals(0, partitions.get(0).getId());
        assertEquals(PartitionSplitter.LOWER_H3_3_BOUND, partitions.get(0).getMinRange());
        assertEquals(PartitionSplitter.HIGH_H3_3_BOUND, partitions.get(0).getMaxRange());
    }

    @Test
    void partitions_sampledNodes_contiguousPartitionsOverAllCells() {
        List<Partition> partitions = PartitionEstimator.partitions(
                Map.of((short) -31636, 100L, (short) 24898, 900L, (short) 25764, 1000L, (short) 26009, 700L), 0.48);

        assertTrue(partitions.size() > 1, partitions.toString());
        assertEquals(PartitionSplitter.LOWER_H3_3_BOUND, partitions.get(0).getMinRange());
        assertEquals(PartitionSplitter.HIGH_H3_3_BOUND, partitions.get(partitions.size() - 1).getMaxRange());
        for (int i = 1; i < partitions.size(); i++) {
            assertEquals(partitions.get(i - 1).getMaxRange(), partitions.get(i).getMinRange());
            assertEquals(i, partitions.get(i).getId());
        }
    }

    private static byte[] blob(byte[] data) throws Exception {
        byte[] header = Fileformat.BlobHeader.newBuilder().setType("OSMData").setDatasize(data.length).build().toByteArray();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        output.writeInt(header.length);
        output.write(header);
        output.write(data);
        return bytes.toByteArray();
    }

    @Test
    void readBlobAfter_falseMarkerAndGarbage_findsNextBlobOnce() throws Exception {
        byte[] marker = "\n\u0007OSMData\u0018".getBytes(StandardCharsets.ISO_8859_1);
        byte[] data = "compressed block".getBytes(StandardCharsets.US_ASCII);
        ByteArrayOutputStream content = new ByteArrayOutputStream();
        content.write(new byte[]{1, 2, 3, 4, 5});
        content.write(new byte[]{0x7f, 0x7f, 0x7f, 0x7f});
        content.write(marker);
        content.write(new byte[]{9, 9, 9});
        int blobOffset = content.size();
        content.write(blob(data));
        File file = tempDir.resolve("blobs.pbf").toFile();
        Files.write(file.toPath(), content.toByteArray());
        Set<Long> sampledHeaders = new HashSet<>();

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            assertArrayEquals(data, PartitionEstimator.readBlobAfter(randomAccessFile, 0, sampledHeaders));
            assertEquals(Set.of((long) blobOffset + Integer.BYTES), sampledHeaders);
            assertEquals(0, PartitionEstimator.readBlobAfter(randomAccessFile, 2, sampledHeaders).length);
            assertEquals(0, PartitionEstimator.readBlobAfter(randomAccessFile, blobOffset + 6, sampledHeaders).length);
        }
    }

    @Test
    void readBlobAfter_truncatedBlob_null() throws Exception {
        byte[] blob = blob(new byte[100]);
        File file = tempDir.resolve("truncated.pbf").toFile();
        Files.write(file.toPath(), Arrays.copyOf(blob, blob.length - 10));

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            assertEquals(0, PartitionEstimator.readBlobAfter(randomAccessFile, 0, new HashSet<>()).length);
        }
    }

    @Test
    void sampleNodeCounts_smallPbf_allNodesCounted() throws Exception {
        assumeTrue(Files.isExecutable(OSMIUM), OSMIUM + " is required");
        Path osm = tempDir.resolve("sample.osm");
        try (InputStream sample = getClass().getResourceAsStream("/pipeline/sample.osm")) {
            Files.copy(Objects.requireNonNull(sample), osm);
        }
        Path pbf = tempDir.resolve("sample.osm.pbf");
        Process process = new ProcessBuilder(OSMIUM.toString(), "cat", osm.toString(), "-o", pbf.toString())
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        assertEquals(0, process.waitFor());

        Map<Short, Long> nodeCounts = PartitionEstimator.sampleNodeCounts(pbf.toFile());

        assertEquals(14, nodeCounts.values().stream().mapToLong(Long::longValue).sum());
        assertFalse(PartitionEstimator.estimatePartitions(pbf.toFile(), 0.48).isEmpty());
    }
}
