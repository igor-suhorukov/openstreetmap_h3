package com.github.isuhorukov.osm.pgsnapshot.pbfextractor;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openstreetmap.osmosis.osmbinary.Fileformat;
import org.openstreetmap.osmosis.osmbinary.Osmformat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.zip.Deflater;

import static com.github.isuhorukov.osm.pgsnapshot.pbfextractor.ReplicationHeader.Freshness.*;
import static org.junit.jupiter.api.Assertions.*;

class ReplicationHeaderTest {

    private static final String GEOFABRIK = "https://download.geofabrik.de/asia/thailand-updates";
    private static final Instant SEPTEMBER_4 = Instant.parse("2026-09-04T20:21:02Z");

    @TempDir
    Path tempDir;

    private File pbf(String type, Fileformat.Blob blob) throws IOException {
        byte[] blobBytes = blob.toByteArray();
        byte[] header = Fileformat.BlobHeader.newBuilder().setType(type).setDatasize(blobBytes.length).build().toByteArray();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(header.length);
            output.write(header);
            output.write(blobBytes);
        }
        Path file = Files.createTempFile(tempDir, "header", ".osm.pbf");
        Files.write(file, bytes.toByteArray());
        return file.toFile();
    }

    private static Osmformat.HeaderBlock replicationHeader() {
        return Osmformat.HeaderBlock.newBuilder().addRequiredFeatures("OsmSchema-V0.6")
                .setOsmosisReplicationTimestamp(SEPTEMBER_4.getEpochSecond())
                .setOsmosisReplicationSequenceNumber(4881)
                .setOsmosisReplicationBaseUrl(GEOFABRIK).build();
    }

    private static byte[] zlib(byte[] data) {
        Deflater deflater = new Deflater();
        deflater.setInput(data);
        deflater.finish();
        byte[] buffer = new byte[data.length + 64];
        int length = deflater.deflate(buffer);
        deflater.end();
        return java.util.Arrays.copyOf(buffer, length);
    }

    @Test
    void read_zlibHeaderBlock_replicationFields() throws IOException {
        byte[] header = replicationHeader().toByteArray();
        File file = pbf("OSMHeader", Fileformat.Blob.newBuilder().setRawSize(header.length)
                .setZlibData(ByteString.copyFrom(zlib(header))).build());

        ReplicationHeader replication = ReplicationHeader.read(file);

        assertEquals(SEPTEMBER_4, replication.getTimestamp());
        assertEquals(4881L, replication.getSequence());
        assertEquals(GEOFABRIK, replication.getBaseUrl());
        assertEquals("sequence 4881, timestamp 2026-09-04T20:21:02Z, url " + GEOFABRIK, replication.toString());
    }

    @Test
    void read_rawHeaderBlockWithoutReplication_nullFields() throws IOException {
        File file = pbf("OSMHeader", Fileformat.Blob.newBuilder().setRaw(Osmformat.HeaderBlock.newBuilder()
                .addRequiredFeatures("OsmSchema-V0.6").build().toByteString()).build());

        ReplicationHeader replication = ReplicationHeader.read(file);

        assertNull(replication.getTimestamp());
        assertNull(replication.getSequence());
        assertNull(replication.getBaseUrl());
    }

    @Test
    void read_unsupportedCompression_replicationIgnored() throws IOException {
        File file = pbf("OSMHeader", Fileformat.Blob.newBuilder().setRawSize(10)
                .setLzmaData(ByteString.copyFrom(new byte[]{1, 2, 3})).build());

        assertSame(ReplicationHeader.NONE, ReplicationHeader.read(file));
    }

    @Test
    void read_firstBlobIsData_throws() throws IOException {
        File file = pbf("OSMData", Fileformat.Blob.newBuilder().setRaw(ByteString.copyFrom(new byte[]{1})).build());

        IOException exception = assertThrows(IOException.class, () -> ReplicationHeader.read(file));
        assertTrue(exception.getMessage().contains("is OSMData, not OSMHeader"), exception.getMessage());
    }

    @Test
    void read_notPbf_throws() throws IOException {
        Path file = Files.writeString(tempDir.resolve("text.osm.pbf"), "<osm version=\"0.6\"></osm>");

        assertThrows(IOException.class, () -> ReplicationHeader.read(file.toFile()));
    }

    @Test
    void blobData_corruptedZlib_throws() {
        Fileformat.Blob blob = Fileformat.Blob.newBuilder().setRawSize(10)
                .setZlibData(ByteString.copyFrom(new byte[]{1, 2, 3, 4})).build();

        assertThrows(IOException.class, () -> ReplicationHeader.blobData(blob));
    }

    @Test
    void freshnessAgainst_sameReplicationUrl_sequenceDecides() {
        ReplicationHeader stored = new ReplicationHeader(SEPTEMBER_4, 4881L, GEOFABRIK);

        assertEquals(NEWER, new ReplicationHeader(SEPTEMBER_4.plusSeconds(86400), 4882L, GEOFABRIK).freshnessAgainst(stored));
        assertEquals(OLDER, new ReplicationHeader(SEPTEMBER_4.minusSeconds(86400), 4880L, GEOFABRIK).freshnessAgainst(stored));
        assertEquals(SAME, new ReplicationHeader(SEPTEMBER_4, 4881L, GEOFABRIK).freshnessAgainst(stored));
        assertEquals(NEWER, new ReplicationHeader(SEPTEMBER_4.plusSeconds(1), 4881L, GEOFABRIK).freshnessAgainst(stored));
        assertEquals(SAME, new ReplicationHeader(null, 4881L, GEOFABRIK).freshnessAgainst(stored));
    }

    @Test
    void freshnessAgainst_otherReplicationUrlOrNoSequence_timestampDecides() {
        ReplicationHeader stored = new ReplicationHeader(SEPTEMBER_4, 4881L, GEOFABRIK);

        assertEquals(OLDER, new ReplicationHeader(SEPTEMBER_4.minusSeconds(60), 7000000L,
                "https://planet.openstreetmap.org/replication/minute").freshnessAgainst(stored));
        assertEquals(NEWER, new ReplicationHeader(SEPTEMBER_4.plusSeconds(60), null, null).freshnessAgainst(stored));
        assertEquals(SAME, new ReplicationHeader(SEPTEMBER_4, null, null).freshnessAgainst(stored));
    }

    @Test
    void freshnessAgainst_noComparableFields_unknown() {
        assertEquals(UNKNOWN, ReplicationHeader.NONE.freshnessAgainst(new ReplicationHeader(SEPTEMBER_4, 4881L, GEOFABRIK)));
        assertEquals(UNKNOWN, new ReplicationHeader(null, 4882L, GEOFABRIK).freshnessAgainst(ReplicationHeader.NONE));
        assertEquals(UNKNOWN, new ReplicationHeader(null, 4882L, GEOFABRIK)
                .freshnessAgainst(new ReplicationHeader(null, 4881L, "https://other.example/updates")));
    }
}
