package com.github.isuhorukov.osm.pgsnapshot.pbfextractor;

import org.openstreetmap.osmosis.osmbinary.Fileformat;
import org.openstreetmap.osmosis.osmbinary.Osmformat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public final class ReplicationHeader {

    public static final ReplicationHeader NONE = new ReplicationHeader(null, null, null);

    public enum Freshness { OLDER, SAME, NEWER, UNKNOWN }

    private static final Logger log = LoggerFactory.getLogger(ReplicationHeader.class);
    private static final String OSM_HEADER = "OSMHeader";
    private static final int MAX_BLOB_HEADER_SIZE = 1 << 16;
    private static final int MAX_BLOB_SIZE = 1 << 25;

    private final Instant timestamp;
    private final Long sequence;
    private final String baseUrl;

    public ReplicationHeader(Instant timestamp, Long sequence, String baseUrl) {
        this.timestamp = timestamp;
        this.sequence = sequence;
        this.baseUrl = baseUrl;
    }

    public static ReplicationHeader read(File pbfFile) throws IOException {
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(new FileInputStream(pbfFile)))) {
            int headerLength = input.readInt();
            if (headerLength <= 0 || headerLength > MAX_BLOB_HEADER_SIZE) {
                throw new IOException("Invalid PBF blob header size " + headerLength + " in " + pbfFile);
            }
            Fileformat.BlobHeader blobHeader = Fileformat.BlobHeader.parseFrom(readBytes(input, headerLength));
            if (!OSM_HEADER.equals(blobHeader.getType())) {
                throw new IOException("The first PBF blob of " + pbfFile + " is " + blobHeader.getType() + ", not " + OSM_HEADER);
            }
            if (blobHeader.getDatasize() <= 0 || blobHeader.getDatasize() > MAX_BLOB_SIZE) {
                throw new IOException("Invalid " + OSM_HEADER + " blob size " + blobHeader.getDatasize() + " in " + pbfFile);
            }
            Fileformat.Blob blob = Fileformat.Blob.parseFrom(readBytes(input, blobHeader.getDatasize()));
            if (!blob.hasRaw() && !blob.hasZlibData()) {
                log.warn("{} block of {} is compressed with an unsupported method, replication fields are ignored",
                        OSM_HEADER, pbfFile);
                return NONE;
            }
            return of(Osmformat.HeaderBlock.parseFrom(blobData(blob)));
        }
    }

    static ReplicationHeader of(Osmformat.HeaderBlock header) {
        return new ReplicationHeader(
                header.hasOsmosisReplicationTimestamp() ? Instant.ofEpochSecond(header.getOsmosisReplicationTimestamp()) : null,
                header.hasOsmosisReplicationSequenceNumber() ? header.getOsmosisReplicationSequenceNumber() : null,
                header.hasOsmosisReplicationBaseUrl() ? header.getOsmosisReplicationBaseUrl() : null);
    }

    static byte[] blobData(Fileformat.Blob blob) throws IOException {
        if (blob.hasRaw()) {
            return blob.getRaw().toByteArray();
        }
        if (!blob.hasZlibData()) {
            throw new IOException("Unsupported compression of " + OSM_HEADER + " block");
        }
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(blob.getZlibData().toByteArray());
            byte[] data = new byte[blob.getRawSize()];
            int length = inflater.inflate(data);
            if (length != data.length || !inflater.finished()) {
                throw new IOException("Corrupted zlib data of " + OSM_HEADER + " block");
            }
            return data;
        } catch (DataFormatException e) {
            throw new IOException("Corrupted zlib data of " + OSM_HEADER + " block", e);
        } finally {
            inflater.end();
        }
    }

    private static byte[] readBytes(DataInputStream input, int length) throws IOException {
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return bytes;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Long getSequence() {
        return sequence;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public Freshness freshnessAgainst(ReplicationHeader stored) {
        boolean sequencesComparable = sequence != null && stored.sequence != null && Objects.equals(baseUrl, stored.baseUrl);
        boolean timestampsComparable = timestamp != null && stored.timestamp != null;
        int timestampOrder = timestampsComparable ? timestamp.compareTo(stored.timestamp) : 0;
        int order;
        if (sequencesComparable) {
            order = Long.compare(sequence, stored.sequence);
        } else if (timestampsComparable) {
            order = timestampOrder;
        } else {
            return Freshness.UNKNOWN;
        }
        if (order == 0) {
            order = timestampOrder;
        }
        if (order == 0) {
            return Freshness.SAME;
        }
        return order < 0 ? Freshness.OLDER : Freshness.NEWER;
    }

    @Override
    public String toString() {
        return "sequence " + sequence + ", timestamp " + timestamp + ", url " + baseUrl;
    }
}
