package com.github.isuhorukov.osm.pgsnapshot.pbfextractor;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.osmbinary.Fileformat;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoder;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoderListener;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class PbfBlobOffsets {
    private static final Logger log = LoggerFactory.getLogger(PbfBlobOffsets.class);

    public static void main(String[] args) throws Exception{
        final String pbfPath = "/home/iam/dev/map/planet-220704/planet-220704_loc_ways.pbf";
        long startParsing = System.currentTimeMillis();
        final Map<Long, Integer> offsets;
        try (FileInputStream pbfStream = new FileInputStream(pbfPath)) {
            offsets = getOffsets(pbfStream);
        }
        log.info("offset parsing took {} ms", System.currentTimeMillis() - startParsing);
        offsets.forEach((offset, blobSize) -> {
            final RawBlob rawBlob;
            try (FileInputStream blobStream = new FileInputStream(pbfPath)) {
                rawBlob = getBlob(blobStream, offset, blobSize);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            PbfBlobDecoder blobDecoder = new PbfBlobDecoder(rawBlob, new PbfBlobDecoderListener() {
                @Override
                public void complete(List<EntityContainer> decodedEntities) {
                    if (!(decodedEntities.get(0) instanceof NodeContainer)) {
                        log.info("non-node first entity: {}", decodedEntities.get(0).getClass().getName());
                    }
                }

                @Override
                public void error() {
                    // blob decoding errors are propagated via PbfBlobDecoder; nothing to do here
                }
            });
            blobDecoder.run();
        });
        log.info("blob count: {}", offsets.size());
    }

    public static RawBlob getBlob(InputStream blobInputStream, long offset, int blobSize) throws IOException {
        IOUtils.skipFully(blobInputStream, offset);
        return new RawBlob("OSMData", IOUtils.toByteArray(blobInputStream,  blobSize));
    }

    public static Map<Long, Integer> getOffsets(InputStream pbfStream) throws IOException {
        final CountingInputStream countingInputStream = new CountingInputStream(pbfStream);
        final Map<Long, Integer> offsets = new TreeMap<>();
        try (DataInputStream dis = new DataInputStream(countingInputStream)) {
            boolean moreBlobs = true;
            while (moreBlobs) {
                int headerLength;
                try {
                    headerLength = dis.readInt();
                } catch (EOFException endOfStream) {
                    moreBlobs = false;
                    continue;
                }
                Fileformat.BlobHeader blobHeader = readHeader(dis, headerLength);
                if ("OSMData".equals(blobHeader.getType())) {
                    offsets.put(countingInputStream.getByteCount(), blobHeader.getDatasize());
                }
                IOUtils.skipFully(dis, blobHeader.getDatasize());
            }
        }
        return offsets;
    }
    private static Fileformat.BlobHeader readHeader(DataInputStream dis, int headerLength) throws IOException {
        byte[] headerBuffer = new byte[headerLength];
        dis.readFully(headerBuffer);
        return Fileformat.BlobHeader.parseFrom(headerBuffer);
    }
}
