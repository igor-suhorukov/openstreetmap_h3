package com.github.isuhorukov.osm.pgsnapshot.pbfextractor;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.osmbinary.Fileformat;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoder;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoderListener;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class PbfBlobOffsets {
    public static void main(String[] args) throws Exception{
        final String pbfPath = "/home/iam/dev/map/planet-220704/planet-220704_loc_ways.pbf";
        long startParsing = System.currentTimeMillis();
        final Map<Long, Integer> offsets = getOffsets(new FileInputStream(pbfPath));
        System.out.println(System.currentTimeMillis()-startParsing);
        offsets.forEach((offset, blobSize) -> {
            long start = System.currentTimeMillis();
            final RawBlob rawBlob;
            try {
                rawBlob = getBlob(new FileInputStream(pbfPath), offset, blobSize);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            PbfBlobDecoder blobDecoder = new PbfBlobDecoder(rawBlob, new PbfBlobDecoderListener() {
                @Override
                public void complete(List<EntityContainer> decodedEntities) {
                    if (!(decodedEntities.get(0) instanceof NodeContainer)) {
                        System.out.println(decodedEntities.get(0).getClass().getName());
                    }
                    int i=0;
                }

                @Override
                public void error() {

                }
            });
            blobDecoder.run();
            //System.out.println(System.currentTimeMillis()-start);
        });
        System.out.println(offsets.size());
    }

    public static RawBlob getBlob(InputStream blobInputStream, long offset, int blobSize) throws IOException {
        blobInputStream.skip(offset);
        return new RawBlob("OSMData", IOUtils.toByteArray(blobInputStream,  blobSize));
    }

    public static Map<Long, Integer> getOffsets(InputStream pbfStream) throws IOException {
        final CountingInputStream countingInputStream = new CountingInputStream(pbfStream);
        DataInputStream dis = new DataInputStream(countingInputStream);
        final Map<Long, Integer> offsets = new TreeMap<>();
        while (true){
            int headerLength;
            try {
                headerLength = dis.readInt();
            } catch (EOFException e) {
                dis.close();
                return offsets;
            }
            Fileformat.BlobHeader blobHeader = readHeader(dis, headerLength);
            if("OSMData".equals(blobHeader.getType())){
                offsets.put(countingInputStream.getByteCount(), blobHeader.getDatasize());
            }
            dis.skip(blobHeader.getDatasize());
        }
    }
    private static Fileformat.BlobHeader readHeader(DataInputStream dis, int headerLength) throws IOException {
        byte[] headerBuffer = new byte[headerLength];
        dis.readFully(headerBuffer);

        Fileformat.BlobHeader blobHeader = Fileformat.BlobHeader.parseFrom(headerBuffer);

        return blobHeader;
    }
}
