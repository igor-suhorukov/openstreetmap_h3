package com.github.isuhorukov.osm.pgsnapshot;

import org.apache.commons.io.FileUtils;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.StreamSplitter;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class Splitter {
    public static class Blocks{
        String directory;
        int blobCount;
        long pbfSplitTime;
        long addLocationsToWaysTime;

        public Blocks(String directory, int blobCount, long pbfSplitTime, long addLocationsToWaysTime) {
            this.directory = directory;
            this.blobCount = blobCount;
            this.pbfSplitTime = pbfSplitTime;
            this.addLocationsToWaysTime = addLocationsToWaysTime;
        }

        public String getDirectory() {
            return directory;
        }

        public int getBlobCount() {
            return blobCount;
        }

        public long getPbfSplitTime() {
            return pbfSplitTime;
        }

        public long getAddLocationsToWaysTime() {
            return addLocationsToWaysTime;
        }
    }
    public static void main(String[] args) throws Exception{
        File sourceFile = new File("/home/iam/dev/map/maldives/maldives-latest.osm.pbf");
        Blocks blocks = splitPbfByBlocks(sourceFile,0);
        System.out.println("Blobs: "+blocks.getBlobCount());
    }

    public static Blocks splitPbfByBlocks(File sourceFile, long addLocationsTime) throws IOException {
        String blockDirectoryName = getBlockDirectoryName(sourceFile);
        new File(blockDirectoryName).mkdirs();
        final long pbfSplitStart = System.currentTimeMillis();
        int blobCount=0;
        try (FileInputStream inputStream = new FileInputStream(sourceFile);
             StreamSplitter streamSplitter = new StreamSplitter(new DataInputStream(inputStream));){
            //Osmformat.HeaderBlock header = new HeaderSeeker().apply(streamSplitter);
            while (streamSplitter.hasNext()) {
                RawBlob rawBlob = streamSplitter.next();
                if("OSMData".equals(rawBlob.getType())){
                    File blobFile = new File(blockDirectoryName, String.format("%08d", blobCount++));
                    FileUtils.writeByteArrayToFile(blobFile, rawBlob.getData(), false);
                }
            }
        }
        return new Blocks(blockDirectoryName, blobCount, System.currentTimeMillis() - pbfSplitStart, addLocationsTime);
    }

    public static String getBlockDirectoryName(File enrichedPbfName) {
        return enrichedPbfName.getAbsolutePath().replace(".pbf", "") + "_blocks";
    }
}
