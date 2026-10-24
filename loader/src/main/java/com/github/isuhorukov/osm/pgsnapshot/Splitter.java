package com.github.isuhorukov.osm.pgsnapshot;

import org.apache.commons.io.FileUtils;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.StreamSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.function.ObjLongConsumer;

public class Splitter {
    private static final Logger log = LoggerFactory.getLogger(Splitter.class);

    public static void main(String[] args) throws Exception{
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: Splitter <source pbf file>");
        }
        File sourceFile = new File(args[0]);
        File blockDirectory = new File(getBlockDirectoryName(sourceFile));
        blockDirectory.mkdirs();
        try (InputStream inputStream = new FileInputStream(sourceFile)) {
            int blobCount = splitPbfByBlocks(inputStream, (rawBlob, blockNumber) -> {
                File blobFile = new File(blockDirectory, String.format("%08d", blockNumber));
                try {
                    FileUtils.writeByteArrayToFile(blobFile, rawBlob.getData(), false);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            log.info("Blobs: {}", blobCount);
        }
    }

    public static int splitPbfByBlocks(InputStream pbfStream, ObjLongConsumer<RawBlob> blockConsumer) {
        int blobCount=0;
        try (StreamSplitter streamSplitter = new StreamSplitter(new DataInputStream(pbfStream))){
            while (streamSplitter.hasNext()) {
                RawBlob rawBlob = streamSplitter.next();
                if("OSMData".equals(rawBlob.getType())){
                    blockConsumer.accept(rawBlob, blobCount++);
                }
            }
        }
        return blobCount;
    }

    public static String getBlockDirectoryName(File pbfFile) {
        return pbfFile.getAbsolutePath().replace(".pbf", "") + "_blocks";
    }
}
