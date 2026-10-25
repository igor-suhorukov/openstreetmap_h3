package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class EncodedBlock {

    public static final EncodedBlock EMPTY = new EncodedBlock(null, Collections.emptyMap(), null);

    private final File directory;
    private final Map<Short, byte[]> fileContents;
    private final BlockStat blockStat;

    private EncodedBlock(File directory, Map<Short, byte[]> fileContents, BlockStat blockStat) {
        this.directory = directory;
        this.fileContents = fileContents;
        this.blockStat = blockStat;
    }

    public static EncodedBlock encode(File directory, Map<Short, StringBuilder> csvResultPerH33, BlockStat blockStat) {
        Map<Short, byte[]> fileContents = new HashMap<>();
        for (Map.Entry<Short, StringBuilder> entry : csvResultPerH33.entrySet()) {
            fileContents.put(entry.getKey(), entry.getValue().toString().getBytes(StandardCharsets.UTF_8));
        }
        return new EncodedBlock(directory, fileContents, blockStat);
    }

    public File getDirectory() { return directory; }
    public Map<Short, byte[]> getFileContents() { return fileContents; }
    public BlockStat getBlockStat() { return blockStat; }
}
