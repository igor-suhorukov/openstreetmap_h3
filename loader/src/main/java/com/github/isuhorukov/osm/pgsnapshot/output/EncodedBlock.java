package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public final class EncodedBlock {

    public static final EncodedBlock EMPTY = new EncodedBlock(null, Collections.emptyMap(), null);

    private final BlockType type;
    private final Map<Short, byte[]> contents;
    private final BlockStat blockStat;

    private EncodedBlock(BlockType type, Map<Short, byte[]> contents, BlockStat blockStat) {
        this.type = type;
        this.contents = contents;
        this.blockStat = blockStat;
    }

    public static EncodedBlock encode(BlockType type, Map<Short, StringBuilder> csvResultPerH33, BlockStat blockStat) {
        Map<Short, byte[]> contents = new TreeMap<>();
        for (Map.Entry<Short, StringBuilder> entry : csvResultPerH33.entrySet()) {
            contents.put(entry.getKey(), entry.getValue().toString().getBytes(StandardCharsets.UTF_8));
        }
        return new EncodedBlock(type, contents, blockStat);
    }

    public BlockType getType() { return type; }
    public Map<Short, byte[]> getContents() { return contents; }
    public BlockStat getBlockStat() { return blockStat; }
}
