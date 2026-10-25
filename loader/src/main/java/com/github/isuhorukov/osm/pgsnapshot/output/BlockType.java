package com.github.isuhorukov.osm.pgsnapshot.output;

public enum BlockType {
    NODES(ResultLayout.NODES_DIR),
    WAYS(ResultLayout.WAYS_DIR),
    RELATIONS(ResultLayout.RELATIONS_DIR);

    private final String directoryName;

    BlockType(String directoryName) {
        this.directoryName = directoryName;
    }

    public String getDirectoryName() {
        return directoryName;
    }

    public static BlockType of(long nodeCount, long wayCount, long relationCount) {
        if (nodeCount > 0) {
            return NODES;
        }
        if (wayCount > 0) {
            return WAYS;
        }
        if (relationCount > 0) {
            return RELATIONS;
        }
        throw new IllegalArgumentException("unknown block type");
    }
}
