package com.github.isuhorukov.osm.pgsnapshot.output;

public final class SpillChunk {

    private final byte[] bytes;
    private final long firstId;
    private final long lastId;

    public SpillChunk(byte[] bytes, long firstId, long lastId) {
        this.bytes = bytes;
        this.firstId = firstId;
        this.lastId = lastId;
    }

    public byte[] getBytes() { return bytes; }
    public long getFirstId() { return firstId; }
    public long getLastId() { return lastId; }

    public static IllegalStateException outOfOrder(String entity, short h33, long id, long previousId) {
        return new IllegalStateException(String.format("%s %d in h3_3 cell %d follows id %d: "
                + "the Parquet export needs a PBF sorted by type and id (osmium sort)", entity, id, h33, previousId));
    }
}
