package com.github.isuhorukov.osm.pgsnapshot.model.table;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.Stat;

public class TableStat {
    StatType type;
    long blockId;
    short h33;
    long count;
    long size;
    long minId;
    long maxId;
    long lastModified;

    public TableStat(StatType type, long blockId, Stat stat) {
        this.type = type;
        this.blockId = blockId;
        this.h33 = stat.getH33();
        this.count = stat.getCount();
        this.size = stat.getSize();
        this.minId = stat.getMinId();
        this.maxId = stat.getMaxId();
        this.lastModified = stat.getLastModified();
    }

    public StatType getType() {
        return type;
    }

    public long getBlockId() {
        return blockId;
    }

    public short getH33() {
        return h33;
    }

    public long getCount() {
        return count;
    }

    public long getSize() {
        return size;
    }

    public long getMinId() {
        return minId;
    }

    public long getMaxId() {
        return maxId;
    }

    public long getLastModified() {
        return lastModified;
    }
}
