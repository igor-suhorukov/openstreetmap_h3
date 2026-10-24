package com.github.isuhorukov.osm.pgsnapshot.model.table;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.Stat;

public class TableStat extends Stat {
    StatType type;
    long blockId;

    public TableStat(StatType type, long blockId, Stat stat) {
        super(stat);
        this.type = type;
        this.blockId = blockId;
    }

    public StatType getType() {
        return type;
    }

    public long getBlockId() {
        return blockId;
    }
}
