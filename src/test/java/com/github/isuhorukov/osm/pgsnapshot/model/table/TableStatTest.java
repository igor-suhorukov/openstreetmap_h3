package com.github.isuhorukov.osm.pgsnapshot.model.table;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.Stat;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TableStatTest {

    @Test
    void constructor_copiesAllStatFields() {
        Stat stat = new Stat((short) 5, 100L, 2048L, (short) 1, (short) 99, 99999L);
        TableStat ts = new TableStat(StatType.N, 7L, stat);

        assertEquals(StatType.N, ts.getType());
        assertEquals(7L, ts.getBlockId());
        assertEquals((short) 5, ts.getH33());
        assertEquals(100L, ts.getCount());
        assertEquals(2048L, ts.getSize());
        assertEquals(99999L, ts.getLastModified());
    }

    @Test
    void getMinId_getMaxId_fromStat() {
        Stat stat = new Stat((short) 0);
        stat.updateIdStat(10L);
        stat.updateIdStat(90L);
        TableStat ts = new TableStat(StatType.W, 1L, stat);

        assertEquals(10L, ts.getMinId());
        assertEquals(90L, ts.getMaxId());
    }
}
