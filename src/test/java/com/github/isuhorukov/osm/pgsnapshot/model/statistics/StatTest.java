package com.github.isuhorukov.osm.pgsnapshot.model.statistics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StatTest {

    @Test
    void constructor_shortOnly_defaultsMinMaxIds() {
        Stat s = new Stat((short) 7);
        assertEquals((short) 7, s.getH33());
        assertEquals(Long.MAX_VALUE, s.getMinId());
        assertEquals(Long.MIN_VALUE, s.getMaxId());
        assertEquals(0, s.getCount());
        assertEquals(0, s.getSize());
    }

    @Test
    void fullConstructor_setsAllFields() {
        Stat s = new Stat((short) 3, 10L, 200L, (short) 1, (short) 9, 12345L);
        assertEquals(10L, s.getCount());
        assertEquals(200L, s.getSize());
        assertEquals(12345L, s.getLastModified());
    }

    @Test
    void incrementCount_increasesCountByOne() {
        Stat s = new Stat((short) 0);
        s.incrementCount();
        s.incrementCount();
        assertEquals(2, s.getCount());
    }

    @Test
    void updateIdStat_tracksMinAndMax() {
        Stat s = new Stat((short) 0);
        s.updateIdStat(50L);
        s.updateIdStat(10L);
        s.updateIdStat(100L);
        assertEquals(10L, s.getMinId());
        assertEquals(100L, s.getMaxId());
    }

    @Test
    void updateLastModified_keepsMaximum() {
        Stat s = new Stat((short) 0);
        s.updateLastModified(100L);
        s.updateLastModified(50L);
        s.updateLastModified(200L);
        assertEquals(200L, s.getLastModified());
    }

    @Test
    void incrementSize_accumulatesSize() {
        Stat s = new Stat((short) 0);
        s.incrementSize(100);
        s.incrementSize(50);
        assertEquals(150L, s.getSize());
    }
}
