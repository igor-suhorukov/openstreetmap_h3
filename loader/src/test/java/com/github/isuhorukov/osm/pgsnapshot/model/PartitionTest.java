package com.github.isuhorukov.osm.pgsnapshot.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PartitionTest {

    @Test
    void contains_valueAtMinRange_true() {
        Partition p = new Partition((short) 10, (short) 20);
        assertTrue(p.contains((short) 10));
    }

    @Test
    void contains_valueInsideRange_true() {
        Partition p = new Partition((short) 10, (short) 20);
        assertTrue(p.contains((short) 15));
    }

    @Test
    void contains_valueAtMaxRange_false() {
        Partition p = new Partition((short) 10, (short) 20);
        assertFalse(p.contains((short) 20));
    }

    @Test
    void contains_valueBelowMin_false() {
        Partition p = new Partition((short) 10, (short) 20);
        assertFalse(p.contains((short) 9));
    }

    @Test
    void fullConstructor_getters() {
        Partition p = new Partition(3, (short) 5, (short) 15, 9999L);
        assertEquals(3, p.getId());
        assertEquals((short) 5, p.getMinRange());
        assertEquals((short) 15, p.getMaxRange());
        assertEquals(9999L, p.getSerializedSize());
    }

    @Test
    void setters_updateValues() {
        Partition p = new Partition((short) 0, (short) 100);
        p.setId(7);
        p.setSerializedSize(42L);
        p.setH33RegionsInside(List.of((short) 10, (short) 20));

        assertEquals(7, p.getId());
        assertEquals(42L, p.getSerializedSize());
        assertEquals(2, p.getH33RegionsInside().size());
    }
}
