package com.github.isuhorukov.osm.pgsnapshot.util;

import com.uber.h3core.H3Core;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class CompactH3Test {

    private static H3Core h3;

    @BeforeAll
    static void init() throws IOException {
        h3 = H3Core.newInstance();
    }

    @Test
    void roundTrip_resolution3() {
        long cell = h3.latLngToCell(4.0, 73.0, 3); // Maldives area
        long compact = CompactH3.toShort3(cell);
        long restored = CompactH3.toFull3(compact);
        assertEquals(cell, restored);
    }

    @Test
    void roundTrip_resolution8() {
        long cell = h3.latLngToCell(4.0, 73.0, 8);
        long compact = CompactH3.toShort8(cell);
        long restored = CompactH3.toFull8(compact);
        assertEquals(cell, restored);
    }

    @Test
    void serialize3_returnsShortFromCompact() {
        long cell = h3.latLngToCell(4.0, 73.0, 3);
        short s = CompactH3.serialize3(cell);
        assertEquals((short) CompactH3.toShort3(cell), s);
    }

    @Test
    void serialize8_returnsIntFromCompact() {
        long cell = h3.latLngToCell(4.0, 73.0, 8);
        int i = CompactH3.serialize8(cell);
        assertEquals((int) CompactH3.toShort8(cell), i);
    }

    @Test
    void serializeShort_truncatesToShort() {
        assertEquals((short) 0x1234, CompactH3.serializeShort(0x1234L));
    }

    @Test
    void serializeInt_truncatesToInt() {
        assertEquals(0x12345678, CompactH3.serializeInt(0x12345678L));
    }
}
