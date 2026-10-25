package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.google.common.hash.Hashing;
import org.junit.jupiter.api.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WayGeometryGeomHashTest {

    private static List<WayNode> wayNodes(double... latitudeLongitude) {
        List<WayNode> wayNodes = new ArrayList<>();
        for (int i = 0; i < latitudeLongitude.length; i += 2) {
            wayNodes.add(new WayNode(i, latitudeLongitude[i], latitudeLongitude[i + 1]));
        }
        return wayNodes;
    }

    @Test
    void geomHash_pinnedValues() {
        assertEquals(0x9ae16a3b2f90404fL, WayGeometry.geomHash(wayNodes()));
        assertEquals(-1046846479977178601L, WayGeometry.geomHash(wayNodes(4.1750, 73.5090)));
        assertEquals(-7797190110283669506L, WayGeometry.geomHash(wayNodes(4.1750, 73.5090, 4.1755, 73.5095)));
        assertEquals(7380755542641947542L, WayGeometry.geomHash(wayNodes(4.1755, 73.5095, 4.1750, 73.5090)));
        assertEquals(-8686171279747987753L, WayGeometry.geomHash(wayNodes(-33.8688197, -151.2092955, -90.0, -180.0)));
        assertEquals(-8268226935895614569L, WayGeometry.geomHash(wayNodes(4.1750001, 73.5090, 4.1755, 73.5095)));
    }

    @Test
    void geomHash_fingerprintOfLittleEndianFixedPointLatitudeLongitude() {
        byte[] coordinates = {
                (byte) 0xF0, 0x0D, (byte) 0x7D, 0x02,
                (byte) 0x84, (byte) 0xA6, (byte) 0xD0, 0x2B};

        assertEquals(41750000, WayGeometry.fixedPoint(4.1750));
        assertEquals(735094404, WayGeometry.fixedPoint(73.5094404));
        assertEquals(Hashing.farmHashFingerprint64().hashBytes(coordinates).asLong(),
                WayGeometry.geomHash(wayNodes(4.1750, 73.5094404)));
    }

    @Test
    void geomHash_nodeOrderAndSmallestShiftChangeHash() {
        long hash = WayGeometry.geomHash(wayNodes(13.7563, 100.5018, 13.7570, 100.5030));

        assertNotEquals(hash, WayGeometry.geomHash(wayNodes(13.7570, 100.5030, 13.7563, 100.5018)));
        assertNotEquals(hash, WayGeometry.geomHash(wayNodes(13.7563001, 100.5018, 13.7570, 100.5030)));
        assertNotEquals(hash, WayGeometry.geomHash(wayNodes(13.7563, 100.5018, 13.7570, 100.5030, 13.7570, 100.5030)));
        assertEquals(hash, WayGeometry.geomHash(wayNodes(13.7563000000001, 100.5018, 13.7570, 100.5030)));
    }

    @Test
    void fixedPoint_restoresPbfIntegerFromOsmosisDouble() {
        for (long raw : new long[]{0, 1, -1, 41750000, -1512092955, 1800000000, -1800000000, 900000000, 123456789}) {
            double osmosisDegrees = (raw * 100) * 1e-9;
            assertEquals(raw, WayGeometry.fixedPoint(osmosisDegrees), "raw " + raw);
        }
    }
}
