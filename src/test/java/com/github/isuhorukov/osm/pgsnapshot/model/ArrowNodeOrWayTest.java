package com.github.isuhorukov.osm.pgsnapshot.model;

import org.junit.jupiter.api.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ArrowNodeOrWayTest {

    private ArrowNodeOrWay node(java.util.Collection<Tag> tags) {
        return new ArrowNodeOrWay(1L, (short) 0, 0, 10.0, 20.0, tags);
    }

    @Test
    void isBuilding_withBuildingTag_true() {
        ArrowNodeOrWay n = node(List.of(new Tag("building", "yes")));
        assertTrue(n.isBuilding());
    }

    @Test
    void isBuilding_withoutBuildingTag_false() {
        ArrowNodeOrWay n = node(List.of(new Tag("highway", "road")));
        assertFalse(n.isBuilding());
    }

    @Test
    void isBuilding_emptyTags_false() {
        ArrowNodeOrWay n = node(Collections.emptyList());
        assertFalse(n.isBuilding());
    }

    @Test
    void isHighway_withHighwayTag_true() {
        ArrowNodeOrWay n = node(List.of(new Tag("highway", "primary")));
        assertTrue(n.isHighway());
    }

    @Test
    void isHighway_withoutHighwayTag_false() {
        ArrowNodeOrWay n = node(List.of(new Tag("name", "Main St")));
        assertFalse(n.isHighway());
    }

    @Test
    void isHighway_emptyTags_false() {
        ArrowNodeOrWay n = node(Collections.emptyList());
        assertFalse(n.isHighway());
    }

    @Test
    void builder_setsAllFields() {
        ArrowNodeOrWay obj = new ArrowNodeOrWay.Builder(42L, (short) 7, 99, 51.5, 0.1,
                List.of(new Tag("amenity", "cafe")))
                .closed(true)
                .scaleDim(1.5f)
                .bboxMinX(-1.0)
                .bboxMaxX(1.0)
                .bboxMinY(-2.0)
                .bboxMaxY(2.0)
                .h33Center((short) 3)
                .pointIdxs(new long[]{10L, 20L})
                .lineStringWkb(new byte[]{0x01})
                .h38Indexes(new int[]{5, 6})
                .bboxWkb(new byte[]{0x02})
                .build();

        assertEquals(42L, obj.getId());
        assertEquals((short) 7, obj.getH33());
        assertEquals(99, obj.getH38());
        assertEquals(51.5, obj.getLatitude());
        assertEquals(0.1, obj.getLongitude());
        assertTrue(obj.isClosed());
        assertEquals(1.5f, obj.getScaleDim());
        assertEquals(-1.0, obj.getBboxMinX());
        assertEquals(1.0, obj.getBboxMaxX());
        assertEquals(-2.0, obj.getBboxMinY());
        assertEquals(2.0, obj.getBboxMaxY());
        assertEquals((short) 3, obj.getH33Center());
        assertArrayEquals(new long[]{10L, 20L}, obj.getPointIdxs());
        assertArrayEquals(new byte[]{0x01}, obj.getLineStringWkb());
        assertArrayEquals(new int[]{5, 6}, obj.getH38Indexes());
        assertArrayEquals(new byte[]{0x02}, obj.getBboxWkb());
        assertTrue(obj.isBuilding() == false);
        assertTrue(obj.getTags().containsKey("amenity"));
    }
}
