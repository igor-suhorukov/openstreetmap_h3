package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.algorithm.locate.IndexedPointInAreaLocator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LatitudeBandsTest {

    private static final int ORIGIN_X = 989_900_000;
    private static final int ORIGIN_Y = 187_900_000;
    private static final GeometryFactory GRID = new GeometryFactory();

    @Test
    void locate_bandedAndPlainPiecesMatchJtsInsideOutsideAndOnBoundary() {
        List<int[][]> comb = List.of(comb(), circle(60, 50, 20, 16));
        List<int[][]> triangle = List.<int[][]>of(new int[][]{{300, 0}, {340, 0}, {320, 40}, {300, 0}});
        List<int[][]> large = List.<int[][]>of(circle(2000, 2000, 1000, 300));
        List<List<int[][]>> pieces = List.of(comb, triangle, large);
        IndexArrays arrays = IndexArrays.of(degrees(pieces), new int[]{0, 0, 1});
        LatitudeBands bands = LatitudeBands.of(arrays);

        assertTrue(bands.hasBands(0));
        assertTrue(bands.bandCount(0) > 1, "bands of the comb " + bands.bandCount(0));
        assertFalse(bands.hasBands(1));
        assertFalse(bands.hasBands(2));
        assertEquals(0, bands.bandCount(1));
        assertMatchesJts(bands, 0, comb, 1);
        assertMatchesJts(bands, 1, triangle, 1);
        assertMatchesJts(bands, 2, large, 7);
    }

    @Test
    void crossing_countsHalfOpenEdgesAndDetectsBoundary() {
        long a = IndexArrays.pack(0, 0);
        long b = IndexArrays.pack(10, 10);
        assertEquals(1, LatitudeBands.crossing(a, b, 0, 5));
        assertEquals(0, LatitudeBands.crossing(a, b, 8, 5));
        assertEquals(-1, LatitudeBands.crossing(a, b, 5, 5));
        assertEquals(0, LatitudeBands.crossing(a, b, 0, 10));
        assertEquals(-1, LatitudeBands.crossing(a, b, 10, 10));
        long c = IndexArrays.pack(20, 10);
        assertEquals(-1, LatitudeBands.crossing(b, c, 15, 10));
        assertEquals(0, LatitudeBands.crossing(b, c, 25, 10));
    }

    private static void assertMatchesJts(LatitudeBands bands, int piece, List<int[][]> rings, int step) {
        IndexedPointInAreaLocator locator = new IndexedPointInAreaLocator(gridPolygon(rings));
        int[] box = box(rings);
        int checked = 0;
        for (int x = box[0] - 3; x <= box[2] + 3; x += step) {
            for (int y = box[1] - 3; y <= box[3] + 3; y += step) {
                assertLocation(bands, piece, locator, x, y);
                checked++;
            }
        }
        for (int[][] ring : rings) {
            for (int[] vertex : ring) {
                for (int dx = -1; dx <= 1; dx++) {
                    for (int dy = -1; dy <= 1; dy++) {
                        assertLocation(bands, piece, locator, vertex[0] + dx, vertex[1] + dy);
                    }
                }
            }
        }
        assertTrue(checked > 1000, "checked " + checked);
    }

    private static void assertLocation(LatitudeBands bands, int piece, IndexedPointInAreaLocator locator, int x, int y) {
        int expected = expected(locator.locate(new Coordinate(x, y)));
        assertEquals(expected, bands.locate(piece, (long) ORIGIN_X + x, (long) ORIGIN_Y + y),
                "piece " + piece + " point " + x + " " + y);
    }

    private static int expected(int location) {
        if (location == Location.INTERIOR) {
            return CompactIndex.INTERIOR;
        }
        return location == Location.BOUNDARY ? CompactIndex.BOUNDARY : CompactIndex.EXTERIOR;
    }

    private static int[][] comb() {
        List<int[]> points = new ArrayList<>(List.of(new int[]{0, 0}, new int[]{200, 0}, new int[]{210, 75},
                new int[]{200, 150}));
        for (int k = 0; k < 10; k++) {
            int right = 200 - 20 * k - 5;
            int left = right - 10;
            points.add(new int[]{right, 150});
            points.add(new int[]{right, 100});
            points.add(new int[]{left, 100});
            points.add(new int[]{left, 150});
        }
        points.add(new int[]{0, 150});
        points.add(new int[]{0, 0});
        return points.toArray(new int[0][]);
    }

    private static int[][] circle(int centreX, int centreY, int radius, int vertices) {
        int[][] ring = new int[vertices + 1][];
        for (int i = 0; i < vertices; i++) {
            double angle = 2 * Math.PI * i / vertices;
            ring[i] = new int[]{centreX + (int) Math.round(radius * Math.cos(angle)),
                    centreY + (int) Math.round(radius * Math.sin(angle))};
        }
        ring[vertices] = ring[0];
        return ring;
    }

    private static int[] box(List<int[][]> rings) {
        int[] box = {Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
        for (int[][] ring : rings) {
            for (int[] vertex : ring) {
                box[0] = Math.min(box[0], vertex[0]);
                box[1] = Math.min(box[1], vertex[1]);
                box[2] = Math.max(box[2], vertex[0]);
                box[3] = Math.max(box[3], vertex[1]);
            }
        }
        return box;
    }

    private static List<Polygon> degrees(List<List<int[][]>> pieces) {
        List<Polygon> polygons = new ArrayList<>();
        for (List<int[][]> rings : pieces) {
            polygons.add(polygon(GridGeometry.FACTORY, rings, ORIGIN_X, ORIGIN_Y, GridGeometry.SCALE));
        }
        return polygons;
    }

    private static Polygon gridPolygon(List<int[][]> rings) {
        return polygon(GRID, rings, 0, 0, 1);
    }

    private static Polygon polygon(GeometryFactory factory, List<int[][]> rings, int originX, int originY,
                                   double scale) {
        LinearRing[] linearRings = new LinearRing[rings.size()];
        for (int r = 0; r < rings.size(); r++) {
            int[][] ring = rings.get(r);
            Coordinate[] coordinates = new Coordinate[ring.length];
            for (int i = 0; i < ring.length; i++) {
                coordinates[i] = new Coordinate((originX + (long) ring[i][0]) / scale,
                        (originY + (long) ring[i][1]) / scale);
            }
            linearRings[r] = factory.createLinearRing(coordinates);
        }
        LinearRing[] holes = new LinearRing[linearRings.length - 1];
        System.arraycopy(linearRings, 1, holes, 0, holes.length);
        return factory.createPolygon(linearRings[0], holes);
    }
}
