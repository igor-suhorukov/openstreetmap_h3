package com.github.isuhorukov.osm.pgsnapshot.util;

import com.uber.h3core.H3Core;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class GeometryH33Test {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final double WGS84_RADIUS = (2.0 * 6378137.0 + (6378137.0 - 6378137.0 / 298.257223563)) / 3.0;
    private static H3Core h3Core;
    private static GeometryH33 geometryH33;

    @BeforeAll
    static void load() throws IOException {
        h3Core = H3Core.newInstance();
        geometryH33 = GeometryH33.instance();
    }

    @Test
    void cells_polygonInsideOneCell_thatCell() {
        short[] cells = geometryH33.cells(square(98.99, 18.79, 0.001));

        assertArrayEquals(new short[]{cell(18.79, 98.99)}, cells);
        assertEquals(cell(18.79, 98.99), GeometryH33.h33(cells));
    }

    @Test
    void cells_polygonAcrossCells_sortedCellsAndMultipleCells() {
        short[] cells = geometryH33.cells(square(98.0, 18.0, 2.0));

        assertTrue(cells.length > 1, Arrays.toString(cells));
        short[] sorted = cells.clone();
        Arrays.sort(sorted);
        assertArrayEquals(sorted, cells);
        assertTrue(contains(cells, cell(18.0, 98.0)) && contains(cells, cell(20.0, 100.0)), Arrays.toString(cells));
        assertEquals(GeometryH33.MULTIPLE_CELLS, GeometryH33.h33(cells));
    }

    @Test
    void cells_multipolygonPartsInDifferentCells_bothCellsSorted() {
        short chiangMai = cell(18.79, 98.99);
        short moscow = cell(55.75, 37.62);

        short[] cells = geometryH33.cells(GEOMETRY_FACTORY.createMultiPolygon(new Polygon[]{
                square(98.99, 18.79, 0.001), square(37.62, 55.75, 0.001)}));

        assertArrayEquals(chiangMai < moscow ? new short[]{chiangMai, moscow} : new short[]{moscow, chiangMai}, cells);
        assertEquals(GeometryH33.MULTIPLE_CELLS, GeometryH33.h33(cells));
    }

    @Test
    void cells_emptyGeometry_noCellsAndMultipleCells() {
        short[] cells = geometryH33.cells(GEOMETRY_FACTORY.createPolygon());

        assertArrayEquals(new short[0], cells);
        assertEquals(GeometryH33.MULTIPLE_CELLS, GeometryH33.h33(cells));
    }

    @Test
    void cells_squareBetweenStraightAndGeodesicCellEdge_geodesicCell() {
        Polygon square = rectangle(37.50176, 55.50852, 37.50216, 55.50872);

        assertArrayEquals(new short[]{4481}, geometryH33.cells(square));
        assertTrue(straightBounds((short) 4522).covers(square));
        assertEquals(4481, cell(55.50862, 37.50196));
    }

    @Test
    void cells_parallelCallsOnColdIndex_sameAsSequential() throws IOException {
        GeometryH33 cold;
        try (InputStream cellPolygons = GeometryH33.class.getResourceAsStream(GeometryH33.CELL_POLYGONS)) {
            cold = GeometryH33.read(cellPolygons);
        }
        List<Polygon> squares = new ArrayList<>();
        for (double longitude = 97.0; longitude < 106.0; longitude += 0.37) {
            for (double latitude = 5.5; latitude < 20.5; latitude += 0.41) {
                squares.add(square(longitude, latitude, 0.3));
            }
        }

        List<String> parallel = squares.parallelStream().map(square -> Arrays.toString(cold.cells(square)))
                .collect(Collectors.toList());

        assertEquals(squares.stream().map(square -> Arrays.toString(geometryH33.cells(square)))
                .collect(Collectors.toList()), parallel);
    }

    @Test
    void read_resource_allCellPolygons() {
        Set<Short> ids = new HashSet<>();
        for (int row = 0; row < geometryH33.size(); row++) {
            ids.add(geometryH33.id(row));
        }

        assertEquals(41380, geometryH33.size());
        assertEquals(41162, ids.size());
    }

    @Test
    void geodesicBounds_firstCell_postgisPointCountAndStraightVertices() {
        Polygon geodesic = geometryH33.geodesicBounds(0);

        assertEquals(0, geometryH33.id(0));
        assertEquals(97, geodesic.getNumPoints());
        Set<Coordinate> geodesicPoints = new HashSet<>(Arrays.asList(geodesic.getCoordinates()));
        for (Coordinate straight : geometryH33.straightBounds(0).getCoordinates()) {
            assertTrue(geodesicPoints.contains(new Coordinate(straight.x, straight.y)), straight.toString());
        }
    }

    @Test
    void geodesicBounds_cellsOutsideAntimeridian_segmentsUpTo5000MetersAndValid() {
        int checked = 0;
        for (int row = 0; row < geometryH33.size(); row += 97) {
            Envelope straight = geometryH33.straightBounds(row).getEnvelopeInternal();
            if (straight.getMaxX() >= 180 || straight.getMinX() <= -180 || straight.getWidth() > 180) {
                continue;
            }
            Polygon geodesic = geometryH33.geodesicBounds(row);
            assertTrue(geodesic.isValid());
            Coordinate[] points = geodesic.getCoordinates();
            for (int i = 1; i < points.length; i++) {
                assertTrue(distanceMeters(points[i - 1], points[i]) <= 5000.001, row + ": " + points[i - 1] + " " + points[i]);
            }
            checked++;
        }
        assertTrue(checked > 400, String.valueOf(checked));
    }

    @Test
    void geodesicBounds_antimeridianCells_shiftedLikePostgis() {
        int east = 0;
        int west = 0;
        for (int row = 0; row < geometryH33.size(); row++) {
            Envelope straight = geometryH33.straightBounds(row).getEnvelopeInternal();
            if (straight.getWidth() > 180) {
                continue;
            }
            Envelope geodesic = geometryH33.geodesicBounds(row).getEnvelopeInternal();
            if (straight.getMaxX() >= 180) {
                east++;
                assertTrue(geodesic.getMinX() >= 0 && geodesic.getMaxX() >= 180, row + " " + geodesic);
            } else if (straight.getMinX() <= -180) {
                west++;
                assertTrue(geodesic.getMaxX() <= 0 && geodesic.getMinX() <= -180, row + " " + geodesic);
            }
        }
        assertEquals(217, east);
        assertEquals(216, west);
    }

    @Test
    void geodesicBounds_cellsWiderThan180Degrees_straightPolygon() {
        int wide = 0;
        for (int row = 0; row < geometryH33.size(); row++) {
            if (geometryH33.straightBounds(row).getEnvelopeInternal().getWidth() > 180) {
                wide++;
                assertSame(geometryH33.straightBounds(row), geometryH33.geodesicBounds(row));
            }
        }
        assertEquals(2, wide);
    }

    private static Geometry straightBounds(short id) {
        for (int row = 0; row < geometryH33.size(); row++) {
            if (geometryH33.id(row) == id) {
                return geometryH33.straightBounds(row);
            }
        }
        throw new AssertionError("No cell " + id);
    }

    private static double distanceMeters(Coordinate start, Coordinate end) {
        double startLatitude = Math.toRadians(start.y);
        double endLatitude = Math.toRadians(end.y);
        double deltaLatitude = endLatitude - startLatitude;
        double deltaLongitude = Math.toRadians(end.x - start.x);
        double a = Math.pow(Math.sin(deltaLatitude / 2), 2)
                + Math.cos(startLatitude) * Math.cos(endLatitude) * Math.pow(Math.sin(deltaLongitude / 2), 2);
        return 2 * WGS84_RADIUS * Math.asin(Math.sqrt(a));
    }

    private static boolean contains(short[] cells, short cell) {
        for (short value : cells) {
            if (value == cell) {
                return true;
            }
        }
        return false;
    }

    private static short cell(double latitude, double longitude) {
        return CompactH3.serialize3(h3Core.latLngToCell(latitude, longitude, 3));
    }

    private static Polygon square(double longitude, double latitude, double size) {
        return rectangle(longitude, latitude, longitude + size, latitude + size);
    }

    private static Polygon rectangle(double minLongitude, double minLatitude, double maxLongitude, double maxLatitude) {
        return GEOMETRY_FACTORY.createPolygon(new Coordinate[]{
                new Coordinate(minLongitude, minLatitude), new Coordinate(maxLongitude, minLatitude),
                new Coordinate(maxLongitude, maxLatitude), new Coordinate(minLongitude, maxLatitude),
                new Coordinate(minLongitude, minLatitude)});
    }
}
