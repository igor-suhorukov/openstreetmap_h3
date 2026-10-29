package com.github.isuhorukov.osm.pgsnapshot.util;

import com.uber.h3core.H3Core;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class GeometryH33Test {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static H3Core h3Core;

    @BeforeAll
    static void loadH3() throws IOException {
        h3Core = H3Core.newInstance();
    }

    @Test
    void of_polygonInsideOneCell_thatCell() {
        Polygon polygon = square(98.30, 7.88, 0.001);

        assertEquals(cell(7.88, 98.30), GeometryH33.of(polygon, h3Core));
    }

    @Test
    void of_polygonAcrossCells_multipleCells() {
        Polygon polygon = square(98.0, 7.5, 2.0);

        assertEquals(GeometryH33.MULTIPLE_CELLS, GeometryH33.of(polygon, h3Core));
    }

    @Test
    void of_multipolygonPartsInDifferentCells_multipleCells() {
        Polygon phuket = square(98.30, 7.88, 0.001);
        Polygon moscow = square(37.62, 55.75, 0.001);
        assertNotEquals(GeometryH33.of(phuket, h3Core), GeometryH33.of(moscow, h3Core));

        assertEquals(GeometryH33.MULTIPLE_CELLS, GeometryH33.of(
                GEOMETRY_FACTORY.createMultiPolygon(new Polygon[]{phuket, moscow}), h3Core));
    }

    @Test
    void of_emptyGeometry_multipleCells() {
        assertEquals(GeometryH33.MULTIPLE_CELLS, GeometryH33.of(GEOMETRY_FACTORY.createPolygon(), h3Core));
    }

    private static short cell(double latitude, double longitude) {
        return CompactH3.serialize3(h3Core.latLngToCell(latitude, longitude, 3));
    }

    private static Polygon square(double longitude, double latitude, double size) {
        return GEOMETRY_FACTORY.createPolygon(new Coordinate[]{
                new Coordinate(longitude, latitude), new Coordinate(longitude + size, latitude),
                new Coordinate(longitude + size, latitude + size), new Coordinate(longitude, latitude + size),
                new Coordinate(longitude, latitude)});
    }
}
