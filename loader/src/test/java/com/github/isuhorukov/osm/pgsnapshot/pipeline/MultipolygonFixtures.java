package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.util.CompactH3;
import com.uber.h3core.H3Core;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

import java.io.IOException;
import java.io.UncheckedIOException;

public final class MultipolygonFixtures {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private MultipolygonFixtures() {
    }

    public static MultiPolygon square(double longitude, double latitude, double size) {
        return GEOMETRY_FACTORY.createMultiPolygon(new Polygon[]{GEOMETRY_FACTORY.createPolygon(new Coordinate[]{
                new Coordinate(longitude, latitude), new Coordinate(longitude + size, latitude),
                new Coordinate(longitude + size, latitude + size), new Coordinate(longitude, latitude + size),
                new Coordinate(longitude, latitude)})});
    }

    public static MultiPolygon circle(double longitude, double latitude, double radius, int points) {
        Coordinate[] ring = new Coordinate[points + 1];
        for (int i = 0; i < points; i++) {
            double angle = 2 * Math.PI * i / points;
            ring[i] = new Coordinate(longitude + radius * Math.cos(angle), latitude + radius * Math.sin(angle));
        }
        ring[points] = ring[0].copy();
        return GEOMETRY_FACTORY.createMultiPolygon(new Polygon[]{GEOMETRY_FACTORY.createPolygon(ring)});
    }

    public static String ewkbHex(Geometry geometry) {
        Geometry withSrid = geometry.copy();
        withSrid.setSRID(4326);
        return WKBWriter.toHex(new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN, true).write(withSrid));
    }

    public static short cell(double latitude, double longitude) {
        try {
            return CompactH3.serialize3(H3Core.newInstance().latLngToCell(latitude, longitude, 3));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
