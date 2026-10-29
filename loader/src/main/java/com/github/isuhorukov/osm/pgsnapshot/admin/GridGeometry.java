package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.operation.overlayng.OverlayNG;
import org.locationtech.jts.operation.overlayng.PrecisionReducer;
import org.locationtech.jts.operation.overlayng.RingClipper;
import org.locationtech.jts.operation.overlayng.UnaryUnionNG;

import java.util.ArrayList;
import java.util.List;

final class GridGeometry {

    static final double SCALE = 1e7;
    static final GeometryFactory FACTORY = new GeometryFactory();
    static final PrecisionModel OSM_GRID = new PrecisionModel(SCALE);
    static final int MAX_VERTICES = 256;
    private static final int MAX_DEPTH = 50;
    private static final double UNBOUNDED = 1000;
    private static final double AUTHALIC_RADIUS_M = 6_371_007.181;
    private static final double MEAN_RADIUS_M = 6_371_008.8;

    private GridGeometry() {
    }

    static int grid(double degrees) {
        return (int) Math.round(degrees * SCALE);
    }

    static Geometry overlay(Geometry a, Geometry b, int opCode) {
        Geometry result = OverlayNG.overlay(a, b, opCode, OSM_GRID);
        return a.getDimension() == 2 && b.getDimension() == 2 ? polygonal(result) : result;
    }

    static Geometry union(List<Geometry> geometries) {
        return polygonal(UnaryUnionNG.union(FACTORY.buildGeometry(geometries), OSM_GRID));
    }

    static Geometry polygonal(Geometry geometry) {
        if (geometry instanceof Polygon || geometry instanceof MultiPolygon) {
            return geometry;
        }
        List<Polygon> polygons = new ArrayList<>();
        addPolygons(geometry, polygons);
        return FACTORY.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    private static void addPolygons(Geometry geometry, List<Polygon> polygons) {
        for (int i = 0; i < geometry.getNumGeometries(); i++) {
            Geometry part = geometry.getGeometryN(i);
            if (part instanceof Polygon) {
                polygons.add((Polygon) part);
            } else if (part instanceof GeometryCollection && part != geometry) {
                addPolygons(part, polygons);
            }
        }
    }

    static void subdivide(Geometry geometry, List<Geometry> out) {
        if (geometry != null && !geometry.isEmpty()) {
            subdivide(geometry, out, 0, geometry.getDimension());
        }
    }

    private static void subdivide(Geometry geometry, List<Geometry> out, int depth, int dimension) {
        if (geometry == null || geometry.isEmpty()) {
            return;
        }
        if (!(geometry instanceof Polygon || geometry instanceof LineString)) {
            for (int i = 0; i < geometry.getNumGeometries(); i++) {
                Geometry part = geometry.getGeometryN(i);
                if (part != geometry) {
                    subdivide(part, out, depth, dimension);
                }
            }
            return;
        }
        if (geometry.getDimension() != dimension) {
            return;
        }
        if (geometry.getNumPoints() <= MAX_VERTICES || depth >= MAX_DEPTH) {
            addIfNotEmpty(geometry, out);
            return;
        }
        for (Envelope half : halves(geometry.getEnvelopeInternal())) {
            subdivide(overlay(geometry, FACTORY.toGeometry(half), OverlayNG.INTERSECTION), out, depth + 1, dimension);
        }
    }

    private static void addIfNotEmpty(Geometry geometry, List<Geometry> out) {
        if (geometry instanceof Polygon ? geometry.getArea() > 0 : geometry.getLength() > 0) {
            out.add(geometry);
        }
    }

    private static Envelope[] halves(Envelope envelope) {
        if (envelope.getWidth() >= envelope.getHeight()) {
            double middle = (envelope.getMinX() + envelope.getMaxX()) / 2;
            return new Envelope[]{new Envelope(envelope.getMinX(), middle, envelope.getMinY(), envelope.getMaxY()),
                    new Envelope(middle, envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY())};
        }
        double middle = (envelope.getMinY() + envelope.getMaxY()) / 2;
        return new Envelope[]{new Envelope(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), middle),
                new Envelope(envelope.getMinX(), envelope.getMaxX(), middle, envelope.getMaxY())};
    }

    static List<Polygon> clipSubdivide(Geometry geometry) {
        List<Polygon> out = new ArrayList<>();
        clipSubdivide(geometry, out);
        return out;
    }

    private static void clipSubdivide(Geometry geometry, List<Polygon> out) {
        if (geometry == null || geometry.isEmpty()) {
            return;
        }
        for (int i = 0; i < geometry.getNumGeometries(); i++) {
            Geometry part = geometry.getGeometryN(i);
            if (part instanceof Polygon) {
                clipPolygon((Polygon) part, out);
            } else if (part instanceof GeometryCollection && part != geometry) {
                clipSubdivide(part, out);
            }
        }
    }

    private static void clipPolygon(Polygon polygon, List<Polygon> out) {
        if (polygon.getNumPoints() <= MAX_VERTICES) {
            if (polygon.getArea() > 0) {
                out.add(polygon);
            }
            return;
        }
        Coordinate[][] rings = new Coordinate[1 + polygon.getNumInteriorRing()][];
        rings[0] = polygon.getExteriorRing().getCoordinates();
        for (int h = 0; h < polygon.getNumInteriorRing(); h++) {
            rings[h + 1] = polygon.getInteriorRingN(h).getCoordinates();
        }
        clipRings(polygon, rings, new Envelope(-UNBOUNDED, UNBOUNDED, -UNBOUNDED, UNBOUNDED), 0, out);
    }

    private static void clipRings(Polygon original, Coordinate[][] rings, Envelope region, int depth,
                                  List<Polygon> out) {
        int points = 0;
        for (Coordinate[] ring : rings) {
            points += ring.length;
        }
        if (points <= MAX_VERTICES || depth >= MAX_DEPTH) {
            emit(original, rings, region, out);
            return;
        }
        Envelope envelope = envelope(rings[0]);
        if (envelope.getWidth() >= envelope.getHeight()) {
            double middle = gridMiddle(envelope.getMinX(), envelope.getMaxX());
            half(original, rings, new Envelope(-UNBOUNDED, middle, -UNBOUNDED, UNBOUNDED),
                    new Envelope(region.getMinX(), Math.min(middle, region.getMaxX()), region.getMinY(),
                            region.getMaxY()), depth, out);
            half(original, rings, new Envelope(middle, UNBOUNDED, -UNBOUNDED, UNBOUNDED),
                    new Envelope(Math.max(middle, region.getMinX()), region.getMaxX(), region.getMinY(),
                            region.getMaxY()), depth, out);
        } else {
            double middle = gridMiddle(envelope.getMinY(), envelope.getMaxY());
            half(original, rings, new Envelope(-UNBOUNDED, UNBOUNDED, -UNBOUNDED, middle),
                    new Envelope(region.getMinX(), region.getMaxX(), region.getMinY(),
                            Math.min(middle, region.getMaxY())), depth, out);
            half(original, rings, new Envelope(-UNBOUNDED, UNBOUNDED, middle, UNBOUNDED),
                    new Envelope(region.getMinX(), region.getMaxX(), Math.max(middle, region.getMinY()),
                            region.getMaxY()), depth, out);
        }
    }

    private static void half(Polygon original, Coordinate[][] rings, Envelope clip, Envelope region, int depth,
                             List<Polygon> out) {
        RingClipper clipper = new RingClipper(clip);
        Coordinate[] shell = clipper.clip(rings[0]);
        if (shell.length < 4) {
            return;
        }
        List<Coordinate[]> kept = new ArrayList<>(rings.length);
        kept.add(shell);
        for (int h = 1; h < rings.length; h++) {
            Coordinate[] hole = clipper.clip(rings[h]);
            if (hole.length >= 4) {
                kept.add(hole);
            }
        }
        clipRings(original, kept.toArray(new Coordinate[0][]), region, depth + 1, out);
    }

    private static void emit(Polygon original, Coordinate[][] rings, Envelope region, List<Polygon> out) {
        Geometry clean;
        try {
            LinearRing[] holes = new LinearRing[rings.length - 1];
            for (int h = 1; h < rings.length; h++) {
                holes[h - 1] = FACTORY.createLinearRing(rings[h]);
            }
            Polygon raw = FACTORY.createPolygon(FACTORY.createLinearRing(rings[0]), holes);
            clean = polygonal(PrecisionReducer.reducePrecision(raw, OSM_GRID));
        } catch (RuntimeException e) {
            clean = overlay(original, FACTORY.toGeometry(region), OverlayNG.INTERSECTION);
        }
        for (int i = 0; i < clean.getNumGeometries(); i++) {
            Geometry part = clean.getGeometryN(i);
            if (part instanceof Polygon && part.getArea() > 0) {
                out.add((Polygon) part);
            }
        }
    }

    private static double gridMiddle(double min, double max) {
        double middle = Math.round((min + max) / 2 * SCALE) / SCALE;
        return middle > min && middle < max ? middle : (min + max) / 2;
    }

    private static Envelope envelope(Coordinate[] ring) {
        Envelope envelope = new Envelope();
        for (Coordinate coordinate : ring) {
            envelope.expandToInclude(coordinate);
        }
        return envelope;
    }

    static double areaM2(Geometry geometry) {
        double area = 0;
        for (int i = 0; i < geometry.getNumGeometries(); i++) {
            Geometry part = geometry.getGeometryN(i);
            if (part instanceof Polygon) {
                Polygon polygon = (Polygon) part;
                area += ringAreaM2(polygon.getExteriorRing().getCoordinateSequence());
                for (int h = 0; h < polygon.getNumInteriorRing(); h++) {
                    area -= ringAreaM2(polygon.getInteriorRingN(h).getCoordinateSequence());
                }
            } else if (part instanceof GeometryCollection && part != geometry) {
                area += areaM2(part);
            }
        }
        return area;
    }

    private static double ringAreaM2(CoordinateSequence ring) {
        double sum = 0;
        for (int i = 0; i + 1 < ring.size(); i++) {
            double x1 = Math.toRadians(ring.getX(i));
            double x2 = Math.toRadians(ring.getX(i + 1));
            double y1 = Math.toRadians(ring.getY(i));
            double y2 = Math.toRadians(ring.getY(i + 1));
            sum += (x2 - x1) * (2 + Math.sin(y1) + Math.sin(y2));
        }
        return Math.abs(sum * AUTHALIC_RADIUS_M * AUTHALIC_RADIUS_M / 2);
    }

    static double lengthM(Geometry geometry) {
        double meters = 0;
        for (int i = 0; i < geometry.getNumGeometries(); i++) {
            Geometry part = geometry.getGeometryN(i);
            if (part instanceof LineString) {
                CoordinateSequence line = ((LineString) part).getCoordinateSequence();
                for (int j = 1; j < line.size(); j++) {
                    meters += haversineM(line.getX(j - 1), line.getY(j - 1), line.getX(j), line.getY(j));
                }
            } else if (part instanceof GeometryCollection && part != geometry) {
                meters += lengthM(part);
            }
        }
        return meters;
    }

    private static double haversineM(double lon1, double lat1, double lon2, double lat2) {
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double sinLat = Math.sin((phi2 - phi1) / 2);
        double sinLon = Math.sin(Math.toRadians(lon2 - lon1) / 2);
        double h = sinLat * sinLat + Math.cos(phi1) * Math.cos(phi2) * sinLon * sinLon;
        return 2 * MEAN_RADIUS_M * Math.asin(Math.min(1, Math.sqrt(h)));
    }
}
