package com.github.isuhorukov.osm.pgsnapshot.util;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.zip.GZIPInputStream;

public final class GeometryH33 {

    public static final short MULTIPLE_CELLS = Short.MAX_VALUE;
    static final String CELL_POLYGONS = "/h3_poly.tsv.gz";
    private static final double MAX_SEGMENT_METERS = 5000;
    private static final int ID_COLUMN = 0;
    private static final int BOUNDS_COLUMN = 5;
    private static final short[] NO_CELLS = new short[0];

    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final short[] ids;
    private final Polygon[] straightBounds;
    private final AtomicReferenceArray<PreparedGeometry> geodesicBounds;
    private final STRtree index = new STRtree();

    private GeometryH33(short[] ids, Polygon[] straightBounds) {
        this.ids = ids;
        this.straightBounds = straightBounds;
        this.geodesicBounds = new AtomicReferenceArray<>(straightBounds.length);
        for (int row = 0; row < straightBounds.length; row++) {
            index.insert(geodesicBounds(straightBounds[row], geometryFactory).getEnvelopeInternal(), row);
        }
        index.build();
    }

    public static GeometryH33 instance() {
        return Holder.INSTANCE;
    }

    static GeometryH33 read(InputStream cellPolygons) throws IOException {
        WKBReader wkbReader = new WKBReader(new GeometryFactory(PackedCoordinateSequenceFactory.DOUBLE_FACTORY));
        List<Short> ids = new ArrayList<>();
        List<Polygon> polygons = new ArrayList<>();
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(new GZIPInputStream(cellPolygons),
                StandardCharsets.US_ASCII))) {
            String line;
            while ((line = lines.readLine()) != null) {
                String[] columns = line.split("\t");
                ids.add(Short.parseShort(columns[ID_COLUMN]));
                polygons.add((Polygon) wkbReader.read(WKBReader.hexToBytes(columns[BOUNDS_COLUMN])));
            }
        } catch (ParseException e) {
            throw new IOException("Invalid H3 cell polygon in " + CELL_POLYGONS, e);
        }
        short[] cellIds = new short[ids.size()];
        for (int row = 0; row < cellIds.length; row++) {
            cellIds[row] = ids.get(row);
        }
        return new GeometryH33(cellIds, polygons.toArray(new Polygon[0]));
    }

    public short[] cells(Geometry geometry) {
        if (geometry.isEmpty()) {
            return NO_CELLS;
        }
        List<?> candidates = index.query(geometry.getEnvelopeInternal());
        short[] cells = new short[candidates.size()];
        int count = 0;
        for (Object candidate : candidates) {
            int row = (Integer) candidate;
            if (!contains(cells, count, ids[row]) && preparedGeodesicBounds(row).intersects(geometry)) {
                cells[count++] = ids[row];
            }
        }
        short[] sorted = Arrays.copyOf(cells, count);
        Arrays.sort(sorted);
        return sorted;
    }

    public short[][] cells(Geometry[] geometries) {
        short[][] cells = new short[geometries.length][];
        for (int i = 0; i < geometries.length; i++) {
            cells[i] = cells(geometries[i]);
        }
        return cells;
    }

    public static short h33(short[] cells) {
        return cells.length == 1 ? cells[0] : MULTIPLE_CELLS;
    }

    Polygon geodesicBounds(int row) {
        return geodesicBounds(straightBounds[row], geometryFactory);
    }

    Polygon straightBounds(int row) {
        return straightBounds[row];
    }

    int size() {
        return ids.length;
    }

    short id(int row) {
        return ids[row];
    }

    static Polygon geodesicBounds(Polygon straight, GeometryFactory geometryFactory) {
        Envelope envelope = straight.getEnvelopeInternal();
        if (envelope.getMaxX() - envelope.getMinX() > 180) {
            return straight;
        }
        LinearRing shell = geodesicRing(straight.getExteriorRing(), envelope, geometryFactory);
        LinearRing[] holes = new LinearRing[straight.getNumInteriorRing()];
        for (int i = 0; i < holes.length; i++) {
            holes[i] = geodesicRing(straight.getInteriorRingN(i), envelope, geometryFactory);
        }
        Polygon geodesic = geometryFactory.createPolygon(shell, holes);
        return geodesic.isValid() ? geodesic : straight;
    }

    private PreparedGeometry preparedGeodesicBounds(int row) {
        PreparedGeometry prepared = geodesicBounds.get(row);
        if (prepared == null) {
            prepared = PreparedGeometryFactory.prepare(geodesicBounds(row));
            if (!geodesicBounds.compareAndSet(row, null, prepared)) {
                prepared = geodesicBounds.get(row);
            }
        }
        return prepared;
    }

    private static LinearRing geodesicRing(LineString straightRing, Envelope straightEnvelope,
                                           GeometryFactory geometryFactory) {
        Coordinate[] coordinates = GeographySegmentizer.segmentize(straightRing.getCoordinates(), MAX_SEGMENT_METERS);
        for (Coordinate coordinate : coordinates) {
            coordinate.x = antimeridianShift(coordinate.x, straightEnvelope);
        }
        return geometryFactory.createLinearRing(coordinates);
    }

    private static double antimeridianShift(double longitude, Envelope straightEnvelope) {
        if (straightEnvelope.getMaxX() >= 180) {
            return shiftLongitude(longitude);
        }
        if (straightEnvelope.getMinX() <= -180) {
            return shiftLongitude(longitude) - 360;
        }
        return longitude;
    }

    private static double shiftLongitude(double longitude) {
        if (longitude < 0) {
            return longitude + 360;
        }
        if (longitude > 180) {
            return longitude - 360;
        }
        return longitude;
    }

    private static boolean contains(short[] cells, int count, short cell) {
        for (int i = 0; i < count; i++) {
            if (cells[i] == cell) {
                return true;
            }
        }
        return false;
    }

    private static final class Holder {
        private static final GeometryH33 INSTANCE = load();

        private static GeometryH33 load() {
            try (InputStream cellPolygons = GeometryH33.class.getResourceAsStream(CELL_POLYGONS)) {
                if (cellPolygons == null) {
                    throw new IllegalStateException("Resource not found: " + CELL_POLYGONS);
                }
                return read(cellPolygons);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
