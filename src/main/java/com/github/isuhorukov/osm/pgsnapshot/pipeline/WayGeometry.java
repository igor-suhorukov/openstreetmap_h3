package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.Serializer;
import com.github.isuhorukov.osm.pgsnapshot.util.CompactH3;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import net.postgis.jdbc.geometry.LinearRing;
import net.postgis.jdbc.geometry.Point;
import net.postgis.jdbc.geometry.Polygon;
import org.geotools.geometry.jts.JTS;
import org.h2gis.functions.spatial.properties.ST_IsClosed;
import org.locationtech.jts.algorithm.MinimumBoundingCircle;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBWriter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class WayGeometry {

    private final net.postgis.jdbc.geometry.LineString lineString;
    private final Polygon bboxGeometry;
    private final Point centre;
    private final double latitude;
    private final double longitude;
    private final float scaleDim;
    private final int h38;
    private final short h33Center;
    private final boolean closed;
    private final boolean nonValid;
    private final long[] pointIdxs;
    private final Set<Integer> wayIntersectionH38Indexes;
    private final double minX;
    private final double maxX;
    private final double minY;
    private final double maxY;
    private final byte[] lineStringWkb;
    private final byte[] bboxWkb;

    private WayGeometry(Builder builder) {
        this.lineString = builder.lineString;
        this.bboxGeometry = builder.bboxGeometry;
        this.centre = builder.centre;
        this.latitude = builder.latitude;
        this.longitude = builder.longitude;
        this.scaleDim = builder.scaleDim;
        this.h38 = builder.h38;
        this.h33Center = builder.h33Center;
        this.closed = builder.closed;
        this.nonValid = builder.nonValid;
        this.pointIdxs = builder.pointIdxs;
        this.wayIntersectionH38Indexes = builder.wayIntersectionH38Indexes;
        this.minX = builder.minX;
        this.maxX = builder.maxX;
        this.minY = builder.minY;
        this.maxY = builder.maxY;
        this.lineStringWkb = builder.lineStringWkb;
        this.bboxWkb = builder.bboxWkb;
    }

    static final class Builder {
        net.postgis.jdbc.geometry.LineString lineString;
        Polygon bboxGeometry;
        Point centre;
        double latitude;
        double longitude;
        float scaleDim;
        int h38;
        short h33Center;
        boolean closed;
        boolean nonValid;
        long[] pointIdxs;
        Set<Integer> wayIntersectionH38Indexes;
        double minX;
        double maxX;
        double minY;
        double maxY;
        byte[] lineStringWkb;
        byte[] bboxWkb;

        Builder lineString(net.postgis.jdbc.geometry.LineString v) { lineString = v; return this; }
        Builder bboxGeometry(Polygon v)                            { bboxGeometry = v; return this; }
        Builder centre(Point v)                                    { centre = v; return this; }
        Builder latitude(double v)                                 { latitude = v; return this; }
        Builder longitude(double v)                                { longitude = v; return this; }
        Builder scaleDim(float v)                                  { scaleDim = v; return this; }
        Builder h38(int v)                                         { h38 = v; return this; }
        Builder h33Center(short v)                                 { h33Center = v; return this; }
        Builder closed(boolean v)                                  { closed = v; return this; }
        Builder nonValid(boolean v)                                { nonValid = v; return this; }
        Builder pointIdxs(long[] v)                                { pointIdxs = v; return this; }
        Builder wayIntersectionH38Indexes(Set<Integer> v)          { wayIntersectionH38Indexes = v; return this; }
        Builder minX(double v)                                     { minX = v; return this; }
        Builder maxX(double v)                                     { maxX = v; return this; }
        Builder minY(double v)                                     { minY = v; return this; }
        Builder maxY(double v)                                     { maxY = v; return this; }
        Builder lineStringWkb(byte[] v)                            { lineStringWkb = v; return this; }
        Builder bboxWkb(byte[] v)                                  { bboxWkb = v; return this; }

        WayGeometry build() { return new WayGeometry(this); }
    }

    private static final class WayPoints {
        final long[] pointIdxs;
        final Coordinate[] coordinates;
        final net.postgis.jdbc.geometry.LineString lineString;

        private WayPoints(long[] pointIdxs, Coordinate[] coordinates,
                          net.postgis.jdbc.geometry.LineString lineString) {
            this.pointIdxs = pointIdxs;
            this.coordinates = coordinates;
            this.lineString = lineString;
        }

        static WayPoints from(List<WayNode> wayNodes) {
            net.postgis.jdbc.geometry.Point[] points = new net.postgis.jdbc.geometry.Point[wayNodes.size()];
            Coordinate[] coordinates = new Coordinate[wayNodes.size()];
            long[] pointIdxs = new long[wayNodes.size()];
            for (int i = 0; i < wayNodes.size(); i++) {
                WayNode wn = wayNodes.get(i);
                double lat = wn.getLatitude();
                double lon = wn.getLongitude();
                points[i] = new net.postgis.jdbc.geometry.Point(lon, lat);
                coordinates[i] = new CoordinateXY(lon, lat);
                pointIdxs[i] = wn.getNodeId();
            }
            net.postgis.jdbc.geometry.LineString ls = new net.postgis.jdbc.geometry.LineString(points);
            ls.setSrid(Serializer.SRID);
            return new WayPoints(pointIdxs, coordinates, ls);
        }
    }

    private static final class CentreData {
        final double latitude;
        final double longitude;
        final float scaleDim;
        final Point centre;
        final double distanceMeter;

        CentreData(double latitude, double longitude, float scaleDim, Point centre, double distanceMeter) {
            this.latitude = latitude;
            this.longitude = longitude;
            this.scaleDim = scaleDim;
            this.centre = centre;
            this.distanceMeter = distanceMeter;
        }
    }

    public static WayGeometry from(org.openstreetmap.osmosis.core.domain.v0_6.Way entity,
                                   H3Core h3Core, GeometryFactory geometryFactory,
                                   CoordinateReferenceSystem coordinateReferenceSystem,
                                   boolean scaleApproximation, WKBWriter wkbWriter) {
        List<WayNode> wayNodes = entity.getWayNodes();
        WayPoints wayPoints = WayPoints.from(wayNodes);

        Geometry currentWayGeometry = wayPoints.coordinates.length > 1
                ? geometryFactory.createLineString(wayPoints.coordinates)
                : geometryFactory.createPoint(wayPoints.coordinates[0]);

        boolean closed = ST_IsClosed.isClosed(currentWayGeometry);
        boolean nonValid = !(currentWayGeometry.isValid() && wayPoints.coordinates.length > 1);

        Envelope envelopeInternal = currentWayGeometry.getEnvelopeInternal();
        double minX = envelopeInternal.getMinX();
        double minY = envelopeInternal.getMinY();
        double maxX = envelopeInternal.getMaxX() + Double.MIN_VALUE;
        double maxY = envelopeInternal.getMaxY() + Double.MIN_VALUE;

        Geometry jtsEnvelope = buildJtsEnvelope(geometryFactory, minX, minY, maxX, maxY);
        Polygon bboxGeometry = buildPostgisBbox(minX, minY, maxX, maxY);

        CentreData centreData = computeCentreAndScale(
                currentWayGeometry, envelopeInternal, scaleApproximation, coordinateReferenceSystem);

        long centerSrcIndex = h3Core.latLngToCell(centreData.latitude, centreData.longitude, 8);
        int h38 = CompactH3.serialize8(centerSrcIndex);
        short h33Center = CompactH3.serialize3(h3Core.latLngToCell(centreData.latitude, centreData.longitude, 3));

        long[] h38SourceIdxs = computeH38SourceIdxs(wayNodes, h3Core);
        boolean isOneH38 = isAllSameH38(h38SourceIdxs, centerSrcIndex);
        Set<Integer> wayIntersectionH38Indexes = computeH38Intersections(
                h38SourceIdxs, isOneH38, closed, wayNodes, h3Core, centreData.distanceMeter, h38);

        return new Builder()
                .lineString(wayPoints.lineString)
                .bboxGeometry(bboxGeometry)
                .centre(centreData.centre)
                .latitude(centreData.latitude).longitude(centreData.longitude)
                .scaleDim(centreData.scaleDim)
                .h38(h38).h33Center(h33Center)
                .closed(closed).nonValid(nonValid)
                .pointIdxs(wayPoints.pointIdxs)
                .wayIntersectionH38Indexes(wayIntersectionH38Indexes)
                .minX(minX).maxX(maxX).minY(minY).maxY(maxY)
                .lineStringWkb(wkbWriter.write(currentWayGeometry))
                .bboxWkb(wkbWriter.write(jtsEnvelope))
                .build();
    }

    private static Geometry buildJtsEnvelope(GeometryFactory geometryFactory,
            double minX, double minY, double maxX, double maxY) {
        return geometryFactory.createPolygon(new Coordinate[]{
                new CoordinateXY(minX, minY), new CoordinateXY(minX, maxY),
                new CoordinateXY(maxX, maxY), new CoordinateXY(maxX, minY),
                new CoordinateXY(minX, minY)
        });
    }

    private static Polygon buildPostgisBbox(double minX, double minY, double maxX, double maxY) {
        Polygon bbox = new Polygon(new LinearRing[]{
                new LinearRing(new net.postgis.jdbc.geometry.Point[]{
                        new net.postgis.jdbc.geometry.Point(minX, minY),
                        new net.postgis.jdbc.geometry.Point(minX, maxY),
                        new net.postgis.jdbc.geometry.Point(maxX, maxY),
                        new net.postgis.jdbc.geometry.Point(maxX, minY),
                        new net.postgis.jdbc.geometry.Point(minX, minY)
                })
        });
        bbox.srid = Serializer.SRID;
        return bbox;
    }

    private static CentreData computeCentreAndScale(Geometry wayGeometry, Envelope envelopeInternal,
            boolean scaleApproximation, CoordinateReferenceSystem crs) {
        double latitude;
        double longitude;
        float scaleDim;
        if (!scaleApproximation) {
            MinimumBoundingCircle boundingCircle = new MinimumBoundingCircle(wayGeometry);
            scaleDim = (float) boundingCircle.getRadius();
            Coordinate coordinate = boundingCircle.getCentre();
            if (coordinate != null) {
                latitude = coordinate.y;
                longitude = boundingCircle.getCentre().x;
            } else {
                org.locationtech.jts.geom.Point centroid = wayGeometry.getCentroid();
                longitude = centroid.getX();
                latitude = centroid.getY();
                scaleDim = (float) envelopeInternal.maxExtent();
            }
        } else {
            org.locationtech.jts.geom.Point centroid = wayGeometry.getCentroid();
            longitude = centroid.getX();
            latitude = centroid.getY();
            scaleDim = (float) envelopeInternal.maxExtent();
        }
        Point centre = Serializer.getPoint(latitude, longitude);
        double distanceMeter = Double.MIN_VALUE;
        try {
            distanceMeter = JTS.orthodromicDistance(
                    new CoordinateXY(longitude, latitude),
                    new CoordinateXY(longitude + scaleDim, latitude), crs);
        } catch (TransformException e) {
            // ignore — distanceMeter stays at Double.MIN_VALUE, scaleDim is unchanged
        }
        if (distanceMeter != Double.MIN_VALUE) {
            scaleDim = (float) distanceMeter;
        }
        return new CentreData(latitude, longitude, scaleDim, centre, distanceMeter);
    }

    private static long[] computeH38SourceIdxs(List<WayNode> wayNodes, H3Core h3Core) {
        long[] h38SourceIdxs = new long[wayNodes.size()];
        for (int i = 0; i < wayNodes.size(); i++) {
            WayNode wn = wayNodes.get(i);
            h38SourceIdxs[i] = h3Core.latLngToCell(wn.getLatitude(), wn.getLongitude(), 8);
        }
        return h38SourceIdxs;
    }

    private static boolean isAllSameH38(long[] h38SourceIdxs, long centerSrcIndex) {
        for (long idx : h38SourceIdxs) {
            if (idx != centerSrcIndex) {
                return false;
            }
        }
        return true;
    }

    private static Set<Integer> computeH38Intersections(long[] h38SourceIdxs, boolean isOneH38,
            boolean closed, List<WayNode> wayNodes, H3Core h3Core, double distanceMeter, int h38) {
        if (isOneH38 || distanceMeter >= 3000.0) {
            return Collections.emptySet();
        }
        Set<Integer> intersections = Arrays.stream(h38SourceIdxs)
                .mapToObj(CompactH3::serialize8)
                .collect(Collectors.toCollection(TreeSet::new));
        for (int i = 0; i < h38SourceIdxs.length - 1; i++) {
            intersections.addAll(h3Core.gridPathCells(h38SourceIdxs[i], h38SourceIdxs[i + 1])
                    .stream().map(CompactH3::serialize8).collect(Collectors.toSet()));
        }
        if (closed) {
            intersections.addAll(h3Core.gridPathCells(
                    h38SourceIdxs[0], h38SourceIdxs[h38SourceIdxs.length - 1])
                    .stream().map(CompactH3::serialize8).collect(Collectors.toSet()));
            intersections.addAll(h3Core.polygonToCells(wayNodes.stream()
                    .map(wn -> new LatLng(wn.getLatitude(), wn.getLongitude())).collect(toList()),
                    null, 8).stream().map(CompactH3::serialize8).collect(Collectors.toSet()));
        }
        if (intersections.size() == 1 && intersections.iterator().next().equals(h38)) {
            return Collections.emptySet();
        }
        return intersections;
    }

    public net.postgis.jdbc.geometry.LineString getLineString() { return lineString; }
    public Polygon getBboxGeometry()                    { return bboxGeometry; }
    public Point getCentre()                            { return centre; }
    public double getLatitude()                         { return latitude; }
    public double getLongitude()                        { return longitude; }
    public float getScaleDim()                          { return scaleDim; }
    public int getH38()                                 { return h38; }
    public short getH33Center()                         { return h33Center; }
    public boolean isClosed()                           { return closed; }
    public boolean isNonValid()                         { return nonValid; }
    public long[] getPointIdxs()                        { return pointIdxs; }
    public Set<Integer> getWayIntersectionH38Indexes()  { return wayIntersectionH38Indexes; }
    public double getMinX()                             { return minX; }
    public double getMaxX()                             { return maxX; }
    public double getMinY()                             { return minY; }
    public double getMaxY()                             { return maxY; }
    public byte[] getLineStringWkb()                    { return lineStringWkb; }
    public byte[] getBboxWkb()                          { return bboxWkb; }
}
