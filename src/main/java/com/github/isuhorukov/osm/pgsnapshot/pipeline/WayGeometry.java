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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class WayGeometry {

    private final Geometry currentWayGeometry;
    private final net.postgis.jdbc.geometry.LineString lineString;
    private final Polygon bboxGeometry;
    private final Geometry envelope;
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

    private WayGeometry(Geometry currentWayGeometry, net.postgis.jdbc.geometry.LineString lineString,
                        Polygon bboxGeometry, Geometry envelope, Point centre,
                        double latitude, double longitude, float scaleDim, int h38, short h33Center,
                        boolean closed, boolean nonValid, long[] pointIdxs,
                        Set<Integer> wayIntersectionH38Indexes,
                        double minX, double maxX, double minY, double maxY,
                        byte[] lineStringWkb, byte[] bboxWkb) {
        this.currentWayGeometry = currentWayGeometry;
        this.lineString = lineString;
        this.bboxGeometry = bboxGeometry;
        this.envelope = envelope;
        this.centre = centre;
        this.latitude = latitude;
        this.longitude = longitude;
        this.scaleDim = scaleDim;
        this.h38 = h38;
        this.h33Center = h33Center;
        this.closed = closed;
        this.nonValid = nonValid;
        this.pointIdxs = pointIdxs;
        this.wayIntersectionH38Indexes = wayIntersectionH38Indexes;
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
        this.lineStringWkb = lineStringWkb;
        this.bboxWkb = bboxWkb;
    }

    public static WayGeometry from(org.openstreetmap.osmosis.core.domain.v0_6.Way entity,
                                   H3Core h3Core, GeometryFactory geometryFactory,
                                   CoordinateReferenceSystem coordinateReferenceSystem,
                                   boolean scaleApproximation, WKBWriter wkbWriter) {
        List<WayNode> wayNodes = entity.getWayNodes();
        net.postgis.jdbc.geometry.Point[] points = new net.postgis.jdbc.geometry.Point[wayNodes.size()];
        Coordinate[] coordinates = new Coordinate[wayNodes.size()];
        long[] pointsIdx = new long[wayNodes.size()];

        for (int pointIdx = 0; pointIdx < wayNodes.size(); pointIdx++) {
            WayNode wayNode = wayNodes.get(pointIdx);
            double lat = wayNode.getLatitude();
            double lon = wayNode.getLongitude();
            points[pointIdx] = new net.postgis.jdbc.geometry.Point(lon, lat);
            coordinates[pointIdx] = new CoordinateXY(lon, lat);
            pointsIdx[pointIdx] = wayNode.getNodeId();
        }

        net.postgis.jdbc.geometry.LineString lineString = new net.postgis.jdbc.geometry.LineString(points);
        lineString.setSrid(Serializer.SRID);

        Geometry currentWayGeometry;
        if (coordinates.length > 1) {
            currentWayGeometry = geometryFactory.createLineString(coordinates);
        } else {
            currentWayGeometry = geometryFactory.createPoint(coordinates[0]);
        }

        boolean closed = ST_IsClosed.isClosed(currentWayGeometry);
        boolean nonValid = !(currentWayGeometry.isValid() && coordinates.length > 1);

        Envelope envelopeInternal = currentWayGeometry.getEnvelopeInternal();
        final double minX = envelopeInternal.getMinX();
        final double minY = envelopeInternal.getMinY();
        final double maxX = envelopeInternal.getMaxX() + Double.MIN_VALUE;
        final double maxY = envelopeInternal.getMaxY() + Double.MIN_VALUE;

        Geometry envelope = geometryFactory.createPolygon(new Coordinate[]{
                new CoordinateXY(minX, minY),
                new CoordinateXY(minX, maxY),
                new CoordinateXY(maxX, maxY),
                new CoordinateXY(maxX, minY),
                new CoordinateXY(minX, minY)
        });
        Polygon bboxGeometry = new Polygon(new LinearRing[]{
                new LinearRing(new net.postgis.jdbc.geometry.Point[]{
                        new net.postgis.jdbc.geometry.Point(minX, minY),
                        new net.postgis.jdbc.geometry.Point(minX, maxY),
                        new net.postgis.jdbc.geometry.Point(maxX, maxY),
                        new net.postgis.jdbc.geometry.Point(maxX, minY),
                        new net.postgis.jdbc.geometry.Point(minX, minY)})
        });
        bboxGeometry.srid = Serializer.SRID;

        float scaleDim;
        Point centre;
        double latitude;
        double longitude;

        if (!scaleApproximation) {
            MinimumBoundingCircle boundingCircle = new MinimumBoundingCircle(currentWayGeometry);
            scaleDim = (float) boundingCircle.getRadius();
            Coordinate coordinate = boundingCircle.getCentre();
            if (coordinate != null) {
                latitude = coordinate.y;
                longitude = boundingCircle.getCentre().x;
            } else {
                org.locationtech.jts.geom.Point centroid = currentWayGeometry.getCentroid();
                longitude = centroid.getX();
                latitude = centroid.getY();
                scaleDim = (float) envelopeInternal.maxExtent();
            }
            centre = Serializer.getPoint(latitude, longitude);
        } else {
            org.locationtech.jts.geom.Point centroid = currentWayGeometry.getCentroid();
            longitude = centroid.getX();
            latitude = centroid.getY();
            centre = Serializer.getPoint(latitude, longitude);
            scaleDim = (float) envelopeInternal.maxExtent();
        }

        long centerSrcIndex = h3Core.latLngToCell(latitude, longitude, 8);
        int h38 = CompactH3.serialize8(centerSrcIndex);
        short h33Center = CompactH3.serialize3(h3Core.latLngToCell(latitude, longitude, 3));

        double distanceMeter = Double.MIN_VALUE;
        try {
            distanceMeter = JTS.orthodromicDistance(new CoordinateXY(longitude, latitude),
                    new CoordinateXY(longitude + scaleDim, latitude), coordinateReferenceSystem);
        } catch (TransformException e) {
            // ignore
        }
        if (distanceMeter != Double.MIN_VALUE) {
            scaleDim = (float) distanceMeter;
        }

        boolean isOneH38 = true;
        long[] h38SourceIdxs = new long[wayNodes.size()];
        for (int pointIndex = 0, wayNodesSize = wayNodes.size(); pointIndex < wayNodesSize; pointIndex++) {
            WayNode wayNode = wayNodes.get(pointIndex);
            long currentSrcIndex = h3Core.latLngToCell(wayNode.getLatitude(), wayNode.getLongitude(), 8);
            h38SourceIdxs[pointIndex] = currentSrcIndex;
            if (centerSrcIndex != currentSrcIndex) {
                isOneH38 = false;
            }
        }

        Set<Integer> wayIntersectionH38Indexes = null;
        if (!isOneH38 && distanceMeter < 3000.0) {
            wayIntersectionH38Indexes = Arrays.stream(h38SourceIdxs).mapToObj(CompactH3::serialize8).collect(Collectors.toCollection(TreeSet::new));
            for (int i = 0; i < h38SourceIdxs.length - 1; i++) {
                wayIntersectionH38Indexes.addAll(h3Core.gridPathCells(h38SourceIdxs[i], h38SourceIdxs[i + 1]).stream().map(CompactH3::serialize8).collect(Collectors.toSet()));
            }
            if (closed) {
                wayIntersectionH38Indexes.addAll(h3Core.gridPathCells(h38SourceIdxs[0], h38SourceIdxs[h38SourceIdxs.length - 1]).stream().map(CompactH3::serialize8).collect(Collectors.toSet()));
                wayIntersectionH38Indexes.addAll(h3Core.polygonToCells(wayNodes.stream().map(wayNode -> new LatLng(wayNode.getLatitude(), wayNode.getLongitude())).collect(toList()), null, 8).stream().map(CompactH3::serialize8).collect(Collectors.toSet()));
            }
            if (wayIntersectionH38Indexes.size() == 1) {
                if (wayIntersectionH38Indexes.iterator().next().equals(h38)) {
                    wayIntersectionH38Indexes = null;
                }
            }
        }

        byte[] lineStringWkb = wkbWriter.write(currentWayGeometry);
        byte[] bboxWkb = wkbWriter.write(envelope);

        return new WayGeometry(currentWayGeometry, lineString, bboxGeometry, envelope, centre,
                latitude, longitude, scaleDim, h38, h33Center, closed, nonValid, pointsIdx,
                wayIntersectionH38Indexes, minX, maxX, minY, maxY, lineStringWkb, bboxWkb);
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
