package com.github.isuhorukov.osm.pgsnapshot.util;

import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.List;

final class GeographySegmentizer {

    private static final double WGS84_MAJOR_AXIS = 6378137.0;
    private static final double WGS84_MINOR_AXIS = WGS84_MAJOR_AXIS - WGS84_MAJOR_AXIS / 298.257223563;
    private static final double WGS84_RADIUS = (2.0 * WGS84_MAJOR_AXIS + WGS84_MINOR_AXIS) / 3.0;
    private static final double FP_TOLERANCE = 5e-14;
    private static final double NUDGE_TOLERANCE = 1e-10;

    private GeographySegmentizer() {
    }

    static Coordinate[] segmentize(Coordinate[] ring, double maxSegmentMeters) {
        Coordinate[] points = geodetic(ring);
        double maxSegment = maxSegmentMeters / WGS84_RADIUS;
        List<Coordinate> segmentized = new ArrayList<>(points.length * 16);
        for (int i = 1; i < points.length; i++) {
            Coordinate start = points[i - 1];
            Coordinate end = points[i];
            if (points.length > 2 && start.x == end.x && start.y == end.y) {
                continue;
            }
            double startLatitude = latitudeRadians(toRadians(start.y));
            double startLongitude = longitudeRadians(toRadians(start.x));
            double endLatitude = latitudeRadians(toRadians(end.y));
            double endLongitude = longitudeRadians(toRadians(end.x));
            double distance = sphereDistance(startLatitude, startLongitude, endLatitude, endLongitude);
            if (distance > maxSegment) {
                split(cartesian(startLatitude, startLongitude), cartesian(endLatitude, endLongitude), start, end,
                        distance, maxSegment, segmentized);
            } else {
                append(segmentized, start.x, start.y, true);
            }
        }
        Coordinate last = points[points.length - 1];
        append(segmentized, last.x, last.y, true);
        return segmentized.toArray(new Coordinate[0]);
    }

    private static void split(double[] start, double[] end, Coordinate startPoint, Coordinate endPoint,
                              double distance, double maxSegment, List<Coordinate> segmentized) {
        if (distance <= maxSegment) {
            append(segmentized, startPoint.x, startPoint.y, false);
            return;
        }
        double[] middle = {(start[0] + end[0]) / 2.0, (start[1] + end[1]) / 2.0, (start[2] + end[2]) / 2.0};
        normalize(middle);
        Coordinate middlePoint = new Coordinate(toDegrees(Math.atan2(middle[1], middle[0])),
                toDegrees(Math.asin(middle[2])));
        split(start, middle, startPoint, middlePoint, distance / 2.0, maxSegment, segmentized);
        split(middle, end, middlePoint, endPoint, distance / 2.0, maxSegment, segmentized);
    }

    private static void append(List<Coordinate> segmentized, double x, double y, boolean repeatedPoints) {
        if (!repeatedPoints && !segmentized.isEmpty()) {
            Coordinate last = segmentized.get(segmentized.size() - 1);
            if (x == last.x && y == last.y) {
                return;
            }
        }
        segmentized.add(new Coordinate(x, y));
    }

    private static Coordinate[] geodetic(Coordinate[] ring) {
        Coordinate[] points = new Coordinate[ring.length];
        for (int i = 0; i < ring.length; i++) {
            double x = nudge(ring[i].x, 180.0);
            double y = nudge(ring[i].y, 90.0);
            if (x < -180.0 || x > 180.0 || y < -90.0 || y > 90.0) {
                x = longitudeDegrees(x);
                y = latitudeDegrees(y);
            }
            points[i] = new Coordinate(x, y);
        }
        return points;
    }

    private static double nudge(double value, double limit) {
        if (value < -limit && -limit - value <= NUDGE_TOLERANCE) {
            return -limit;
        }
        if (value > limit && value - limit <= NUDGE_TOLERANCE) {
            return limit;
        }
        return value;
    }

    private static double longitudeDegrees(double longitude) {
        double normalized = longitude;
        if (normalized > 360.0) {
            normalized = Math.IEEEremainder(normalized, 360.0);
        }
        if (normalized < -360.0) {
            normalized = Math.IEEEremainder(normalized, -360.0);
        }
        if (normalized > 180.0) {
            normalized = -360.0 + normalized;
        }
        if (normalized < -180.0) {
            normalized = 360 + normalized;
        }
        if (normalized == -180.0) {
            return 180.0;
        }
        if (normalized == -360.0) {
            return 0.0;
        }
        return normalized;
    }

    private static double latitudeDegrees(double latitude) {
        double normalized = latitude;
        if (normalized > 360.0) {
            normalized = Math.IEEEremainder(normalized, 360.0);
        }
        if (normalized < -360.0) {
            normalized = Math.IEEEremainder(normalized, -360.0);
        }
        if (normalized > 180.0) {
            normalized = 180.0 - normalized;
        }
        if (normalized < -180.0) {
            normalized = -180.0 - normalized;
        }
        if (normalized > 90.0) {
            normalized = 180.0 - normalized;
        }
        if (normalized < -90.0) {
            normalized = -180.0 - normalized;
        }
        return normalized;
    }

    private static double longitudeRadians(double longitude) {
        if (longitude == -1.0 * Math.PI) {
            return Math.PI;
        }
        if (longitude == -2.0 * Math.PI) {
            return 0.0;
        }
        double normalized = longitude;
        if (normalized > 2.0 * Math.PI) {
            normalized = Math.IEEEremainder(normalized, 2.0 * Math.PI);
        }
        if (normalized < -2.0 * Math.PI) {
            normalized = Math.IEEEremainder(normalized, -2.0 * Math.PI);
        }
        if (normalized > Math.PI) {
            normalized = -2.0 * Math.PI + normalized;
        }
        if (normalized < -1.0 * Math.PI) {
            normalized = 2.0 * Math.PI + normalized;
        }
        if (normalized == -2.0 * Math.PI) {
            normalized *= -1.0;
        }
        return normalized;
    }

    private static double latitudeRadians(double latitude) {
        double normalized = latitude;
        if (normalized > 2.0 * Math.PI) {
            normalized = Math.IEEEremainder(normalized, 2.0 * Math.PI);
        }
        if (normalized < -2.0 * Math.PI) {
            normalized = Math.IEEEremainder(normalized, -2.0 * Math.PI);
        }
        if (normalized > Math.PI) {
            normalized = Math.PI - normalized;
        }
        if (normalized < -1.0 * Math.PI) {
            normalized = -1.0 * Math.PI - normalized;
        }
        if (normalized > Math.PI / 2) {
            normalized = Math.PI - normalized;
        }
        if (normalized < -1.0 * Math.PI / 2) {
            normalized = -1.0 * Math.PI - normalized;
        }
        return normalized;
    }

    private static double sphereDistance(double startLatitude, double startLongitude, double endLatitude,
                                         double endLongitude) {
        if (Math.abs(startLatitude - endLatitude) <= FP_TOLERANCE
                && Math.abs(startLongitude - endLongitude) <= FP_TOLERANCE) {
            return 0.0;
        }
        double deltaLongitude = endLongitude - startLongitude;
        double cosDeltaLongitude = Math.cos(deltaLongitude);
        double cosEndLatitude = Math.cos(endLatitude);
        double sinEndLatitude = Math.sin(endLatitude);
        double cosStartLatitude = Math.cos(startLatitude);
        double sinStartLatitude = Math.sin(startLatitude);
        double a1 = cosEndLatitude * Math.sin(deltaLongitude);
        double a2 = cosStartLatitude * sinEndLatitude - sinStartLatitude * cosEndLatitude * cosDeltaLongitude;
        double a = Math.sqrt(a1 * a1 + a2 * a2);
        double b = sinStartLatitude * sinEndLatitude + cosStartLatitude * cosEndLatitude * cosDeltaLongitude;
        return Math.atan2(a, b);
    }

    private static double[] cartesian(double latitude, double longitude) {
        return new double[]{Math.cos(latitude) * Math.cos(longitude), Math.cos(latitude) * Math.sin(longitude),
                Math.sin(latitude)};
    }

    private static void normalize(double[] point) {
        double length = Math.sqrt(point[0] * point[0] + point[1] * point[1] + point[2] * point[2]);
        if (Math.abs(length) <= FP_TOLERANCE) {
            point[0] = 0.0;
            point[1] = 0.0;
            point[2] = 0.0;
            return;
        }
        point[0] = point[0] / length;
        point[1] = point[1] / length;
        point[2] = point[2] / length;
    }

    private static double toRadians(double degrees) {
        return Math.PI * degrees / 180.0;
    }

    private static double toDegrees(double radians) {
        return 180.0 * radians / Math.PI;
    }
}
