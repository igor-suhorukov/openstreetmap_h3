package com.github.isuhorukov.osm.pgsnapshot.util;

import org.locationtech.jts.geom.Coordinate;

public final class MinimumEnclosingCircle {

    private static final double OUTSIDE_TOLERANCE = 1e-10;
    private static final long SHUFFLE_SEED = 0x9E3779B97F4A7C15L;

    private final double centreX;
    private final double centreY;
    private final double radius;

    private MinimumEnclosingCircle(double centreX, double centreY, double radius) {
        this.centreX = centreX;
        this.centreY = centreY;
        this.radius = radius;
    }

    public static MinimumEnclosingCircle of(Coordinate[] coordinates) {
        int count = coordinates.length;
        if (count == 0) {
            throw new IllegalArgumentException("At least one coordinate is required");
        }
        double originX = coordinates[0].x;
        double originY = coordinates[0].y;
        double[] xs = new double[count];
        double[] ys = new double[count];
        for (int i = 0; i < count; i++) {
            xs[i] = coordinates[i].x - originX;
            ys[i] = coordinates[i].y - originY;
        }
        Welzl welzl = new Welzl(xs, ys);
        welzl.compute();
        return new MinimumEnclosingCircle(originX + welzl.centreX, originY + welzl.centreY, welzl.enclosingRadius());
    }

    public double getCentreX() { return centreX; }
    public double getCentreY() { return centreY; }
    public double getRadius() { return radius; }

    static final class Welzl {
        private final double[] xs;
        private final double[] ys;
        private final double maxRadiusSquared;
        private double centreX;
        private double centreY;
        private double radiusSquared;

        Welzl(double[] xs, double[] ys) {
            this.xs = xs;
            this.ys = ys;
            double minX = 0;
            double maxX = 0;
            double minY = 0;
            double maxY = 0;
            for (int i = 0; i < xs.length; i++) {
                minX = Math.min(minX, xs[i]);
                maxX = Math.max(maxX, xs[i]);
                minY = Math.min(minY, ys[i]);
                maxY = Math.max(maxY, ys[i]);
            }
            double width = maxX - minX;
            double height = maxY - minY;
            this.maxRadiusSquared = (width * width + height * height) / 4 * (1 + OUTSIDE_TOLERANCE);
        }

        void compute() {
            shuffle();
            setPoint(0);
            for (int i = 1; i < xs.length; i++) {
                if (isOutside(i)) {
                    computeWithBoundaryPoint(i);
                }
            }
        }

        double getCentreX() { return centreX; }
        double getCentreY() { return centreY; }
        double getRadiusSquared() { return radiusSquared; }

        double enclosingRadius() {
            double enclosingRadiusSquared = 0;
            for (int i = 0; i < xs.length; i++) {
                enclosingRadiusSquared = Math.max(enclosingRadiusSquared, distanceSquared(i, centreX, centreY));
            }
            return Math.sqrt(enclosingRadiusSquared);
        }

        void setCircumcircle(int first, int second, int third) {
            double ax = xs[first] - xs[third];
            double ay = ys[first] - ys[third];
            double bx = xs[second] - xs[third];
            double by = ys[second] - ys[third];
            double denominator = 2 * (ax * by - ay * bx);
            double aSquared = ax * ax + ay * ay;
            double bSquared = bx * bx + by * by;
            double circumcentreX = xs[third] + (by * aSquared - ay * bSquared) / denominator;
            double circumcentreY = ys[third] + (ax * bSquared - bx * aSquared) / denominator;
            double circumradiusSquared = Math.max(distanceSquared(first, circumcentreX, circumcentreY),
                    Math.max(distanceSquared(second, circumcentreX, circumcentreY),
                            distanceSquared(third, circumcentreX, circumcentreY)));
            if (circumradiusSquared <= maxRadiusSquared) {
                centreX = circumcentreX;
                centreY = circumcentreY;
                radiusSquared = circumradiusSquared;
            } else {
                setFarthestPairDiameter(first, second, third);
            }
        }

        private void computeWithBoundaryPoint(int boundary) {
            setPoint(boundary);
            for (int j = 0; j < boundary; j++) {
                if (isOutside(j)) {
                    computeWithBoundaryPoints(boundary, j);
                }
            }
        }

        private void computeWithBoundaryPoints(int first, int second) {
            setDiameter(first, second);
            for (int k = 0; k < second; k++) {
                if (isOutside(k)) {
                    setCircumcircle(first, second, k);
                }
            }
        }

        private void shuffle() {
            long state = SHUFFLE_SEED ^ xs.length;
            for (int i = xs.length - 1; i > 0; i--) {
                state ^= state << 13;
                state ^= state >>> 7;
                state ^= state << 17;
                int j = (int) Long.remainderUnsigned(state, i + 1L);
                double x = xs[i];
                xs[i] = xs[j];
                xs[j] = x;
                double y = ys[i];
                ys[i] = ys[j];
                ys[j] = y;
            }
        }

        private boolean isOutside(int point) {
            return distanceSquared(point, centreX, centreY) > radiusSquared * (1 + OUTSIDE_TOLERANCE);
        }

        private void setPoint(int point) {
            centreX = xs[point];
            centreY = ys[point];
            radiusSquared = 0;
        }

        private void setDiameter(int first, int second) {
            double diameterCentreX = (xs[first] + xs[second]) / 2;
            double diameterCentreY = (ys[first] + ys[second]) / 2;
            centreX = diameterCentreX;
            centreY = diameterCentreY;
            radiusSquared = Math.max(distanceSquared(first, diameterCentreX, diameterCentreY),
                    distanceSquared(second, diameterCentreX, diameterCentreY));
        }

        private void setFarthestPairDiameter(int first, int second, int third) {
            double d12 = distanceSquared(first, xs[second], ys[second]);
            double d13 = distanceSquared(first, xs[third], ys[third]);
            double d23 = distanceSquared(second, xs[third], ys[third]);
            if (d12 >= d13 && d12 >= d23) {
                setDiameter(first, second);
            } else if (d13 >= d23) {
                setDiameter(first, third);
            } else {
                setDiameter(second, third);
            }
        }

        private double distanceSquared(int point, double x, double y) {
            double dx = xs[point] - x;
            double dy = ys[point] - y;
            return dx * dx + dy * dy;
        }
    }
}
