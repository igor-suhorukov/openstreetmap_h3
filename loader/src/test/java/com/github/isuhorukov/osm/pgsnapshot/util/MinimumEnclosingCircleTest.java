package com.github.isuhorukov.osm.pgsnapshot.util;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.algorithm.MinimumBoundingCircle;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class MinimumEnclosingCircleTest {

    private static final double TOLERANCE = 1e-12;
    private static final double ENCLOSURE_TOLERANCE = 1e-13;
    private static final GeometryFactory FACTORY = new GeometryFactory();

    private static Coordinate[] coordinates(double... xy) {
        Coordinate[] coordinates = new Coordinate[xy.length / 2];
        for (int i = 0; i < coordinates.length; i++) {
            coordinates[i] = new CoordinateXY(xy[2 * i], xy[2 * i + 1]);
        }
        return coordinates;
    }

    private static void assertCircle(double centreX, double centreY, double radius, MinimumEnclosingCircle circle) {
        assertEquals(centreX, circle.getCentreX(), TOLERANCE, "centre x");
        assertEquals(centreY, circle.getCentreY(), TOLERANCE, "centre y");
        assertEquals(radius, circle.getRadius(), TOLERANCE, "radius");
    }

    private static void assertEnclosesAll(Coordinate[] coordinates, MinimumEnclosingCircle circle) {
        assertFalse(Double.isNaN(circle.getCentreX()) || Double.isNaN(circle.getCentreY()) || Double.isNaN(circle.getRadius()));
        for (Coordinate coordinate : coordinates) {
            double distance = Math.hypot(coordinate.x - circle.getCentreX(), coordinate.y - circle.getCentreY());
            assertTrue(distance <= circle.getRadius() + ENCLOSURE_TOLERANCE,
                    () -> coordinate + " outside " + circle.getRadius() + " in " + Arrays.toString(coordinates));
        }
    }

    private static void assertMatchesJts(Coordinate[] coordinates) {
        MinimumEnclosingCircle circle = MinimumEnclosingCircle.of(coordinates);
        assertEnclosesAll(coordinates, circle);
        MinimumBoundingCircle jts = new MinimumBoundingCircle(coordinates.length > 1
                ? FACTORY.createLineString(coordinates) : FACTORY.createPoint(coordinates[0]));
        Coordinate jtsCentre = jts.getCentre();
        if (jtsCentre == null) {
            assertCircle(coordinates[0].x, coordinates[0].y, 0, circle);
            return;
        }
        String message = Arrays.toString(coordinates);
        assertEquals(jtsCentre.x, circle.getCentreX(), TOLERANCE, message);
        assertEquals(jtsCentre.y, circle.getCentreY(), TOLERANCE, message);
        assertEquals(jts.getRadius(), circle.getRadius(), TOLERANCE, message);
    }

    @Test
    void of_noCoordinates_throws() {
        Coordinate[] empty = new Coordinate[0];
        assertThrows(IllegalArgumentException.class, () -> MinimumEnclosingCircle.of(empty));
    }

    @Test
    void of_singlePoint_pointWithZeroRadius() {
        MinimumEnclosingCircle circle = MinimumEnclosingCircle.of(coordinates(100.5, 13.7));

        assertEquals(100.5, circle.getCentreX());
        assertEquals(13.7, circle.getCentreY());
        assertEquals(0, circle.getRadius());
    }

    @Test
    void of_identicalPoints_pointWithZeroRadius() {
        MinimumEnclosingCircle circle = MinimumEnclosingCircle.of(coordinates(100.5, 13.7, 100.5, 13.7, 100.5, 13.7));

        assertEquals(100.5, circle.getCentreX());
        assertEquals(13.7, circle.getCentreY());
        assertEquals(0, circle.getRadius());
    }

    @Test
    void of_twoPoints_midpointAndHalfDistance() {
        assertCircle(100.55, 13.8, Math.hypot(0.1, 0.2) / 2,
                MinimumEnclosingCircle.of(coordinates(100.5, 13.7, 100.6, 13.9)));
    }

    @Test
    void of_rightTriangle_hypotenuseIsDiameter() {
        assertCircle(100.0015, 13.002, 0.0025,
                MinimumEnclosingCircle.of(coordinates(100, 13, 100.003, 13, 100, 13.004)));
    }

    @Test
    void of_obtuseTriangle_longestSideIsDiameter() {
        assertCircle(100.005, 13, 0.005,
                MinimumEnclosingCircle.of(coordinates(100, 13, 100.01, 13, 100.005, 13.001)));
    }

    @Test
    void of_equilateralTriangle_circumcircle() {
        double side = 0.002;
        double height = side * Math.sqrt(3) / 2;
        assertCircle(100 + side / 2, 13 + height / 3, side / Math.sqrt(3),
                MinimumEnclosingCircle.of(coordinates(100, 13, 100 + side, 13, 100 + side / 2, 13 + height)));
    }

    @Test
    void of_closedRectangleBuilding_rectangleCentreAndHalfDiagonal() {
        assertCircle(98.9805, 18.7905, Math.hypot(0.001, 0.0006) / 2, MinimumEnclosingCircle.of(coordinates(
                98.98, 18.7902, 98.981, 18.7902, 98.981, 18.7908, 98.98, 18.7908, 98.98, 18.7902)));
    }

    @Test
    void of_collinearPointsInAnyOrderWithDuplicates_extremePointsAreDiameter() {
        List<Coordinate> points = new ArrayList<>();
        for (int i = 0; i <= 20; i++) {
            points.add(new CoordinateXY(100 + i * 0.001, 13 + i * 0.002));
            points.add(new CoordinateXY(100 + i * 0.001, 13 + i * 0.002));
        }
        Collections.shuffle(points, new Random(3));
        Coordinate[] coordinates = points.toArray(new Coordinate[0]);

        MinimumEnclosingCircle circle = MinimumEnclosingCircle.of(coordinates);

        assertCircle(100.01, 13.02, Math.hypot(0.02, 0.04) / 2, circle);
        assertEnclosesAll(coordinates, circle);
    }

    @Test
    void of_nearlyCollinearPoints_noNaNAndMatchesJts() {
        Random random = new Random(5);
        for (int test = 0; test < 2000; test++) {
            int count = 3 + random.nextInt(30);
            Coordinate[] coordinates = new Coordinate[count];
            for (int i = 0; i < count; i++) {
                double t = random.nextDouble() * 0.01;
                coordinates[i] = new CoordinateXY(100.123 + t, 13.456 + 0.5 * t + (random.nextDouble() - 0.5) * 1e-15);
            }
            assertMatchesJts(coordinates);
        }
    }

    private static double[] circumcircle(double[] xs, double[] ys) {
        MinimumEnclosingCircle.Welzl welzl = new MinimumEnclosingCircle.Welzl(xs, ys);
        welzl.setCircumcircle(0, 1, 2);
        return new double[]{welzl.getCentreX(), welzl.getCentreY(), welzl.getRadiusSquared()};
    }

    @Test
    void setCircumcircle_collinearPoints_farthestPairDiameterInsteadOfNaN() {
        assertArrayEquals(new double[]{1, 1, 2}, circumcircle(new double[]{0, 2, 1}, new double[]{0, 2, 1}));
    }

    @Test
    void setCircumcircle_radiusAboveBoundingBoxLimit_farthestPairDiameter() {
        assertArrayEquals(new double[]{2, 0, 4}, circumcircle(new double[]{0, 4, 2}, new double[]{0, 0, 1e-9}), 1e-12);
    }

    @Test
    void setCircumcircle_acuteTriangle_circumcircle() {
        assertArrayEquals(new double[]{0, 0, 1}, circumcircle(new double[]{-1, 1, 0}, new double[]{0, 0, 1}), 1e-15);
    }

    @Test
    void of_pointsJustOutsideWithinTolerance_stillEnclosed() {
        double excess = 1 + 1e-11;
        for (int copies = 1; copies <= 8; copies++) {
            List<Coordinate> points = new ArrayList<>();
            for (int i = 0; i < copies; i++) {
                points.add(new CoordinateXY(100, 13));
                points.add(new CoordinateXY(102, 13));
            }
            points.add(new CoordinateXY(101, 13 + excess));
            points.add(new CoordinateXY(101, 13 - excess));
            Coordinate[] coordinates = points.toArray(new Coordinate[0]);

            MinimumEnclosingCircle circle = MinimumEnclosingCircle.of(coordinates);

            for (Coordinate coordinate : coordinates) {
                double distance = Math.hypot(coordinate.x - circle.getCentreX(), coordinate.y - circle.getCentreY());
                assertTrue(distance <= circle.getRadius() * (1 + 1e-15), coordinate + " outside, copies " + copies);
            }
            assertEquals(excess, circle.getRadius(), 1e-12);
        }
    }

    @Test
    void of_pointsOnCircle_circumscribedCircle() {
        int count = 1000;
        Coordinate[] coordinates = new Coordinate[count];
        for (int i = 0; i < count; i++) {
            double angle = 2 * Math.PI * i / count;
            coordinates[i] = new CoordinateXY(100.5 + 0.01 * Math.cos(angle), 13.5 + 0.01 * Math.sin(angle));
        }

        MinimumEnclosingCircle circle = MinimumEnclosingCircle.of(coordinates);

        assertCircle(100.5, 13.5, 0.01, circle);
        assertEnclosesAll(coordinates, circle);
    }

    @Test
    void of_sameInput_identicalResult() {
        Coordinate[] coordinates = coordinates(100.5645673, 13.7686574, 100.5645263, 13.7685423, 100.5647745,
                13.7684789, 100.5648062, 13.768603, 100.5647701, 13.7686112, 100.5645673, 13.7686574);

        MinimumEnclosingCircle first = MinimumEnclosingCircle.of(coordinates);
        MinimumEnclosingCircle second = MinimumEnclosingCircle.of(coordinates.clone());

        assertEquals(first.getCentreX(), second.getCentreX());
        assertEquals(first.getCentreY(), second.getCentreY());
        assertEquals(first.getRadius(), second.getRadius());
    }

    @Test
    void of_permutedInput_sameCircle() {
        Random random = new Random(11);
        List<Coordinate> points = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            points.add(new CoordinateXY(100 + random.nextDouble() * 0.01, 13 + random.nextDouble() * 0.01));
        }
        MinimumEnclosingCircle expected = MinimumEnclosingCircle.of(points.toArray(new Coordinate[0]));
        for (int permutation = 0; permutation < 20; permutation++) {
            Collections.shuffle(points, random);
            assertCircle(expected.getCentreX(), expected.getCentreY(), expected.getRadius(),
                    MinimumEnclosingCircle.of(points.toArray(new Coordinate[0])));
        }
    }

    @Test
    void of_coordinatesNearAntimeridianAndPoles_matchesJts() {
        assertMatchesJts(coordinates(179.9999, 89.99, 179.99995, 89.9901, 179.99991, 89.99015, 179.9999, 89.99));
        assertMatchesJts(coordinates(-179.9999, -89.99, -179.99995, -89.9901, -179.99991, -89.99015));
        assertMatchesJts(coordinates(-0.0001, -0.0002, 0.0003, 0.0001, 0.0002, -0.0003, -0.0001, -0.0002));
    }

    @Test
    void of_randomWays_matchJtsMinimumBoundingCircle() {
        Random random = new Random(1);
        for (int test = 0; test < 200_000; test++) {
            int count = 1 + random.nextInt(test % 20 == 0 ? 300 : 12);
            int mode = random.nextInt(5);
            double scale = mode == 4 ? 1e-5 : 0.01;
            Coordinate[] coordinates = new Coordinate[count];
            for (int i = 0; i < count; i++) {
                double x;
                double y;
                if (mode == 1) {
                    x = random.nextInt(4) * 0.25;
                    y = random.nextInt(4) * 0.25;
                } else if (mode == 2) {
                    double t = random.nextInt(6) * 0.1;
                    x = t;
                    y = 2 * t;
                } else if (mode == 3) {
                    double angle = random.nextInt(16) * Math.PI / 8;
                    x = Math.cos(angle);
                    y = Math.sin(angle);
                } else {
                    x = random.nextDouble();
                    y = random.nextDouble();
                }
                coordinates[i] = new CoordinateXY(98 + 7 * (test % 7) / 7.0 + x * scale, 6 + 14 * (test % 5) / 5.0 + y * scale);
            }
            if (count > 2 && random.nextBoolean()) {
                coordinates[count - 1] = new CoordinateXY(coordinates[0].x, coordinates[0].y);
            }
            assertMatchesJts(coordinates);
        }
    }

    @Test
    void of_largeWay_matchesJts() {
        Random random = new Random(7);
        Coordinate[] coordinates = new Coordinate[100_000];
        for (int i = 0; i < coordinates.length; i++) {
            double angle = random.nextDouble() * 2 * Math.PI;
            double distance = Math.sqrt(random.nextDouble()) * 0.5;
            coordinates[i] = new CoordinateXY(100 + distance * Math.cos(angle), 15 + distance * Math.sin(angle));
        }
        assertMatchesJts(coordinates);
    }
}
