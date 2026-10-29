package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.operation.overlayng.OverlayNG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.isuhorukov.osm.pgsnapshot.admin.IndexArrays.xOf;
import static com.github.isuhorukov.osm.pgsnapshot.admin.IndexArrays.yOf;

final class CompactIndex {

    static final int EXTERIOR = 0;
    static final int INTERIOR = 1;
    static final int BOUNDARY = 2;
    private static final int ON_BOUNDARY = -1;
    private static final byte APART = 0;
    private static final byte NEAR = 1;
    private static final byte HIT = 2;
    private static final int SEGMENT_BUDGET = 4096;

    private final List<AdminUnit> units;
    private final AdminOptions options;
    private final IndexArrays arrays;
    private final long[] xy;
    private final int[] ringStart;
    private final int[] pieceRing;
    private final int[] pieceUnit;
    private final PackedTree tree;
    private final Comparator<Integer> pointOrder;

    CompactIndex(List<AdminUnit> units, AdminOptions options, IndexArrays arrays) {
        this.units = units;
        this.options = options;
        this.arrays = arrays;
        this.xy = arrays.xy;
        this.ringStart = arrays.ringStart;
        this.pieceRing = arrays.pieceRing;
        this.pieceUnit = arrays.pieceUnit;
        this.tree = new PackedTree(arrays.pieceBox.clone());
        this.pointOrder = Comparator.comparingInt((Integer unit) -> -units.get(unit).getLevel())
                .thenComparingDouble(unit -> units.get(unit).getAreaM2())
                .thenComparingLong(unit -> units.get(unit).getId());
    }

    static CompactIndex of(List<AdminUnit> units, AdminOptions options, List<Polygon> pieces, int[] pieceUnits) {
        return new CompactIndex(units, options, IndexArrays.of(pieces, pieceUnits));
    }

    IndexArrays getArrays() {
        return arrays;
    }

    int pieceCount() {
        return pieceUnit.length;
    }

    int pieceUnit(int piece) {
        return pieceUnit[piece];
    }

    int locate(int piece, long x, long y) {
        boolean inside = false;
        for (int r = pieceRing[piece]; r < pieceRing[piece + 1]; r++) {
            int crossings = ringCrossings(r, x, y);
            if (crossings == ON_BOUNDARY) {
                return BOUNDARY;
            }
            inside ^= crossings == 1;
        }
        return inside ? INTERIOR : EXTERIOR;
    }

    private int ringCrossings(int r, long x, long y) {
        int parity = 0;
        long ax = xOf(xy[ringStart[r]]);
        long ay = yOf(xy[ringStart[r]]);
        for (int i = ringStart[r] + 1; i < ringStart[r + 1]; i++) {
            long bx = xOf(xy[i]);
            long by = yOf(xy[i]);
            long cross = (bx - ax) * (y - ay) - (x - ax) * (by - ay);
            boolean bAbove = by > y;
            if ((ay > y) != bAbove) {
                if (cross == 0) {
                    return ON_BOUNDARY;
                }
                parity ^= (cross > 0) == bAbove ? 1 : 0;
            } else if ((ay == y || by == y) && x >= Math.min(ax, bx) && x <= Math.max(ax, bx) && cross == 0) {
                return ON_BOUNDARY;
            }
            ax = bx;
            ay = by;
        }
        return parity;
    }

    UnitShares point(double longitude, double latitude) {
        return point(GridGeometry.grid(longitude), GridGeometry.grid(latitude));
    }

    UnitShares point(int x, int y) {
        PackedTree.Scratch scratch = new PackedTree.Scratch();
        int candidates = tree.search(x, y, x, y, scratch);
        int found = 0;
        for (int i = 0; i < candidates; i++) {
            int piece = scratch.candidates[i];
            if (locate(piece, x, y) != EXTERIOR && !contains(scratch.hits, found, pieceUnit[piece])) {
                scratch.hits = PackedTree.push(scratch.hits, found++, pieceUnit[piece]);
            }
        }
        if (found == 0) {
            return UnitShares.NONE;
        }
        Integer[] order = new Integer[found];
        for (int i = 0; i < found; i++) {
            order[i] = scratch.hits[i];
        }
        Arrays.sort(order, pointOrder);
        int[] result = new int[found];
        float[] shares = new float[found];
        for (int i = 0; i < found; i++) {
            result[i] = order[i];
            shares[i] = 1f;
        }
        return new UnitShares(result, shares);
    }

    private static boolean contains(int[] values, int count, int value) {
        for (int i = 0; i < count; i++) {
            if (values[i] == value) {
                return true;
            }
        }
        return false;
    }

    UnitShares geometry(Geometry geometry, boolean areal) {
        double total = areal ? geometry.getArea() : geometry.getLength();
        if (total <= 0) {
            Coordinate centre = geometry.getEnvelopeInternal().centre();
            return point(centre.x, centre.y);
        }
        List<Geometry> parts = new ArrayList<>();
        if (geometry.getNumPoints() > 4 * GridGeometry.MAX_VERTICES) {
            GridGeometry.subdivide(geometry, parts);
        } else {
            parts.add(geometry);
        }
        Map<Integer, Double> amounts = new HashMap<>(4);
        for (Geometry part : parts) {
            for (int k = 0; k < part.getNumGeometries(); k++) {
                component(part.getGeometryN(k), areal, amounts);
            }
        }
        return finish(amounts, total, areal, geometry);
    }

    private void component(Geometry component, boolean areal, Map<Integer, Double> amounts) {
        double measure = areal ? component.getArea() : component.getLength();
        if (measure <= 0) {
            return;
        }
        ComponentEdges edges = new ComponentEdges(component);
        PackedTree.Scratch scratch = new PackedTree.Scratch();
        int count = tree.search(edges.minX, edges.minY, edges.maxX, edges.maxY, scratch);
        if (count == 0) {
            return;
        }
        Candidates candidates = new Candidates(component, areal, measure, edges, Arrays.copyOf(scratch.candidates, count));
        for (int unit : candidates.units) {
            if (candidates.hit(unit)) {
                addPieces(unit, candidates, amounts);
            } else if (candidates.refInside(unit)) {
                amounts.merge(unit, measure, Double::sum);
            }
        }
    }

    private void addPieces(int unit, Candidates candidates, Map<Integer, Double> amounts) {
        for (int i = 0; i < candidates.pieces.length; i++) {
            if (pieceUnit[candidates.pieces[i]] == unit) {
                double amount = candidates.amount(i);
                if (amount > 0) {
                    amounts.merge(unit, amount, Double::sum);
                }
            }
        }
    }

    private UnitShares finish(Map<Integer, Double> amounts, double total, boolean areal, Geometry geometry) {
        if (amounts.isEmpty()) {
            return UnitShares.NONE;
        }
        boolean small = false;
        for (double amount : amounts.values()) {
            small |= amount / total < options.getMinShare();
        }
        double totalMetric = 0;
        if (small) {
            totalMetric = areal ? GridGeometry.areaM2(geometry) : GridGeometry.lengthM(geometry);
        }
        double minMetric = areal ? options.getMinAreaM2() : options.getMinLengthM();
        List<Map.Entry<Integer, Double>> kept = new ArrayList<>(amounts.size());
        for (Map.Entry<Integer, Double> amount : amounts.entrySet()) {
            double share = amount.getValue() / total;
            if (share >= options.getMinShare() || share * totalMetric >= minMetric) {
                kept.add(amount);
            }
        }
        kept.sort(Comparator.comparingDouble((Map.Entry<Integer, Double> amount) -> -amount.getValue())
                .thenComparingInt(amount -> -units.get(amount.getKey()).getLevel())
                .thenComparingDouble(amount -> units.get(amount.getKey()).getAreaM2())
                .thenComparingLong(amount -> units.get(amount.getKey()).getId()));
        int[] result = new int[kept.size()];
        float[] shares = new float[kept.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = kept.get(i).getKey();
            shares[i] = (float) (kept.get(i).getValue() / total);
        }
        return new UnitShares(result, shares);
    }

    Polygon piece(int piece) {
        int first = pieceRing[piece];
        int last = pieceRing[piece + 1];
        LinearRing[] holes = new LinearRing[last - first - 1];
        for (int r = first + 1; r < last; r++) {
            holes[r - first - 1] = ring(r);
        }
        return GridGeometry.FACTORY.createPolygon(ring(first), holes);
    }

    private LinearRing ring(int r) {
        Coordinate[] coordinates = new Coordinate[ringStart[r + 1] - ringStart[r]];
        for (int i = 0; i < coordinates.length; i++) {
            long value = xy[ringStart[r] + i];
            coordinates[i] = new Coordinate(xOf(value) / GridGeometry.SCALE, yOf(value) / GridGeometry.SCALE);
        }
        return GridGeometry.FACTORY.createLinearRing(coordinates);
    }

    private static boolean within(long p, long q, long r) {
        long rx = xOf(r);
        long ry = yOf(r);
        return rx >= Math.min(xOf(p), xOf(q)) && rx <= Math.max(xOf(p), xOf(q))
                && ry >= Math.min(yOf(p), yOf(q)) && ry <= Math.max(yOf(p), yOf(q));
    }

    private static long orient(long p, long q, long r) {
        long px = xOf(p);
        long py = yOf(p);
        return (xOf(q) - px) * (yOf(r) - py) - (yOf(q) - py) * (xOf(r) - px);
    }

    private static boolean opposite(long first, long second) {
        return (first > 0 && second < 0) || (first < 0 && second > 0);
    }

    static boolean segmentsIntersect(long a, long b, long c, long d) {
        long d1 = orient(c, d, a);
        long d2 = orient(c, d, b);
        long d3 = orient(a, b, c);
        long d4 = orient(a, b, d);
        if (opposite(d1, d2) && opposite(d3, d4)) {
            return true;
        }
        return (d1 == 0 && within(c, d, a)) || (d2 == 0 && within(c, d, b))
                || (d3 == 0 && within(a, b, c)) || (d4 == 0 && within(a, b, d));
    }

    private final class Candidates {
        private final Geometry component;
        private final boolean areal;
        private final double measure;
        private final ComponentEdges edges;
        private final int[] pieces;
        private final byte[] state;
        private final int[] units;

        private Candidates(Geometry component, boolean areal, double measure, ComponentEdges edges, int[] pieces) {
            this.component = component;
            this.areal = areal;
            this.measure = measure;
            this.edges = edges;
            this.pieces = pieces;
            this.state = new byte[pieces.length];
            int[] distinct = new int[pieces.length];
            int count = 0;
            for (int i = 0; i < pieces.length; i++) {
                state[i] = classify(pieces[i], edges);
                if (!contains(distinct, count, pieceUnit[pieces[i]])) {
                    distinct[count++] = pieceUnit[pieces[i]];
                }
            }
            this.units = Arrays.copyOf(distinct, count);
        }

        private byte classify(int piece, ComponentEdges edges) {
            boolean near = false;
            int budget = SEGMENT_BUDGET;
            for (int r = pieceRing[piece]; r < pieceRing[piece + 1]; r++) {
                for (int i = ringStart[r]; i < ringStart[r + 1] - 1; i++) {
                    if (!edges.near(xy[i], xy[i + 1])) {
                        continue;
                    }
                    near = true;
                    if (!arrays.isCut(i)) {
                        budget -= 2 * edges.edgeCount;
                        if (budget < 0 || edges.hits(xy[i], xy[i + 1])) {
                            return HIT;
                        }
                    }
                }
            }
            return near ? NEAR : APART;
        }

        private boolean hit(int unit) {
            for (int i = 0; i < pieces.length; i++) {
                if (pieceUnit[pieces[i]] == unit && state[i] == HIT) {
                    return true;
                }
            }
            return false;
        }

        private boolean refInside(int unit) {
            for (int piece : pieces) {
                if (pieceUnit[piece] == unit && locate(piece, edges.refX, edges.refY) != EXTERIOR) {
                    return true;
                }
            }
            return false;
        }

        private double amount(int i) {
            if (state[i] == APART) {
                return locate(pieces[i], edges.refX, edges.refY) != EXTERIOR ? measure : 0;
            }
            Geometry intersection = GridGeometry.overlay(component, piece(pieces[i]), OverlayNG.INTERSECTION);
            return areal ? intersection.getArea() : intersection.getLength();
        }
    }

    private static final class ComponentEdges {
        private final long[][] rings;
        private final boolean areal;
        private final int edgeCount;
        private final int minX;
        private final int minY;
        private final int maxX;
        private final int maxY;
        private final int refX;
        private final int refY;

        private ComponentEdges(Geometry component) {
            List<CoordinateSequence> sequences = sequences(component);
            areal = component instanceof Polygon;
            rings = new long[sequences.size()][];
            int[] box = {Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
            int edges = 0;
            for (int s = 0; s < sequences.size(); s++) {
                CoordinateSequence sequence = sequences.get(s);
                rings[s] = new long[sequence.size()];
                for (int j = 0; j < sequence.size(); j++) {
                    int x = GridGeometry.grid(sequence.getX(j));
                    int y = GridGeometry.grid(sequence.getY(j));
                    rings[s][j] = IndexArrays.pack(x, y);
                    box[0] = Math.min(box[0], x);
                    box[1] = Math.min(box[1], y);
                    box[2] = Math.max(box[2], x);
                    box[3] = Math.max(box[3], y);
                }
                edges += Math.max(0, sequence.size() - 1);
            }
            edgeCount = Math.max(1, edges);
            minX = box[0];
            minY = box[1];
            maxX = box[2];
            maxY = box[3];
            refX = xOf(rings[0][0]);
            refY = yOf(rings[0][0]);
        }

        private static List<CoordinateSequence> sequences(Geometry component) {
            List<CoordinateSequence> sequences = new ArrayList<>();
            if (component instanceof Polygon) {
                Polygon polygon = (Polygon) component;
                sequences.add(polygon.getExteriorRing().getCoordinateSequence());
                for (int h = 0; h < polygon.getNumInteriorRing(); h++) {
                    sequences.add(polygon.getInteriorRingN(h).getCoordinateSequence());
                }
            } else if (component instanceof LineString) {
                sequences.add(((LineString) component).getCoordinateSequence());
            } else {
                throw new IllegalArgumentException("Unexpected component " + component.getGeometryType());
            }
            return sequences;
        }

        private boolean near(long a, long b) {
            return Math.max(xOf(a), xOf(b)) >= minX && Math.min(xOf(a), xOf(b)) <= maxX
                    && Math.max(yOf(a), yOf(b)) >= minY && Math.min(yOf(a), yOf(b)) <= maxY;
        }

        private boolean hits(long a, long b) {
            return touches(a, b) || (areal && contains(xOf(a), yOf(a)));
        }

        private boolean touches(long a, long b) {
            for (long[] ring : rings) {
                for (int j = 0; j + 1 < ring.length; j++) {
                    if (boxesOverlap(a, b, ring[j], ring[j + 1]) && segmentsIntersect(a, b, ring[j], ring[j + 1])) {
                        return true;
                    }
                }
            }
            return false;
        }

        private static boolean boxesOverlap(long a, long b, long c, long d) {
            return Math.max(xOf(c), xOf(d)) >= Math.min(xOf(a), xOf(b))
                    && Math.min(xOf(c), xOf(d)) <= Math.max(xOf(a), xOf(b))
                    && Math.max(yOf(c), yOf(d)) >= Math.min(yOf(a), yOf(b))
                    && Math.min(yOf(c), yOf(d)) <= Math.max(yOf(a), yOf(b));
        }

        private boolean contains(long x, long y) {
            boolean inside = false;
            for (long[] ring : rings) {
                for (int j = 0; j + 1 < ring.length; j++) {
                    long ax = xOf(ring[j]);
                    long ay = yOf(ring[j]);
                    long bx = xOf(ring[j + 1]);
                    long by = yOf(ring[j + 1]);
                    boolean bAbove = by > y;
                    if ((ay > y) != bAbove && ((bx - ax) * (y - ay) - (x - ax) * (by - ay) > 0) == bAbove) {
                        inside = !inside;
                    }
                }
            }
            return inside;
        }
    }
}
