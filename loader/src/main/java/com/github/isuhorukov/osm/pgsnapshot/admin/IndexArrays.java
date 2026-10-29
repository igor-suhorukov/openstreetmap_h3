package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class IndexArrays {

    final long[] xy;
    final int[] ringStart;
    final int[] pieceRing;
    final int[] pieceUnit;
    final int[] pieceBox;
    final long[] cut;

    IndexArrays(long[] xy, int[] ringStart, int[] pieceRing, int[] pieceUnit, int[] pieceBox, long[] cut) {
        this.xy = xy;
        this.ringStart = ringStart;
        this.pieceRing = pieceRing;
        this.pieceUnit = pieceUnit;
        this.pieceBox = pieceBox;
        this.cut = cut;
    }

    static IndexArrays of(List<Polygon> pieces, int[] pieceUnits) {
        int count = pieces.size();
        int rings = 0;
        long vertices = 0;
        for (Polygon polygon : pieces) {
            rings += 1 + polygon.getNumInteriorRing();
            vertices += polygon.getNumPoints();
        }
        long[] xy = new long[Math.toIntExact(vertices)];
        int[] ringStart = new int[rings + 1];
        int[] pieceRing = new int[count + 1];
        int[] pieceBox = new int[4 * count];
        int ring = 0;
        int vertex = 0;
        for (int i = 0; i < count; i++) {
            Polygon polygon = pieces.get(i);
            pieceRing[i] = ring;
            int[] box = {Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE};
            for (int h = -1; h < polygon.getNumInteriorRing(); h++) {
                LineString line = h < 0 ? polygon.getExteriorRing() : polygon.getInteriorRingN(h);
                ringStart[ring++] = vertex;
                vertex = addRing(line.getCoordinateSequence(), xy, vertex, box);
            }
            System.arraycopy(box, 0, pieceBox, 4 * i, 4);
        }
        ringStart[ring] = vertex;
        pieceRing[count] = ring;
        int[] pieceUnit = pieceUnits.clone();
        return new IndexArrays(xy, ringStart, pieceRing, pieceUnit, pieceBox, cutEdges(xy, ringStart, pieceRing, pieceUnit));
    }

    private static int addRing(CoordinateSequence sequence, long[] xy, int start, int[] box) {
        int vertex = start;
        for (int j = 0; j < sequence.size(); j++) {
            int x = GridGeometry.grid(sequence.getX(j));
            int y = GridGeometry.grid(sequence.getY(j));
            xy[vertex++] = pack(x, y);
            box[0] = Math.min(box[0], x);
            box[1] = Math.min(box[1], y);
            box[2] = Math.max(box[2], x);
            box[3] = Math.max(box[3], y);
        }
        return vertex;
    }

    static long pack(int x, int y) {
        return ((long) x << 32) | (y & 0xffffffffL);
    }

    static int xOf(long value) {
        return (int) (value >> 32);
    }

    static int yOf(long value) {
        return (int) value;
    }

    boolean isCut(int edge) {
        return (cut[edge >>> 6] & (1L << edge)) != 0;
    }

    private static long[] cutEdges(long[] xy, int[] ringStart, int[] pieceRing, int[] pieceUnit) {
        long[] cut = new long[(xy.length + 63) >>> 6];
        for (List<long[]> edges : axisEdges(xy, ringStart, pieceRing, pieceUnit).values()) {
            if (edges.size() < 2) {
                continue;
            }
            edges.sort(Comparator.comparingLong(edge -> edge[0]));
            for (long[] edge : edges) {
                if (covered(edges, edge)) {
                    int i = (int) edge[3];
                    cut[i >>> 6] |= 1L << i;
                }
            }
        }
        return cut;
    }

    private static Map<Long, List<long[]>> axisEdges(long[] xy, int[] ringStart, int[] pieceRing, int[] pieceUnit) {
        Map<Long, List<long[]>> lines = new HashMap<>();
        for (int piece = 0; piece < pieceUnit.length; piece++) {
            for (int r = pieceRing[piece]; r < pieceRing[piece + 1]; r++) {
                for (int i = ringStart[r]; i < ringStart[r + 1] - 1; i++) {
                    addAxisEdge(lines, pieceUnit[piece], piece, i, xy[i], xy[i + 1]);
                }
            }
        }
        return lines;
    }

    private static void addAxisEdge(Map<Long, List<long[]>> lines, int unit, int piece, int edge, long a, long b) {
        int ax = xOf(a);
        int ay = yOf(a);
        int bx = xOf(b);
        int by = yOf(b);
        if (ax == bx && ay != by) {
            lines.computeIfAbsent(lineKey(unit, 0, ax), key -> new ArrayList<>())
                    .add(new long[]{Math.min(ay, by), Math.max(ay, by), piece, edge});
        } else if (ay == by && ax != bx) {
            lines.computeIfAbsent(lineKey(unit, 1, ay), key -> new ArrayList<>())
                    .add(new long[]{Math.min(ax, bx), Math.max(ax, bx), piece, edge});
        }
    }

    private static long lineKey(int unit, int axis, int coordinate) {
        return ((long) unit << 33) | ((long) axis << 32) | (coordinate & 0xffffffffL);
    }

    private static boolean covered(List<long[]> edges, long[] edge) {
        long reach = edge[0];
        for (int i = 0; i < edges.size() && edges.get(i)[0] <= reach; i++) {
            long[] other = edges.get(i);
            if (other[2] != edge[2] && other[1] > reach) {
                reach = other[1];
            }
            if (reach >= edge[1]) {
                return true;
            }
        }
        return false;
    }
}
