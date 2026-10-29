package com.github.isuhorukov.osm.pgsnapshot.admin;

import java.util.Arrays;

import static com.github.isuhorukov.osm.pgsnapshot.admin.IndexArrays.xOf;
import static com.github.isuhorukov.osm.pgsnapshot.admin.IndexArrays.yOf;

final class LatitudeBands {

    private static final int MIN_EDGES = 16;
    private static final int EDGES_PER_BAND = 8;
    private static final int MAX_BANDS = 32;
    private static final int MAX_OFFSET = 0xFF;
    private static final int ON_BOUNDARY = -1;

    private final long[] xy;
    private final int[] ringStart;
    private final int[] pieceRing;
    private final int[] pieceBox;
    private final int[] pieceBand;
    private final int[] bandHeight;
    private final int[] bandStart;
    private final byte[] edges;

    private LatitudeBands(IndexArrays arrays, int[] pieceBand, int[] bandHeight, int[] bandStart, byte[] edges) {
        this.xy = arrays.xy;
        this.ringStart = arrays.ringStart;
        this.pieceRing = arrays.pieceRing;
        this.pieceBox = arrays.pieceBox;
        this.pieceBand = pieceBand;
        this.bandHeight = bandHeight;
        this.bandStart = bandStart;
        this.edges = edges;
    }

    static LatitudeBands of(IndexArrays arrays) {
        Builder builder = new Builder(arrays);
        for (int piece = 0; piece < arrays.pieceUnit.length; piece++) {
            builder.add(piece);
        }
        return builder.build();
    }

    boolean hasBands(int piece) {
        return pieceBand[piece + 1] > pieceBand[piece];
    }

    int bandCount(int piece) {
        return Math.max(0, pieceBand[piece + 1] - pieceBand[piece] - 1);
    }

    int locate(int piece, long x, long y) {
        if (!hasBands(piece)) {
            return locateAll(piece, x, y);
        }
        int minY = pieceBox[4 * piece + 1];
        if (y < minY || y > pieceBox[4 * piece + 3]) {
            return CompactIndex.EXTERIOR;
        }
        int band = pieceBand[piece] + (int) (y - minY) / bandHeight[piece];
        int base = ringStart[pieceRing[piece]];
        boolean inside = false;
        for (int k = bandStart[band]; k < bandStart[band + 1]; k++) {
            int i = base + (edges[k] & MAX_OFFSET);
            int crossing = crossing(xy[i], xy[i + 1], x, y);
            if (crossing == ON_BOUNDARY) {
                return CompactIndex.BOUNDARY;
            }
            inside ^= crossing == 1;
        }
        return inside ? CompactIndex.INTERIOR : CompactIndex.EXTERIOR;
    }

    private int locateAll(int piece, long x, long y) {
        boolean inside = false;
        for (int r = pieceRing[piece]; r < pieceRing[piece + 1]; r++) {
            for (int i = ringStart[r]; i < ringStart[r + 1] - 1; i++) {
                int crossing = crossing(xy[i], xy[i + 1], x, y);
                if (crossing == ON_BOUNDARY) {
                    return CompactIndex.BOUNDARY;
                }
                inside ^= crossing == 1;
            }
        }
        return inside ? CompactIndex.INTERIOR : CompactIndex.EXTERIOR;
    }

    static int crossing(long a, long b, long x, long y) {
        long ax = xOf(a);
        long ay = yOf(a);
        long bx = xOf(b);
        long by = yOf(b);
        long cross = (bx - ax) * (y - ay) - (x - ax) * (by - ay);
        boolean bAbove = by > y;
        if ((ay > y) != bAbove) {
            if (cross == 0) {
                return ON_BOUNDARY;
            }
            return (cross > 0) == bAbove ? 1 : 0;
        }
        if ((ay == y || by == y) && x >= Math.min(ax, bx) && x <= Math.max(ax, bx) && cross == 0) {
            return ON_BOUNDARY;
        }
        return 0;
    }

    private static final class Builder {
        private final IndexArrays arrays;
        private final int[] pieceBand;
        private final int[] bandHeight;
        private int[] bandStart = new int[1024];
        private byte[] edges = new byte[4096];
        private int bands;
        private int entries;

        private Builder(IndexArrays arrays) {
            this.arrays = arrays;
            this.pieceBand = new int[arrays.pieceUnit.length + 1];
            this.bandHeight = new int[arrays.pieceUnit.length];
        }

        private void add(int piece) {
            pieceBand[piece] = bands;
            int first = arrays.ringStart[arrays.pieceRing[piece]];
            int last = arrays.ringStart[arrays.pieceRing[piece + 1]];
            int edgeCount = last - first - (arrays.pieceRing[piece + 1] - arrays.pieceRing[piece]);
            int minY = arrays.pieceBox[4 * piece + 1];
            long span = (long) arrays.pieceBox[4 * piece + 3] - minY + 1;
            long wanted = Math.min(Math.min(MAX_BANDS, edgeCount / EDGES_PER_BAND), span);
            if (edgeCount < MIN_EDGES || last - first - 2 > MAX_OFFSET || wanted < 2) {
                return;
            }
            long height = (span + wanted - 1) / wanted;
            int count = (int) ((span + height - 1) / height);
            bandHeight[piece] = (int) height;
            int[] sizes = new int[count];
            forEachEdge(piece, minY, height, (band, edge) -> sizes[band]++);
            bandStart = ensure(bandStart, bands + count + 1);
            int[] fill = new int[count];
            for (int band = 0; band < count; band++) {
                bandStart[bands + band] = entries;
                fill[band] = entries;
                entries += sizes[band];
            }
            bandStart[bands + count] = entries;
            if (entries > edges.length) {
                edges = Arrays.copyOf(edges, Math.max(entries, 2 * edges.length));
            }
            forEachEdge(piece, minY, height, (band, edge) -> edges[fill[band]++] = (byte) (edge - first));
            bands += count + 1;
        }

        private void forEachEdge(int piece, int minY, long height, EdgeBand action) {
            for (int r = arrays.pieceRing[piece]; r < arrays.pieceRing[piece + 1]; r++) {
                for (int i = arrays.ringStart[r]; i < arrays.ringStart[r + 1] - 1; i++) {
                    int ay = yOf(arrays.xy[i]);
                    int by = yOf(arrays.xy[i + 1]);
                    int low = (int) (((long) Math.min(ay, by) - minY) / height);
                    int high = (int) (((long) Math.max(ay, by) - minY) / height);
                    for (int band = low; band <= high; band++) {
                        action.accept(band, i);
                    }
                }
            }
        }

        private static int[] ensure(int[] values, int size) {
            return size <= values.length ? values : Arrays.copyOf(values, Math.max(size, 2 * values.length));
        }

        private LatitudeBands build() {
            pieceBand[pieceBand.length - 1] = bands;
            return new LatitudeBands(arrays, pieceBand, bandHeight, Arrays.copyOf(bandStart, bands),
                    Arrays.copyOf(edges, entries));
        }
    }

    @FunctionalInterface
    private interface EdgeBand {
        void accept(int band, int edge);
    }
}
