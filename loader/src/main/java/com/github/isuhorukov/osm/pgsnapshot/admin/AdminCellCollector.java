package com.github.isuhorukov.osm.pgsnapshot.admin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class AdminCellCollector {

    private final Map<Thread, LongSet> sets = new ConcurrentHashMap<>();

    void add(int unit, short cell) {
        Thread thread = Thread.currentThread();
        LongSet set = sets.get(thread);
        if (set == null) {
            set = sets.computeIfAbsent(thread, key -> new LongSet());
        }
        set.add(key(unit, cell));
    }

    static long key(int unit, short cell) {
        return ((long) unit << 16) | (cell & 0xFFFF);
    }

    void assignCells(List<AdminUnit> units) {
        long[] leafPairs = new long[0];
        for (LongSet set : sets.values()) {
            leafPairs = concat(leafPairs, set.values());
        }
        long[] pairs = unique(leafPairs);
        long[] all = new long[0];
        int count = 0;
        for (long pair : pairs) {
            int unit = (int) (pair >>> 16);
            short cell = (short) pair;
            int[] path = units.get(unit).getPath();
            if (all.length < count + path.length) {
                all = Arrays.copyOf(all, Math.max(all.length * 2, count + path.length));
            }
            for (int ancestor : path) {
                all[count++] = key(ancestor, cell);
            }
        }
        long[] cells = unique(Arrays.copyOf(all, count));
        int start = 0;
        while (start < cells.length) {
            int unit = (int) (cells[start] >>> 16);
            int end = start;
            while (end < cells.length && (int) (cells[end] >>> 16) == unit) {
                end++;
            }
            short[] unitCells = new short[end - start];
            for (int i = start; i < end; i++) {
                unitCells[i - start] = (short) cells[i];
            }
            Arrays.sort(unitCells);
            units.get(unit).setCells(unitCells);
            start = end;
        }
    }

    private static long[] concat(long[] first, long[] second) {
        long[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private static long[] unique(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        int count = 0;
        for (int i = 0; i < sorted.length; i++) {
            if (i == 0 || sorted[i] != sorted[count - 1]) {
                sorted[count++] = sorted[i];
            }
        }
        return Arrays.copyOf(sorted, count);
    }

    static final class LongSet {
        private static final long EMPTY = -1;
        private long[] table = newTable(64);
        private int size;

        private static long[] newTable(int capacity) {
            long[] table = new long[capacity];
            Arrays.fill(table, EMPTY);
            return table;
        }

        void add(long value) {
            int mask = table.length - 1;
            int slot = (int) mix(value) & mask;
            while (table[slot] != EMPTY) {
                if (table[slot] == value) {
                    return;
                }
                slot = (slot + 1) & mask;
            }
            table[slot] = value;
            if (++size * 2 > table.length) {
                grow();
            }
        }

        private void grow() {
            long[] old = table;
            table = newTable(old.length * 2);
            size = 0;
            for (long value : old) {
                if (value != EMPTY) {
                    add(value);
                }
            }
        }

        long[] values() {
            long[] values = new long[size];
            int count = 0;
            for (long value : table) {
                if (value != EMPTY) {
                    values[count++] = value;
                }
            }
            return values;
        }

        private static long mix(long value) {
            long h = value * 0x9E3779B97F4A7C15L;
            return h ^ (h >>> 32);
        }
    }
}
