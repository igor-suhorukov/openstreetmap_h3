package com.github.isuhorukov.osm.pgsnapshot.admin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class PackedTree {

    private static final int NODE_SIZE = 16;
    private static final int HILBERT_MAX = 65535;

    private final int numItems;
    private final int[] boxes;
    private final int[] indices;
    private final int[] levelBounds;

    PackedTree(int[] itemBoxes) {
        int count = itemBoxes.length / 4;
        numItems = count;
        List<Integer> bounds = new ArrayList<>();
        int levelCount = count;
        int numNodes = count;
        bounds.add(numNodes);
        while (levelCount > 1 || bounds.size() == 1) {
            levelCount = Math.max(1, (levelCount + NODE_SIZE - 1) / NODE_SIZE);
            numNodes += levelCount;
            bounds.add(numNodes);
        }
        levelBounds = bounds.stream().mapToInt(Integer::intValue).toArray();
        boxes = new int[numNodes * 4];
        indices = new int[numNodes];
        if (count == 0) {
            return;
        }
        sortByHilbert(itemBoxes, count);
        buildLevels();
    }

    private void sortByHilbert(int[] itemBoxes, int count) {
        int minX = Integer.MAX_VALUE;
        int minY = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int maxY = Integer.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            minX = Math.min(minX, itemBoxes[4 * i]);
            minY = Math.min(minY, itemBoxes[4 * i + 1]);
            maxX = Math.max(maxX, itemBoxes[4 * i + 2]);
            maxY = Math.max(maxY, itemBoxes[4 * i + 3]);
        }
        double width = Math.max(1, (double) maxX - minX);
        double height = Math.max(1, (double) maxY - minY);
        long[] keys = new long[count];
        for (int i = 0; i < count; i++) {
            double centreX = ((double) itemBoxes[4 * i] + itemBoxes[4 * i + 2]) / 2 - minX;
            double centreY = ((double) itemBoxes[4 * i + 1] + itemBoxes[4 * i + 3]) / 2 - minY;
            int hilbertX = (int) Math.min(HILBERT_MAX, Math.floor(HILBERT_MAX * centreX / width));
            int hilbertY = (int) Math.min(HILBERT_MAX, Math.floor(HILBERT_MAX * centreY / height));
            keys[i] = ((long) (hilbert(hilbertX, hilbertY) ^ Integer.MIN_VALUE) << 32) | i;
        }
        Arrays.sort(keys);
        for (int position = 0; position < count; position++) {
            int item = (int) (keys[position] & 0xffffffffL);
            System.arraycopy(itemBoxes, 4 * item, boxes, 4 * position, 4);
            indices[position] = item;
        }
    }

    private void buildLevels() {
        int position = 0;
        for (int level = 0; level < levelBounds.length - 1; level++) {
            int end = levelBounds[level];
            int parent = end;
            while (position < end) {
                int first = position;
                int nodeMinX = Integer.MAX_VALUE;
                int nodeMinY = Integer.MAX_VALUE;
                int nodeMaxX = Integer.MIN_VALUE;
                int nodeMaxY = Integer.MIN_VALUE;
                for (int j = 0; j < NODE_SIZE && position < end; j++, position++) {
                    nodeMinX = Math.min(nodeMinX, boxes[4 * position]);
                    nodeMinY = Math.min(nodeMinY, boxes[4 * position + 1]);
                    nodeMaxX = Math.max(nodeMaxX, boxes[4 * position + 2]);
                    nodeMaxY = Math.max(nodeMaxY, boxes[4 * position + 3]);
                }
                boxes[4 * parent] = nodeMinX;
                boxes[4 * parent + 1] = nodeMinY;
                boxes[4 * parent + 2] = nodeMaxX;
                boxes[4 * parent + 3] = nodeMaxY;
                indices[parent] = first;
                parent++;
            }
        }
    }

    private int upperBound(int nodeIndex) {
        for (int bound : levelBounds) {
            if (bound > nodeIndex) {
                return bound;
            }
        }
        return levelBounds[levelBounds.length - 1];
    }

    int search(int minX, int minY, int maxX, int maxY, Scratch scratch) {
        if (numItems == 0) {
            return 0;
        }
        int count = 0;
        int stackSize = 0;
        int nodeIndex = indices.length - 1;
        while (true) {
            int end = Math.min(nodeIndex + NODE_SIZE, upperBound(nodeIndex));
            for (int position = nodeIndex; position < end; position++) {
                int box = 4 * position;
                if (maxX < boxes[box] || maxY < boxes[box + 1] || minX > boxes[box + 2] || minY > boxes[box + 3]) {
                    continue;
                }
                if (nodeIndex < numItems) {
                    scratch.candidates = push(scratch.candidates, count++, indices[position]);
                } else {
                    scratch.stack = push(scratch.stack, stackSize++, indices[position]);
                }
            }
            if (stackSize == 0) {
                return count;
            }
            nodeIndex = scratch.stack[--stackSize];
        }
    }

    static int[] push(int[] array, int index, int value) {
        int[] result = index < array.length ? array : Arrays.copyOf(array, array.length * 2);
        result[index] = value;
        return result;
    }

    static int hilbert(int x, int y) {
        int a = x ^ y;
        int b = 0xFFFF ^ a;
        int c = 0xFFFF ^ (x | y);
        int d = x & (y ^ 0xFFFF);
        int nextA = a | (b >> 1);
        int nextB = (a >> 1) ^ a;
        int nextC = ((c >> 1) ^ (b & (d >> 1))) ^ c;
        int nextD = ((a & (c >> 1)) ^ (d >> 1)) ^ d;
        a = nextA;
        b = nextB;
        c = nextC;
        d = nextD;
        nextA = (a & (a >> 2)) ^ (b & (b >> 2));
        nextB = (a & (b >> 2)) ^ (b & ((a ^ b) >> 2));
        nextC ^= (a & (c >> 2)) ^ (b & (d >> 2));
        nextD ^= (b & (c >> 2)) ^ ((a ^ b) & (d >> 2));
        a = nextA;
        b = nextB;
        c = nextC;
        d = nextD;
        nextA = (a & (a >> 4)) ^ (b & (b >> 4));
        nextB = (a & (b >> 4)) ^ (b & ((a ^ b) >> 4));
        nextC ^= (a & (c >> 4)) ^ (b & (d >> 4));
        nextD ^= (b & (c >> 4)) ^ ((a ^ b) & (d >> 4));
        a = nextA;
        b = nextB;
        c = nextC;
        d = nextD;
        nextC ^= (a & (c >> 8)) ^ (b & (d >> 8));
        nextD ^= (b & (c >> 8)) ^ ((a ^ b) & (d >> 8));
        a = nextC ^ (nextC >> 1);
        b = nextD ^ (nextD >> 1);
        int i0 = x ^ y;
        int i1 = b | (0xFFFF ^ (i0 | a));
        i0 = (i0 | (i0 << 8)) & 0x00FF00FF;
        i0 = (i0 | (i0 << 4)) & 0x0F0F0F0F;
        i0 = (i0 | (i0 << 2)) & 0x33333333;
        i0 = (i0 | (i0 << 1)) & 0x55555555;
        i1 = (i1 | (i1 << 8)) & 0x00FF00FF;
        i1 = (i1 | (i1 << 4)) & 0x0F0F0F0F;
        i1 = (i1 | (i1 << 2)) & 0x33333333;
        i1 = (i1 | (i1 << 1)) & 0x55555555;
        return (i1 << 1) | i0;
    }

    static final class Scratch {
        int[] candidates = new int[16];
        int[] stack = new int[16];
        int[] hits = new int[4];
    }
}
