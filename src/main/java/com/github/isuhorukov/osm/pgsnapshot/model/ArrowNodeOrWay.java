package com.github.isuhorukov.osm.pgsnapshot.model;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collection;
import java.util.Map;

public class ArrowNodeOrWay {
    private final long id;
    private final short h33;
    private final int h38;
    private final double latitude;
    private final double longitude;
    private final Map<String, String> tags;
    private final long[] pointIdxs;
    private final short h33Center;
    private final boolean closed;
    private final float scaleDim;
    private final double bboxMinX;
    private final double bboxMaxX;
    private final double bboxMinY;
    private final double bboxMaxY;
    private final byte[] lineStringWkb;
    private final int[] h38Indexes;
    private final byte[] bboxWkb;

    public ArrowNodeOrWay(long id, short h33, int h38, double latitude, double longitude, Collection<Tag> entityTags) {
        this(id, h33, h38, latitude, longitude, TagsUtil.tagsToMap(entityTags),
                null, (short) 0, false, 0f, 0, 0, 0, 0, null, null, null);
    }

    private ArrowNodeOrWay(long id, short h33, int h38, double latitude, double longitude,
                           Map<String, String> tags, long[] pointIdxs, short h33Center, boolean closed,
                           float scaleDim, double bboxMinX, double bboxMaxX, double bboxMinY, double bboxMaxY,
                           byte[] lineStringWkb, int[] h38Indexes, byte[] bboxWkb) {
        this.id = id;
        this.h33 = h33;
        this.h38 = h38;
        this.latitude = latitude;
        this.longitude = longitude;
        this.tags = tags;
        this.pointIdxs = pointIdxs;
        this.h33Center = h33Center;
        this.closed = closed;
        this.scaleDim = scaleDim;
        this.bboxMinX = bboxMinX;
        this.bboxMaxX = bboxMaxX;
        this.bboxMinY = bboxMinY;
        this.bboxMaxY = bboxMaxY;
        this.lineStringWkb = lineStringWkb;
        this.h38Indexes = h38Indexes;
        this.bboxWkb = bboxWkb;
    }

    public long getId() {
        return id;
    }

    public short getH33() {
        return h33;
    }

    public int getH38() {
        return h38;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public long[] getPointIdxs() {
        return pointIdxs;
    }

    public short getH33Center() {
        return h33Center;
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isBuilding() {
        return tags != null && !tags.isEmpty() && tags.containsKey("building");
    }

    public boolean isHighway() {
        return tags != null && !tags.isEmpty() && tags.containsKey("highway");
    }

    public float getScaleDim() {
        return scaleDim;
    }

    public double getBboxMinX() {
        return bboxMinX;
    }

    public double getBboxMaxX() {
        return bboxMaxX;
    }

    public double getBboxMinY() {
        return bboxMinY;
    }

    public double getBboxMaxY() {
        return bboxMaxY;
    }

    public byte[] getLineStringWkb() {
        return lineStringWkb;
    }

    public int[] getH38Indexes() {
        return h38Indexes;
    }

    public byte[] getBboxWkb() {
        return bboxWkb;
    }

    public static class Builder {
        private final long id;
        private final short h33;
        private final int h38;
        private final double latitude;
        private final double longitude;
        private final java.util.Collection<org.openstreetmap.osmosis.core.domain.v0_6.Tag> entityTags;
        private long[] pointIdxs;
        private short h33Center;
        private boolean closed;
        private float scaleDim;
        private double bboxMinX;
        private double bboxMaxX;
        private double bboxMinY;
        private double bboxMaxY;
        private byte[] lineStringWkb;
        private int[] h38Indexes;
        private byte[] bboxWkb;

        public Builder(long id, short h33, int h38, double latitude, double longitude,
                       java.util.Collection<org.openstreetmap.osmosis.core.domain.v0_6.Tag> entityTags) {
            this.id = id;
            this.h33 = h33;
            this.h38 = h38;
            this.latitude = latitude;
            this.longitude = longitude;
            this.entityTags = entityTags;
        }

        public Builder pointIdxs(long[] v)   { this.pointIdxs = v;      return this; }
        public Builder h33Center(short v)     { this.h33Center = v;      return this; }
        public Builder closed(boolean v)      { this.closed = v;         return this; }
        public Builder scaleDim(float v)      { this.scaleDim = v;       return this; }
        public Builder bboxMinX(double v)     { this.bboxMinX = v;       return this; }
        public Builder bboxMaxX(double v)     { this.bboxMaxX = v;       return this; }
        public Builder bboxMinY(double v)     { this.bboxMinY = v;       return this; }
        public Builder bboxMaxY(double v)     { this.bboxMaxY = v;       return this; }
        public Builder lineStringWkb(byte[] v){ this.lineStringWkb = v;  return this; }
        public Builder h38Indexes(int[] v)    { this.h38Indexes = v;     return this; }
        public Builder bboxWkb(byte[] v)      { this.bboxWkb = v;        return this; }

        public ArrowNodeOrWay build() {
            return new ArrowNodeOrWay(id, h33, h38, latitude, longitude, TagsUtil.tagsToMap(entityTags),
                    pointIdxs, h33Center, closed, scaleDim, bboxMinX, bboxMaxX, bboxMinY, bboxMaxY,
                    lineStringWkb, h38Indexes, bboxWkb);
        }
    }
}
