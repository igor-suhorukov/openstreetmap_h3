package com.github.isuhorukov.osm.pgsnapshot.model;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collection;
import java.util.Map;

public class ArrowNodeOrWay {
    long id;
    short h33;
    int h38;
    double latitude;
    double longitude;
    Map<String, String> tags;
    long[] pointIdxs;
    short h33Center;
    boolean closed;
    float scaleDim;
    double bboxMinX;
    double bboxMaxX;
    double bboxMinY;
    double bboxMaxY;
    byte[] lineStringWkb;
    int[] h38Indexes;
    byte[] bboxWkb;

    public ArrowNodeOrWay(long id, short h33, int h38, double latitude, double longitude, Collection<Tag> entityTags) {
        this.id = id;
        this.h33 = h33;
        this.h38 = h38;
        this.latitude = latitude;
        this.longitude = longitude;
        this.tags = TagsUtil.tagsToMap(entityTags);
    }

    public void setPointIdxs(long[] pointIdxs) {
        this.pointIdxs = pointIdxs;
    }

    public void setH33Center(short h33Center) {
        this.h33Center = h33Center;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    public void setScaleDim(float scaleDim) {
        this.scaleDim = scaleDim;
    }

    public void setBboxMinX(double bboxMinX) {
        this.bboxMinX = bboxMinX;
    }

    public void setBboxMaxX(double bboxMaxX) {
        this.bboxMaxX = bboxMaxX;
    }

    public void setBboxMinY(double bboxMinY) {
        this.bboxMinY = bboxMinY;
    }

    public void setBboxMaxY(double bboxMaxY) {
        this.bboxMaxY = bboxMaxY;
    }

    public void setLineStringWkb(byte[] lineStringWkb) {
        this.lineStringWkb = lineStringWkb;
    }

    public void setH38Indexes(int[] h38Indexes) {
        this.h38Indexes = h38Indexes;
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

    public void setBboxWkb(byte[] bboxWkb) {
        this.bboxWkb = bboxWkb;
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
            ArrowNodeOrWay obj = new ArrowNodeOrWay(id, h33, h38, latitude, longitude, entityTags);
            obj.pointIdxs = pointIdxs;
            obj.h33Center = h33Center;
            obj.closed = closed;
            obj.scaleDim = scaleDim;
            obj.bboxMinX = bboxMinX;
            obj.bboxMaxX = bboxMaxX;
            obj.bboxMinY = bboxMinY;
            obj.bboxMaxY = bboxMaxY;
            obj.lineStringWkb = lineStringWkb;
            obj.h38Indexes = h38Indexes;
            obj.bboxWkb = bboxWkb;
            return obj;
        }
    }
}
