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
}
