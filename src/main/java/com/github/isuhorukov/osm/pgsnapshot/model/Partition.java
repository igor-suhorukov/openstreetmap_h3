package com.github.isuhorukov.osm.pgsnapshot.model;

import java.util.List;

public class Partition {
    int id;
    short minRange;
    short maxRange;
    long serializedSize;
    List<Short> h33RegionsInside;

    public Partition(short minRange, short maxRange) {
        this.minRange = minRange;
        this.maxRange = maxRange;
    }

    public Partition(int id, short minRange, short maxRange, long serializedSize) {
        this.id = id;
        this.minRange = minRange;
        this.maxRange = maxRange;
        this.serializedSize = serializedSize;
    }

    public int getId() {
        return id;
    }

    public short getMinRange() {
        return minRange;
    }

    public short getMaxRange() {
        return maxRange;
    }

    public long getSerializedSize() {
        return serializedSize;
    }

    public List<Short> getH33RegionsInside() {
        return h33RegionsInside;
    }

    public void setSerializedSize(long serializedSize) {
        this.serializedSize = serializedSize;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setH33RegionsInside(List<Short> h33RegionsInside) {
        this.h33RegionsInside = h33RegionsInside;
    }

}
