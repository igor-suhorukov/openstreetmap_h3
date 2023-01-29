package com.github.isuhorukov.osm.pgsnapshot.model.statistics;

import java.io.Serializable;

public class Stat implements Serializable {
    short h33;
    long count;
    long size;
    long minId=Long.MAX_VALUE;
    long maxId=Long.MIN_VALUE;
    long lastModified;

    public Stat(short h33) {
        this.h33 = h33;
    }

    public Stat(short h33, long count, long size, short minId, short maxId, long lastModified) {
        this.h33 = h33;
        this.count = count;
        this.size = size;
        this.minId = minId;
        this.maxId = maxId;
        this.lastModified = lastModified;
    }

    public short getH33() {
        return h33;
    }

    public long getCount() {
        return count;
    }

    public long getSize() {
        return size;
    }

    public long getMinId() {
        return minId;
    }

    public long getMaxId() {
        return maxId;
    }

    public long getLastModified() {
        return lastModified;
    }

    public void incrementCount() {
        this.count++;
    }

    public void updateIdStat(long id){
        this.maxId = Math.max(maxId, id);
        this.minId = Math.min(minId, id);
    }

    public void updateLastModified(long lastModified){
        this.lastModified=Math.max(this.lastModified, lastModified);
    }
    public void incrementSize(int itemSize) {
        this.size+=itemSize;
    }
}
