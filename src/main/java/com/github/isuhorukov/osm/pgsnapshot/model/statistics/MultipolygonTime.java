package com.github.isuhorukov.osm.pgsnapshot.model.statistics;

public class MultipolygonTime {
    long multipolygonExportTime;
    long splitMultipolygonByPartsTime;

    public MultipolygonTime() {
    }

    public long getMultipolygonExportTime() {
        return multipolygonExportTime;
    }

    public void setMultipolygonExportTime(long multipolygonExportTime) {
        this.multipolygonExportTime = multipolygonExportTime;
    }

    public long getSplitMultipolygonByPartsTime() {
        return splitMultipolygonByPartsTime;
    }

    public void setSplitMultipolygonByPartsTime(long splitMultipolygonByPartsTime) {
        this.splitMultipolygonByPartsTime = splitMultipolygonByPartsTime;
    }
}
