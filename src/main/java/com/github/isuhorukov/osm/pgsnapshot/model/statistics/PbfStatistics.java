package com.github.isuhorukov.osm.pgsnapshot.model.statistics;

import java.io.Serializable;
import java.util.List;

public class PbfStatistics implements Serializable {
    List<BlockStat> blockStatistics;
    long multipolygonCount;
    long dataProcessingTime;
    long pbfSplitTime;
    long addLocationsToWaysTime;
    long multipolygonExportTime;
    long splitMultipolygonByPartsTime;
    long totalTime;

    public PbfStatistics(List<BlockStat> blockStatistics) {
        this.blockStatistics = blockStatistics;
    }

    public long getMultipolygonCount() {
        return multipolygonCount;
    }

    public void setMultipolygonCount(long multipolygonCount) {
        this.multipolygonCount = multipolygonCount;
    }

    public void setDataProcessingTime(long dataProcessingTime) {
        this.dataProcessingTime = dataProcessingTime;
    }

    public void setPbfSplitTime(long pbfSplitTime) {
        this.pbfSplitTime = pbfSplitTime;
    }

    public void setAddLocationsToWaysTime(long addLocationsToWaysTime) {
        this.addLocationsToWaysTime = addLocationsToWaysTime;
    }

    public void setMultipolygonExportTime(long multipolygonExportTime) {
        this.multipolygonExportTime = multipolygonExportTime;
    }

    public void setSplitMultipolygonByPartsTime(long splitMultipolygonByPartsTime) {
        this.splitMultipolygonByPartsTime = splitMultipolygonByPartsTime;
    }

    public List<BlockStat> getBlockStatistics() {
        return blockStatistics;
    }

    public long getDataProcessingTime() {
        return dataProcessingTime;
    }

    public long getPbfSplitTime() {
        return pbfSplitTime;
    }

    public long getAddLocationsToWaysTime() {
        return addLocationsToWaysTime;
    }

    public long getMultipolygonExportTime() {
        return multipolygonExportTime;
    }

    public long getSplitMultipolygonByPartsTime() {
        return splitMultipolygonByPartsTime;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(long totalTime) {
        this.totalTime = totalTime;
    }
}
