package com.github.isuhorukov.osm.pgsnapshot.model.statistics;

import java.io.Serializable;
import java.util.Map;

public class BlockStat implements Serializable {
    long id;
    long nodeCount;
    long wayCount;
    long relationCount;
    long relationMembersCount;
    long multipolygonCount;
    Map<Short, Stat> nodeStat;
    Map<Short, Stat> wayStat;
    long processingTime;
    long waitingForSaveTime;
    long saveTime;
    long threadTime;
    long threadStart;

    public BlockStat(long id) {
        this.id = id;
    }

    public void setNodeCount(long nodeCount) {
        this.nodeCount = nodeCount;
    }

    public void setWayCount(long wayCount) {
        this.wayCount = wayCount;
    }

    public void setRelationCount(long relationCount) {
        this.relationCount = relationCount;
    }

    public void setRelationMembersCount(long relationMembersCount) {
        this.relationMembersCount = relationMembersCount;
    }

    public void setProcessingTime(long processingTime) {
        this.processingTime = processingTime;
    }

    public void setNodeStat(Map<Short, Stat> nodeStat) {
        this.nodeStat = nodeStat;
    }

    public void setWayStat(Map<Short, Stat> wayStat) {
        this.wayStat = wayStat;
    }

    public void setThreadTime(long threadTime) {
        this.threadTime = threadTime;
    }

    public long getProcessingTime() {
        return processingTime;
    }

    public long getThreadTime() {
        return threadTime;
    }

    public long getNodeCount() {
        return nodeCount;
    }

    public long getWayCount() {
        return wayCount;
    }

    public long getRelationCount() {
        return relationCount;
    }

    public long getRelationMembersCount() {
        return relationMembersCount;
    }

    public long getId() {
        return id;
    }

    public Map<Short, Stat> getNodeStat() {
        return nodeStat;
    }

    public Map<Short, Stat> getWayStat() {
        return wayStat;
    }

    public long getMultipolygonCount() {
        return multipolygonCount;
    }

    public void setMultipolygonCount(long multipolygonCount) {
        this.multipolygonCount = multipolygonCount;
    }

    public long getSaveTime() {
        return saveTime;
    }

    public void setSaveTime(long saveTime) {
        this.saveTime = saveTime;
    }

    public long getWaitingForSaveTime() {
        return waitingForSaveTime;
    }

    public void setWaitingForSaveTime(long waitingForSaveTime) {
        this.waitingForSaveTime = waitingForSaveTime;
    }

    public void setThreadStart(long threadStart) {
        this.threadStart = threadStart;
    }

    public long getThreadStart() {
        return threadStart;
    }
}
