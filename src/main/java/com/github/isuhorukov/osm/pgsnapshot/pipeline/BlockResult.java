package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockResult {

    private final Map<Short, StringBuilder> csvResultPerH33 = new HashMap<>();
    private final ArrayList<ArrowNodeOrWay> arrowNodeOrWays = new ArrayList<>();
    private final ArrayList<ArrowRelation> arrowRelations = new ArrayList<>();
    private final BlockStat blockStat;
    private long nodeCount;
    private long wayCount;
    private long relationCount;

    public BlockResult(long blockNumber) {
        this.blockStat = new BlockStat(blockNumber);
    }

    public Map<Short, StringBuilder> getCsvResultPerH33() { return csvResultPerH33; }
    public List<ArrowNodeOrWay> getArrowNodeOrWays() { return arrowNodeOrWays; }
    public List<ArrowRelation> getArrowRelations()   { return arrowRelations; }
    public BlockStat getBlockStat()                       { return blockStat; }
    public long getNodeCount()                            { return nodeCount; }
    public long getWayCount()                             { return wayCount; }
    public long getRelationCount()                        { return relationCount; }

    public void setNodeCount(long nodeCount)         { this.nodeCount = nodeCount; }
    public void setWayCount(long wayCount)           { this.wayCount = wayCount; }
    public void setRelationCount(long relationCount) { this.relationCount = relationCount; }
}
