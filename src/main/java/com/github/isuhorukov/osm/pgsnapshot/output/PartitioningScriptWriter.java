package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.table.StatType;
import com.github.isuhorukov.osm.pgsnapshot.model.table.TableStat;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class PartitioningScriptWriter {

    private static final Logger log = LoggerFactory.getLogger(PartitioningScriptWriter.class);

    private PartitioningScriptWriter() {
    }

    public static void savePartitioningScripts(File resultDirectory, int scriptCount,
                                               double thresholdPercentFromMaxPartition,
                                               List<BlockStat> blockStatistics, boolean storeColumnar) {
        List<TableStat> tableStat = blockStatistics.stream().map(statItem -> {
            if (statItem.getRelationCount() > 0) {
                return null;
            }
            if (statItem.getNodeCount() > 0 && statItem.getWayCount() > 0) {
                throw new IllegalStateException("Invalid block - mixed content nodes and ways");
            }
            if (statItem.getNodeCount() > 0) {
                if (statItem.getNodeStat() == null) {
                    return null;
                }
                return statItem.getNodeStat().values().stream().map(stat -> new TableStat(StatType.N, statItem.getId(), stat)).collect(toList());
            }
            if (statItem.getWayCount() > 0) {
                if (statItem.getWayStat() == null) {
                    return null;
                }
                return statItem.getWayStat().values().stream().map(stat -> new TableStat(StatType.W, statItem.getId(), stat)).collect(toList());
            }
            throw new IllegalStateException("Block has no nodes, ways, or relations");
        }).filter(Objects::nonNull).flatMap(Collection::stream).collect(toList());

        Map<Short, Long> waysSizeStat = tableStat.stream().filter(ts -> ts.getType() == StatType.W)
                .collect(Collectors.groupingBy(TableStat::getH33, TreeMap::new, Collectors.summingLong(TableStat::getSize)));
        Map<Short, Long> nodesSizeStat = tableStat.stream().filter(ts -> ts.getType() == StatType.N)
                .collect(Collectors.groupingBy(TableStat::getH33, TreeMap::new, Collectors.summingLong(TableStat::getSize)));

        List<Partition> partitionsWay = PartitionSplitter.distributeH33ByPartitionsForWays(
                waysSizeStat.entrySet().stream().filter(entry -> !entry.getKey().equals(Short.MAX_VALUE))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                thresholdPercentFromMaxPartition);
        List<Partition> partitionsNode = PartitionSplitter.distributeH33ByPartitionsForNodes(
                partitionsWay,
                nodesSizeStat.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        long minWayPartitionSizeMb = partitionsWay.stream().map(Partition::getSerializedSize)
                .min(Comparator.comparingLong(Long::longValue)).orElseThrow() / 1000000;
        double avgWayPartitionSizeMb = partitionsWay.stream().map(Partition::getSerializedSize)
                .collect(Collectors.averagingLong(Long::longValue)) / 1000000;
        log.info("{} {} {}", partitionsWay.size(), minWayPartitionSizeMb, avgWayPartitionSizeMb);
        PartitionSplitter.createNodesScript(resultDirectory, scriptCount, partitionsNode, storeColumnar);
        PartitionSplitter.createWaysScript(resultDirectory, scriptCount, partitionsWay, storeColumnar);
        PartitionSplitter.createMultipolygonScript(resultDirectory, partitionsWay, storeColumnar);
    }
}
