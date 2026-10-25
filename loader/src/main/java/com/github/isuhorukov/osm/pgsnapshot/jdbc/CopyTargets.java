package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.output.BlockType;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;

import java.util.HashMap;
import java.util.Map;

final class CopyTargets {

    private static final int H33_OFFSET = -Short.MIN_VALUE;
    private static final int H33_VALUES = 1 << 16;

    private final String[] nodeTables = new String[H33_VALUES];
    private final String[] wayTables = new String[H33_VALUES];
    private final Map<String, CopyWorker> workers = new HashMap<>();

    void addPartition(Partition partition, CopyWorker worker) {
        String nodeTable = PartitionSplitter.partitionTableName(PartitionSplitter.NODES, partition.getId());
        String wayTable = PartitionSplitter.partitionTableName(PartitionSplitter.WAYS, partition.getId());
        for (int h33 = partition.getMinRange(); h33 < partition.getMaxRange(); h33++) {
            nodeTables[h33 + H33_OFFSET] = nodeTable;
            wayTables[h33 + H33_OFFSET] = wayTable;
        }
        workers.put(nodeTable, worker);
        workers.put(wayTable, worker);
    }

    void addInput(String inputPath, CopyWorker worker) {
        workers.put(inputPath, worker);
    }

    String target(BlockType type, short h33) {
        String target;
        if (type == BlockType.NODES) {
            target = nodeTables[h33 + H33_OFFSET];
        } else if (type == BlockType.WAYS && h33 != Short.MAX_VALUE) {
            target = wayTables[h33 + H33_OFFSET];
        } else {
            target = inputPath(type, h33);
        }
        if (target == null) {
            throw new IllegalStateException("No partition for " + type + " with h3_3 " + h33);
        }
        return target;
    }

    CopyWorker worker(String target) {
        CopyWorker worker = workers.get(target);
        if (worker == null) {
            throw new IllegalStateException("No COPY worker for " + target);
        }
        return worker;
    }

    static String inputPath(BlockType type, short h33) {
        return String.format("%s%s/%05d.tsv", InputFiles.INPUT_DIRECTORY, type.getDirectoryName(), h33);
    }
}
