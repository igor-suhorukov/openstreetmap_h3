package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;

final class PartitionTable {

    private final String entityName;
    private final String name;
    private final String createSql;
    private final String copySql;
    private final String analyzeSql;
    private final String attachSql;

    PartitionTable(String entityName, String copyColumns, Partition partition, boolean storeColumnar) {
        this.entityName = entityName;
        this.name = PartitionSplitter.partitionTableName(entityName, partition.getId());
        this.createSql = PartitionSplitter.createPartitionTableSql(entityName, partition.getId(), storeColumnar);
        this.copySql = PartitionSplitter.copyPartitionSql(entityName, partition.getId(), copyColumns, "STDIN", storeColumnar);
        this.analyzeSql = PartitionSplitter.analyzePartitionSql(entityName, partition.getId());
        this.attachSql = PartitionSplitter.attachPartitionSql(entityName, partition);
    }

    String getEntityName() {
        return entityName;
    }

    String getName() {
        return name;
    }

    String getCreateSql() {
        return createSql;
    }

    String getCopySql() {
        return copySql;
    }

    String getAnalyzeSql() {
        return analyzeSql;
    }

    String getAttachSql() {
        return attachSql;
    }
}
