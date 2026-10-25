package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

final class PartitionCopyWorker extends CopyWorker {

    private static final List<String> ATTACH_ORDER = List.of(PartitionSplitter.NODES, PartitionSplitter.WAYS);

    private final Map<String, PartitionTable> tables = new HashMap<>();
    private final List<PartitionTable> createdTables = new ArrayList<>();

    PartitionCopyWorker(String name, Connection connection, int queueCapacity, long pendingBytesLimit,
                        Consumer<Throwable> failureListener, Collection<PartitionTable> tables) throws SQLException {
        super(name, connection, queueCapacity, pendingBytesLimit, failureListener);
        for (PartitionTable table : tables) {
            this.tables.put(table.getName(), table);
        }
    }

    @Override
    protected void begin() throws SQLException {
        PsqlRunner.executeSql(getConnection(), "BEGIN");
    }

    @Override
    protected String copySql(String target) throws SQLException {
        PartitionTable table = tables.get(target);
        if (table == null) {
            throw new IllegalArgumentException("Unknown partition table " + target);
        }
        if (!createdTables.contains(table)) {
            PsqlRunner.executeSql(getConnection(), table.getCreateSql());
            createdTables.add(table);
        }
        return table.getCopySql();
    }

    @Override
    protected void finish() throws SQLException {
        for (PartitionTable table : createdTables) {
            PsqlRunner.executeSql(getConnection(), table.getAnalyzeSql());
        }
        for (String entityName : ATTACH_ORDER) {
            for (PartitionTable table : createdTables) {
                if (entityName.equals(table.getEntityName())) {
                    PsqlRunner.executeSql(getConnection(), table.getAttachSql());
                }
            }
        }
        PsqlRunner.executeSql(getConnection(), "COMMIT");
    }
}
