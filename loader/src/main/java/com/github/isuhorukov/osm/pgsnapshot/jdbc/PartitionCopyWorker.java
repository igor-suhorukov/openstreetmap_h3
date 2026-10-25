package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

final class PartitionCopyWorker extends CopyWorker {

    private static final List<String> ATTACH_ORDER = List.of(PartitionSplitter.NODES, PartitionSplitter.WAYS);

    private final Map<String, PartitionTable> tables = new HashMap<>();
    private final List<PartitionTable> createdTables = new ArrayList<>();
    private final boolean createEmptyTables;

    PartitionCopyWorker(String name, Connection connection, int queueCapacity, long pendingBytesLimit,
                        Consumer<Throwable> failureListener, Collection<PartitionTable> tables) throws SQLException {
        this(name, connection, queueCapacity, pendingBytesLimit, failureListener, tables, false);
    }

    PartitionCopyWorker(String name, Connection connection, int queueCapacity, long pendingBytesLimit,
                        Consumer<Throwable> failureListener, Collection<PartitionTable> tables,
                        boolean createEmptyTables) throws SQLException {
        super(name, connection, queueCapacity, pendingBytesLimit, failureListener);
        for (PartitionTable table : tables) {
            this.tables.put(table.getName(), table);
        }
        this.createEmptyTables = createEmptyTables;
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
        create(table);
        return table.getCopySql();
    }

    private void create(PartitionTable table) throws SQLException {
        if (!createdTables.contains(table)) {
            PsqlRunner.executeSql(getConnection(), table.getCreateSql());
            createdTables.add(table);
        }
    }

    @Override
    protected void finish() throws SQLException {
        if (createEmptyTables) {
            for (PartitionTable table : new TreeMap<>(tables).values()) {
                create(table);
            }
        }
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
