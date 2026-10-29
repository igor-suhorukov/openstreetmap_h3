package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.ExternalProcessing;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.output.ImportState;
import com.github.isuhorukov.osm.pgsnapshot.output.OrderedBlockWriter;
import com.github.isuhorukov.osm.pgsnapshot.output.ResourceCopier;
import com.github.isuhorukov.osm.pgsnapshot.pbfextractor.ReplicationHeader;
import com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonTags;
import com.github.isuhorukov.osm.pgsnapshot.pipeline.StoredVersions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class PostgresqlUpdater implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PostgresqlUpdater.class);
    private static final long UPDATE_LOCK_KEY = 0x6f736d5f75706474L;
    private static final double ANALYZE_CHANGED_FRACTION = 0.1;
    private static final String STORED_STATE_SQL = "SELECT replication_timestamp, replication_sequence, replication_url, "
            + "parameters::text FROM osm_import_state ORDER BY id DESC LIMIT 1";
    private static final String PARAMETERS_DIFFERENCE_SQL = "SELECT coalesce(string_agg(key || ': ' "
            + "|| coalesce(s.value::text, 'absent') || ' -> ' || coalesce(c.value::text, 'absent'), '; ' ORDER BY key), '') "
            + "FROM jsonb_each(?::jsonb) s FULL JOIN jsonb_each(?::jsonb) c USING (key) WHERE s.value IS DISTINCT FROM c.value";
    private static final String CHANGE_COUNTS_SQL = "SELECT object_type, count(*) FROM osm_change_log "
            + "WHERE import_id = (SELECT max(id) FROM osm_import_state) GROUP BY object_type";
    private static final String ESTIMATED_ROWS_SQL = "SELECT coalesce((SELECT sum(greatest(c.reltuples, 0)) FROM pg_inherits i "
            + "JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = ?::regclass), "
            + "(SELECT greatest(reltuples, 0) FROM pg_class WHERE oid = ?::regclass))::bigint";
    private static final String DEPENDENT_FOREIGN_KEYS_SQL = "WITH maintained AS (SELECT t.relid FROM unnest(?::text[]) n "
            + "CROSS JOIN LATERAL pg_partition_tree(n::regclass) t UNION SELECT n::regclass FROM unnest(?::text[]) n) "
            + "SELECT c.conrelid::regclass::text || ' (' || c.conname || ') -> ' || c.confrelid::regclass::text "
            + "FROM pg_constraint c WHERE c.contype = 'f' AND c.conparentid = 0 "
            + "AND c.confrelid IN (SELECT relid FROM maintained) AND c.conrelid NOT IN (SELECT relid FROM maintained) "
            + "ORDER BY 1";
    private static final String RELATIONS = "relations";
    private static final List<MaintainedTable> MAINTAINED_TABLES = List.of(new MaintainedTable("nodes", 'N', "nodes"),
            new MaintainedTable("ways", 'W', "ways"), new MaintainedTable(RELATIONS, 'R', RELATIONS),
            new MaintainedTable("relation_members", 'R', RELATIONS),
            new MaintainedTable("multipolygon", 'M', "multipolygon"));

    private final CliParameters parameters;
    private final OpenedConnections connections;
    private final InputFiles inputFiles = new InputFiles();
    private final MultipolygonSubset multipolygonSubset;
    private Connection updateConnection;
    private VersionCatalog catalog;
    private ScriptCopyWorker deltaWorker;
    private boolean upToDate;
    private boolean completed;

    private PostgresqlUpdater(CliParameters parameters, OpenedConnections connections, ExternalProcessing externalProcessing,
                              MultipolygonTags multipolygonTags) {
        this.parameters = parameters;
        this.connections = connections;
        this.multipolygonSubset = new MultipolygonSubset(externalProcessing, multipolygonTags);
    }

    public static PostgresqlUpdater start(CliParameters parameters, File sourcePbfFile, List<TagExtension> tagExtensions,
                                          MultipolygonTags multipolygonTags, ExternalProcessing externalProcessing,
                                          Consumer<Throwable> failureListener)
            throws IOException, SQLException, InterruptedException {
        OpenedConnections connections = new OpenedConnections(
                new PostgresqlConnections(parameters.getPostgresqlJdbcUrl(), System.getenv()));
        PostgresqlUpdater updater = new PostgresqlUpdater(parameters, connections, externalProcessing, multipolygonTags);
        try {
            updater.prepare(sourcePbfFile, tagExtensions, failureListener);
        } catch (IOException | SQLException | InterruptedException | RuntimeException e) {
            updater.close();
            throw e;
        }
        return updater;
    }

    private void prepare(File sourcePbfFile, List<TagExtension> tagExtensions, Consumer<Throwable> failureListener)
            throws IOException, SQLException, InterruptedException {
        updateConnection = connections.open("update");
        lock();
        StoredState stored = storedState();
        String parametersJson = ImportState.parametersJson(parameters, tagExtensions);
        checkParameters(stored.parametersJson, parametersJson);
        ReplicationHeader replication = ReplicationHeader.read(sourcePbfFile);
        ReplicationHeader.Freshness freshness = replication.freshnessAgainst(stored.replication);
        if (freshness == ReplicationHeader.Freshness.OLDER) {
            throw new UpdateRefusedException("The source PBF (" + replication + ") is older than the database ("
                    + stored.replication + ")");
        }
        if (freshness == ReplicationHeader.Freshness.SAME) {
            log.info("The database is already up to date with the source PBF ({})", replication);
            upToDate = true;
            return;
        }
        refuseIfDependentForeignKeys();
        inputFiles.add(PostgresqlLoader.IMPORT_STATE_PATH,
                ImportState.tsv(ImportState.UPDATE, sourcePbfFile, replication, parametersJson));
        inputFiles.add(PostgresqlLoader.STATIC_DIRECTORY + ResourceCopier.MULTIPOLYGON_INSERT_SQL,
                ResourceCopier.multipolygonInsertScript());
        new PsqlRunner(inputFiles).run(updateConnection, ResourceCopier.updateScript(ResourceCopier.UPDATE_PREPARE_SQL));
        catalog = VersionCatalog.read(connections);
        StreamedScript deltaScript = StreamedScript.of(ResourceCopier.updateScript(ResourceCopier.UPDATE_DELTA_SQL));
        int queueCapacity = PostgresqlLoader.queueCapacity(parameters);
        long pendingBytesLimit = PostgresqlLoader.pendingBytesLimit(1, Runtime.getRuntime().maxMemory());
        deltaWorker = new ScriptCopyWorker("copy-delta", connections.open("copy-delta"), queueCapacity,
                pendingBytesLimit, failureListener, deltaScript, new PsqlRunner(inputFiles));
        deltaWorker.start();
    }

    private void lock() throws SQLException {
        try (PreparedStatement statement = updateConnection.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
            statement.setLong(1, UPDATE_LOCK_KEY);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                if (!resultSet.getBoolean(1)) {
                    throw new UpdateRefusedException("Another update of this database is running");
                }
            }
        }
    }

    private StoredState storedState() throws SQLException {
        try (Statement statement = updateConnection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery("SELECT to_regclass('osm_import_state') IS NOT NULL")) {
                resultSet.next();
                if (!resultSet.getBoolean(1)) {
                    throw new UpdateRefusedException("The database was loaded without -updatable: "
                            + "load it again with -updatable to enable updates");
                }
            }
            try (ResultSet resultSet = statement.executeQuery(STORED_STATE_SQL)) {
                if (!resultSet.next()) {
                    throw new UpdateRefusedException("The database has no completed load in osm_import_state");
                }
                Timestamp timestamp = resultSet.getTimestamp(1);
                long sequence = resultSet.getLong(2);
                Long storedSequence = resultSet.wasNull() ? null : sequence;
                return new StoredState(new ReplicationHeader(timestamp != null ? timestamp.toInstant() : null,
                        storedSequence, resultSet.getString(3)), resultSet.getString(4));
            }
        }
    }

    private void refuseIfDependentForeignKeys() throws SQLException {
        String[] tables = MAINTAINED_TABLES.stream().map(table -> table.name).distinct().toArray(String[]::new);
        List<String> foreignKeys = new ArrayList<>();
        try (PreparedStatement statement = updateConnection.prepareStatement(DEPENDENT_FOREIGN_KEYS_SQL)) {
            Array names = updateConnection.createArrayOf("text", tables);
            statement.setArray(1, names);
            statement.setArray(2, names);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    foreignKeys.add(resultSet.getString(1));
                }
            }
        }
        if (!foreignKeys.isEmpty()) {
            throw new UpdateRefusedException("Foreign keys of other tables reference tables whose rows -update deletes "
                    + "and inserts again: " + String.join(", ", foreignKeys)
                    + ". Drop them, run -update, and add them again");
        }
    }

    private void checkParameters(String storedParametersJson, String parametersJson) throws SQLException {
        try (PreparedStatement statement = updateConnection.prepareStatement(PARAMETERS_DIFFERENCE_SQL)) {
            statement.setString(1, storedParametersJson);
            statement.setString(2, parametersJson);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                String difference = resultSet.getString(1);
                if (!difference.isEmpty()) {
                    throw new UpdateRefusedException("Loader parameters differ from the ones the database was loaded with: "
                            + difference + ". Use the same parameters and extension rules or load the database again");
                }
            }
        }
    }

    public boolean isUpToDate() {
        return upToDate;
    }

    public StoredVersions getStoredVersions() {
        return catalog;
    }

    public OrderedBlockWriter blockWriter(int maxBlocksInFlight, Consumer<Throwable> failureListener) {
        return new DeltaBlockWriter(deltaWorker, maxBlocksInFlight, failureListener);
    }

    public void finishDelta() throws InterruptedException {
        long start = System.currentTimeMillis();
        List<byte[]> changes = catalog.changes();
        if (!changes.isEmpty()) {
            deltaWorker.submit(new CopyChunk(DeltaBlockWriter.CHANGES, changes));
        }
        CopyWorker.finishAll(List.of(deltaWorker));
        log.info("delta committed in {} ms", System.currentTimeMillis() - start);
    }

    public long copyMultipolygonSubset(File sourcePbfFile) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        try (Connection connection = connections.open("multipolygon-subset")) {
            multipolygonSubset.copy(connection, new PsqlRunner(inputFiles), sourcePbfFile);
        } catch (SQLException e) {
            throw new IllegalStateException("multipolygon subset COPY failed", e);
        }
        return System.currentTimeMillis() - start;
    }

    public void cancelMultipolygonSubset() {
        multipolygonSubset.cancel();
    }

    public void applyRows() throws IOException, SQLException {
        long start = System.currentTimeMillis();
        new PsqlRunner(inputFiles).run(updateConnection, ResourceCopier.updateScript(ResourceCopier.UPDATE_APPLY_SQL));
        log.info("rows applied in {} ms", System.currentTimeMillis() - start);
    }

    public void applyMultipolygons() throws IOException, SQLException {
        long start = System.currentTimeMillis();
        new PsqlRunner(inputFiles).run(updateConnection, ResourceCopier.updateScript(ResourceCopier.UPDATE_MULTIPOLYGON_SQL));
        log.info("multipolygons applied and update committed in {} ms", System.currentTimeMillis() - start);
    }

    public void vacuum() throws IOException, InterruptedException, SQLException {
        long start = System.currentTimeMillis();
        Map<Character, Long> changes = changeCounts();
        Map<String, ParallelTasks.Task> tasks = new LinkedHashMap<>();
        for (MaintainedTable table : MAINTAINED_TABLES) {
            long changed = changes.getOrDefault(table.objectType, 0L);
            if (changed > 0) {
                String sql = (changed >= ANALYZE_CHANGED_FRACTION * estimatedRows(table.objectTable)
                        ? "VACUUM (ANALYZE) " : "VACUUM ") + table.name;
                tasks.put("vacuum-" + table.name, () -> {
                    try (Connection connection = connections.open("vacuum " + table.name)) {
                        PsqlRunner.executeSql(connection, sql);
                    }
                    log.info("{}", sql);
                });
            }
        }
        ParallelTasks.run(tasks);
        completed = true;
        log.info("vacuum finished in {} ms", System.currentTimeMillis() - start);
    }

    private Map<Character, Long> changeCounts() throws SQLException {
        Map<Character, Long> counts = new HashMap<>();
        try (Statement statement = updateConnection.createStatement();
             ResultSet resultSet = statement.executeQuery(CHANGE_COUNTS_SQL)) {
            while (resultSet.next()) {
                counts.put(resultSet.getString(1).charAt(0), resultSet.getLong(2));
            }
        }
        log.info("changes by object type: {}", counts);
        return counts;
    }

    private long estimatedRows(String table) throws SQLException {
        try (PreparedStatement statement = updateConnection.prepareStatement(ESTIMATED_ROWS_SQL)) {
            statement.setString(1, table);
            statement.setString(2, table);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }

    @Override
    public void close() {
        if (!completed && deltaWorker != null) {
            CopyWorker.abortAll(List.of(deltaWorker));
        }
        if (!completed) {
            cancelMultipolygonSubset();
        }
        connections.closeAll(!completed && !upToDate);
        if (!completed && deltaWorker != null) {
            CopyWorker.joinAborted(List.of(deltaWorker));
        }
    }

    private static final class StoredState {
        private final ReplicationHeader replication;
        private final String parametersJson;

        private StoredState(ReplicationHeader replication, String parametersJson) {
            this.replication = replication;
            this.parametersJson = parametersJson;
        }
    }

    private static final class MaintainedTable {
        private final String name;
        private final char objectType;
        private final String objectTable;

        private MaintainedTable(String name, char objectType, String objectTable) {
            this.name = name;
            this.objectType = objectType;
            this.objectTable = objectTable;
        }
    }
}
