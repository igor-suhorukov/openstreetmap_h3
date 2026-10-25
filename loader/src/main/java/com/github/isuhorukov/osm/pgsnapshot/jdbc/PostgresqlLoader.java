package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.ExternalProcessing;
import com.github.isuhorukov.osm.pgsnapshot.PipedProcess;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import com.github.isuhorukov.osm.pgsnapshot.output.OrderedBlockWriter;
import com.github.isuhorukov.osm.pgsnapshot.output.ResourceCopier;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import com.github.isuhorukov.osm.pgsnapshot.output.StatisticsWriter;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;
import org.postgresql.copy.CopyIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class PostgresqlLoader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PostgresqlLoader.class);
    private static final String STATIC_DIRECTORY = ResultLayout.STATIC_DIR + "/";
    private static final int QUEUED_CHUNKS_PER_WORKER_THREAD = 16;
    private static final long MIN_PENDING_BYTES_PER_WORKER = 1L << 20;
    private static final long MAX_PENDING_BYTES_PER_WORKER = 128L << 20;
    private static final List<String> FINALIZE_SCRIPTS = List.of(ResourceCopier.MULTIPOLYGON_IMPORT_SQL,
            ResourceCopier.WAYS_DEFAULT_PARTITION_SQL, ResourceCopier.NODES_ANALYZE_SQL);

    private final CliParameters parameters;
    private final PostgresqlConnections connections;
    private final InputFiles inputFiles = new InputFiles();
    private final List<Connection> openedConnections = new CopyOnWriteArrayList<>();
    private final List<CopyWorker> workers = new ArrayList<>();
    private final CopyTargets targets = new CopyTargets();
    private final AtomicReference<PipedProcess> multipolygonExport = new AtomicReference<>();
    private volatile boolean multipolygonExportCancelled;
    private boolean completed;

    private PostgresqlLoader(CliParameters parameters, PostgresqlConnections connections) {
        this.parameters = parameters;
        this.connections = connections;
    }

    public static PostgresqlLoader start(CliParameters parameters, File sourcePbfFile, List<TagExtension> tagExtensions,
                                         Set<String> multipolygonTagKeys, Consumer<Throwable> failureListener)
            throws IOException, SQLException {
        PostgresqlConnections connections = new PostgresqlConnections(parameters.getPostgresqlJdbcUrl(), System.getenv());
        List<Partition> partitions = PartitionEstimator.estimatePartitions(sourcePbfFile,
                parameters.getThresholdPercentFromMaxPartition());
        PostgresqlLoader loader = new PostgresqlLoader(parameters, connections);
        try {
            loader.initializeDatabase(partitions, tagExtensions, multipolygonTagKeys);
            loader.startWorkers(partitions, failureListener);
        } catch (IOException | SQLException | RuntimeException e) {
            loader.close();
            throw e;
        }
        return loader;
    }

    private void initializeDatabase(List<Partition> partitions, List<TagExtension> tagExtensions, Set<String> multipolygonTagKeys)
            throws IOException, SQLException {
        long start = System.currentTimeMillis();
        boolean columnarStorage = parameters.isColumnarStorage();
        for (Partition partition : partitions) {
            log.info("partition {}\t[{},{})\testimated weight {}", partition.getId(), partition.getMinRange(),
                    partition.getMaxRange(), partition.getSerializedSize());
        }
        inputFiles.add(STATIC_DIRECTORY + PartitionSplitter.MULTIPOLYGON_TABLES_SQL,
                PartitionSplitter.multipolygonTablesSql(partitions, columnarStorage));
        inputFiles.addResource(STATIC_DIRECTORY + ResourceCopier.H3_POLY_TSV_GZ, "/" + ResourceCopier.H3_POLY_TSV_GZ);
        inputFiles.add(STATIC_DIRECTORY + ResourceCopier.MULTIPOLYGON_SQL,
                ResourceCopier.multipolygonScript(columnarStorage, multipolygonTagKeys));
        try (Connection connection = open("init")) {
            PsqlRunner runner = new PsqlRunner(inputFiles);
            runner.run(connection, ResourceCopier.databaseInitScript(columnarStorage));
            JdbcPostgresqlResources extensionResources = new JdbcPostgresqlResources(inputFiles);
            for (TagExtension tagExtension : tagExtensions) {
                tagExtension.writePostgresqlResources(extensionResources);
            }
            for (String importScript : extensionResources.getImportScripts()) {
                runner.run(connection, importScript);
            }
        }
        log.info("database schema created in {} ms", System.currentTimeMillis() - start);
    }

    private void startWorkers(List<Partition> partitions, Consumer<Throwable> failureListener) throws IOException, SQLException {
        int queueCapacity = Math.max(1, parameters.getWorkers()) * QUEUED_CHUNKS_PER_WORKER_THREAD;
        boolean columnarStorage = parameters.isColumnarStorage();
        List<List<Partition>> partitionsByWorker = PartitionSplitter.balanceBySize(partitions, parameters.getScriptCount());
        long pendingBytesLimit = pendingBytesLimit(partitionsByWorker.size() + 2, Runtime.getRuntime().maxMemory());
        for (int workerIndex = 0; workerIndex < partitionsByWorker.size(); workerIndex++) {
            List<PartitionTable> tables = new ArrayList<>();
            for (Partition partition : partitionsByWorker.get(workerIndex)) {
                tables.add(new PartitionTable(PartitionSplitter.NODES, PartitionSplitter.NODE_COLUMNS, partition, columnarStorage));
                tables.add(new PartitionTable(PartitionSplitter.WAYS, PartitionSplitter.WAY_COLUMNS, partition, columnarStorage));
            }
            String name = "copy-partitions-" + workerIndex;
            PartitionCopyWorker worker = new PartitionCopyWorker(name, open(name), queueCapacity, pendingBytesLimit,
                    failureListener, tables);
            for (Partition partition : partitionsByWorker.get(workerIndex)) {
                targets.addPartition(partition, worker);
            }
            workers.add(worker);
        }
        addScriptWorker("copy-relations", ResourceCopier.resource("/" + ResourceCopier.RELATIONS_IMPORT_SQL),
                queueCapacity, pendingBytesLimit, failureListener);
        addScriptWorker("copy-ways-32767", ResourceCopier.ways32767ImportScript(columnarStorage),
                queueCapacity, pendingBytesLimit, failureListener);
        for (CopyWorker worker : workers) {
            worker.start();
        }
    }

    static long pendingBytesLimit(int workerCount, long maxMemory) {
        return Math.max(MIN_PENDING_BYTES_PER_WORKER,
                Math.min(MAX_PENDING_BYTES_PER_WORKER, maxMemory / (4L * Math.max(1, workerCount))));
    }

    private void addScriptWorker(String name, String script, int queueCapacity, long pendingBytesLimit,
                                 Consumer<Throwable> failureListener) throws SQLException {
        StreamedScript streamedScript = StreamedScript.of(script);
        ScriptCopyWorker worker = new ScriptCopyWorker(name, open(name), queueCapacity, pendingBytesLimit,
                failureListener, streamedScript, new PsqlRunner(inputFiles));
        for (String inputPath : streamedScript.getCopySqlByInputPath().keySet()) {
            targets.addInput(inputPath, worker);
        }
        workers.add(worker);
    }

    public OrderedBlockWriter blockWriter(int maxBlocksInFlight, Consumer<Throwable> failureListener) {
        return new PgCopyBlockWriter(targets, maxBlocksInFlight, failureListener);
    }

    public long copyMultipolygons(ExternalProcessing externalProcessing, File sourcePbfFile)
            throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        try (Connection connection = open("copy-multipolygons");
             PipedProcess export = externalProcessing.startMultipolygonExport(sourcePbfFile)) {
            multipolygonExport.set(export);
            if (multipolygonExportCancelled) {
                throw new InterruptedException("multipolygon export cancelled");
            }
            CopyIn copyIn = PsqlRunner.copyManager(connection).copyIn(MultipolygonCopy.COPY_SQL);
            long rows = MultipolygonCopy.copyRelations(export.getInputStream(), copyIn);
            export.waitForSuccess();
            copyIn.endCopy();
            log.info("{} multipolygon relations copied in {} ms", rows, System.currentTimeMillis() - start);
        } catch (SQLException e) {
            throw new IllegalStateException("multipolygon COPY failed", e);
        } finally {
            multipolygonExport.set(null);
        }
        return System.currentTimeMillis() - start;
    }

    public void cancelMultipolygons() {
        multipolygonExportCancelled = true;
        PipedProcess export = multipolygonExport.get();
        if (export != null) {
            export.close();
        }
    }

    public void finishBlocks() throws InterruptedException {
        long start = System.currentTimeMillis();
        for (CopyWorker worker : workers) {
            worker.endOfStream();
        }
        for (CopyWorker worker : workers) {
            worker.join();
        }
        IllegalStateException failure = null;
        for (CopyWorker worker : workers) {
            if (worker.getFailure() != null) {
                if (failure == null) {
                    failure = new IllegalStateException("COPY worker " + worker.getName() + " failed", worker.getFailure());
                } else {
                    failure.addSuppressed(worker.getFailure());
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
        log.info("COPY workers committed in {} ms", System.currentTimeMillis() - start);
    }

    public void finishImport() throws IOException, InterruptedException, SQLException {
        long start = System.currentTimeMillis();
        List<Future<Void>> scripts = new ArrayList<>();
        for (String scriptName : FINALIZE_SCRIPTS) {
            String script = ResourceCopier.resource("/" + scriptName);
            FutureTask<Void> scriptTask = new FutureTask<>(() -> {
                try (Connection connection = open("finalize " + scriptName)) {
                    new PsqlRunner(inputFiles).run(connection, script);
                }
                return null;
            });
            new Thread(scriptTask, "finalize-" + scriptName).start();
            scripts.add(scriptTask);
        }
        for (Future<Void> script : scripts) {
            awaitScript(script);
        }
        log.info("import finalized in {} ms", System.currentTimeMillis() - start);
    }

    private static void awaitScript(Future<Void> script) throws IOException, InterruptedException, SQLException {
        try {
            script.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException) {
                throw (SQLException) cause;
            }
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException(cause);
        }
    }

    public void saveStatistics(PbfStatistics statistics) throws IOException, SQLException {
        for (Map.Entry<String, byte[]> statisticsFile : StatisticsWriter.statisticsFiles(statistics).entrySet()) {
            inputFiles.add(ResultLayout.IMPORT_RELATED_METADATA_DIR + "/" + statisticsFile.getKey(), statisticsFile.getValue());
        }
        try (Connection connection = open("statistics")) {
            new PsqlRunner(inputFiles).run(connection, ResourceCopier.resource("/" + ResourceCopier.DATABASE_AFTER_INIT_SQL));
        }
        completed = true;
    }

    @Override
    public void close() {
        if (!completed) {
            for (CopyWorker worker : workers) {
                worker.abort();
            }
            cancelMultipolygons();
        }
        for (Connection connection : openedConnections) {
            closeConnection(connection, !completed);
        }
        if (!completed) {
            for (CopyWorker worker : workers) {
                joinAborted(worker);
            }
        }
    }

    private Connection open(String purpose) throws SQLException {
        Connection connection = connections.open(purpose);
        openedConnections.add(connection);
        return connection;
    }

    private static void closeConnection(Connection connection, boolean abort) {
        try {
            if (abort) {
                connection.abort(Runnable::run);
            } else {
                connection.close();
            }
        } catch (SQLException e) {
            log.warn("Failed to close PostgreSQL connection", e);
        }
    }

    private static void joinAborted(CopyWorker worker) {
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
