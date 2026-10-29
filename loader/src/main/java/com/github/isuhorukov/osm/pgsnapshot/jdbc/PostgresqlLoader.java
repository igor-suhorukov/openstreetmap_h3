package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.ExternalProcessing;
import com.github.isuhorukov.osm.pgsnapshot.PipedProcess;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import com.github.isuhorukov.osm.pgsnapshot.output.ImportState;
import com.github.isuhorukov.osm.pgsnapshot.output.OrderedBlockWriter;
import com.github.isuhorukov.osm.pgsnapshot.output.ResourceCopier;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import com.github.isuhorukov.osm.pgsnapshot.output.StatisticsWriter;
import com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonTags;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionEstimator;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class PostgresqlLoader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PostgresqlLoader.class);
    static final String STATIC_DIRECTORY = ResultLayout.STATIC_DIR + "/";
    static final String IMPORT_STATE_PATH = ResultLayout.IMPORT_RELATED_METADATA_DIR + '/' + ImportState.OSM_IMPORT_STATE_TSV;
    static final int QUEUED_CHUNKS_PER_WORKER_THREAD = 16;
    private static final long MIN_PENDING_BYTES_PER_WORKER = 1L << 20;
    private static final long MAX_PENDING_BYTES_PER_WORKER = 128L << 20;

    private final CliParameters parameters;
    private final OpenedConnections connections;
    private final MultipolygonTags multipolygonTags;
    private final InputFiles inputFiles = new InputFiles();
    private final List<CopyWorker> workers = new ArrayList<>();
    private final CopyTargets targets = new CopyTargets();
    private final AtomicReference<PipedProcess> multipolygonExport = new AtomicReference<>();
    private volatile boolean multipolygonExportCancelled;
    private boolean completed;

    private PostgresqlLoader(CliParameters parameters, OpenedConnections connections, MultipolygonTags multipolygonTags) {
        this.parameters = parameters;
        this.connections = connections;
        this.multipolygonTags = multipolygonTags;
    }

    public static PostgresqlLoader start(CliParameters parameters, File sourcePbfFile, List<TagExtension> tagExtensions,
                                         MultipolygonTags multipolygonTags, Consumer<Throwable> failureListener)
            throws IOException, SQLException {
        OpenedConnections connections = new OpenedConnections(
                new PostgresqlConnections(parameters.getPostgresqlJdbcUrl(), System.getenv()));
        List<Partition> partitions = PartitionEstimator.estimatePartitions(sourcePbfFile,
                parameters.getThresholdPercentFromMaxPartition());
        PostgresqlLoader loader = new PostgresqlLoader(parameters, connections, multipolygonTags);
        try {
            loader.initializeDatabase(partitions, tagExtensions);
            loader.startWorkers(partitions, failureListener);
        } catch (IOException | SQLException | RuntimeException e) {
            loader.close();
            throw e;
        }
        return loader;
    }

    private void initializeDatabase(List<Partition> partitions, List<TagExtension> tagExtensions)
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
        inputFiles.add(STATIC_DIRECTORY + ResourceCopier.MULTIPOLYGON_SQL, ResourceCopier.multipolygonScript(columnarStorage));
        inputFiles.add(STATIC_DIRECTORY + ResourceCopier.MULTIPOLYGON_INSERT_SQL,
                ResourceCopier.multipolygonInsertScript());
        try (Connection connection = connections.open("init")) {
            PsqlRunner runner = new PsqlRunner(inputFiles);
            runner.run(connection, ResourceCopier.databaseInitScript(columnarStorage, parameters.isUpdatable()));
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
        int queueCapacity = queueCapacity(parameters);
        boolean columnarStorage = parameters.isColumnarStorage();
        boolean updatable = parameters.isUpdatable();
        List<List<Partition>> partitionsByWorker = PartitionSplitter.balanceBySize(partitions, parameters.getScriptCount());
        long pendingBytesLimit = pendingBytesLimit(partitionsByWorker.size() + 2, Runtime.getRuntime().maxMemory());
        for (int workerIndex = 0; workerIndex < partitionsByWorker.size(); workerIndex++) {
            List<PartitionTable> tables = new ArrayList<>();
            for (Partition partition : partitionsByWorker.get(workerIndex)) {
                tables.add(new PartitionTable(PartitionSplitter.NODES, PartitionSplitter.nodeColumns(updatable),
                        partition, columnarStorage));
                tables.add(new PartitionTable(PartitionSplitter.WAYS, PartitionSplitter.wayColumns(updatable),
                        partition, columnarStorage));
            }
            String name = "copy-partitions-" + workerIndex;
            PartitionCopyWorker worker = new PartitionCopyWorker(name, connections.open(name), queueCapacity,
                    pendingBytesLimit, failureListener, tables, updatable);
            for (Partition partition : partitionsByWorker.get(workerIndex)) {
                targets.addPartition(partition, worker);
            }
            workers.add(worker);
        }
        addScriptWorker("copy-relations", ResourceCopier.relationsImportScript(updatable),
                queueCapacity, pendingBytesLimit, failureListener);
        addScriptWorker("copy-ways-32767", ResourceCopier.ways32767ImportScript(columnarStorage, updatable),
                queueCapacity, pendingBytesLimit, failureListener);
        for (CopyWorker worker : workers) {
            worker.start();
        }
    }

    static int queueCapacity(CliParameters parameters) {
        return Math.max(1, parameters.getWorkers()) * QUEUED_CHUNKS_PER_WORKER_THREAD;
    }

    static long pendingBytesLimit(int workerCount, long maxMemory) {
        return Math.max(MIN_PENDING_BYTES_PER_WORKER,
                Math.min(MAX_PENDING_BYTES_PER_WORKER, maxMemory / (4L * Math.max(1, workerCount))));
    }

    private void addScriptWorker(String name, String script, int queueCapacity, long pendingBytesLimit,
                                 Consumer<Throwable> failureListener) throws SQLException {
        StreamedScript streamedScript = StreamedScript.of(script);
        ScriptCopyWorker worker = new ScriptCopyWorker(name, connections.open(name), queueCapacity, pendingBytesLimit,
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
        try (Connection connection = connections.open("copy-multipolygons");
             PipedProcess export = externalProcessing.startMultipolygonExport(sourcePbfFile)) {
            multipolygonExport.set(export);
            if (multipolygonExportCancelled) {
                throw new InterruptedException("multipolygon export cancelled");
            }
            long rows = MultipolygonCopy.copyExport(connection, export, multipolygonTags, parameters.getWorkers());
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
        CopyWorker.finishAll(workers);
        log.info("COPY workers committed in {} ms", System.currentTimeMillis() - start);
    }

    public void finishImport() throws IOException, InterruptedException, SQLException {
        long start = System.currentTimeMillis();
        boolean updatable = parameters.isUpdatable();
        Map<String, String> scripts = new LinkedHashMap<>();
        scripts.put(ResourceCopier.MULTIPOLYGON_IMPORT_SQL, ResourceCopier.resource("/" + ResourceCopier.MULTIPOLYGON_IMPORT_SQL));
        scripts.put(ResourceCopier.WAYS_DEFAULT_PARTITION_SQL, ResourceCopier.waysDefaultPartitionScript(updatable));
        scripts.put(ResourceCopier.NODES_ANALYZE_SQL, ResourceCopier.nodesAnalyzeScript(updatable));
        Map<String, ParallelTasks.Task> tasks = new LinkedHashMap<>();
        for (Map.Entry<String, String> script : scripts.entrySet()) {
            tasks.put("finalize-" + script.getKey(), () -> {
                try (Connection connection = connections.open("finalize " + script.getKey())) {
                    new PsqlRunner(inputFiles).run(connection, script.getValue());
                }
            });
        }
        ParallelTasks.run(tasks);
        log.info("import finalized in {} ms", System.currentTimeMillis() - start);
    }

    public void addImportState(byte[] importState) {
        inputFiles.add(IMPORT_STATE_PATH, importState);
    }

    public void saveStatistics(PbfStatistics statistics) throws IOException, SQLException {
        for (Map.Entry<String, byte[]> statisticsFile : StatisticsWriter.statisticsFiles(statistics).entrySet()) {
            inputFiles.add(ResultLayout.IMPORT_RELATED_METADATA_DIR + "/" + statisticsFile.getKey(), statisticsFile.getValue());
        }
        try (Connection connection = connections.open("statistics")) {
            new PsqlRunner(inputFiles).run(connection, ResourceCopier.databaseAfterInitScript(parameters.isUpdatable()));
        }
        completed = true;
    }

    @Override
    public void close() {
        if (!completed) {
            CopyWorker.abortAll(workers);
            cancelMultipolygons();
        }
        connections.closeAll(!completed);
        if (!completed) {
            CopyWorker.joinAborted(workers);
        }
    }
}
