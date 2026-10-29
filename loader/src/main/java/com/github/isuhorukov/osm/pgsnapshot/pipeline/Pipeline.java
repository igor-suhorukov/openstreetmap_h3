package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.ArrowFormat;
import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.ExternalProcessing;
import com.github.isuhorukov.osm.pgsnapshot.PipedProcess;
import com.github.isuhorukov.osm.pgsnapshot.Splitter;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminBinder;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminColumns;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminUnits;
import com.github.isuhorukov.osm.pgsnapshot.arrow.AdminDirectoryArrow;
import com.github.isuhorukov.osm.pgsnapshot.arrow.ArrowIpcWriter;
import com.github.isuhorukov.osm.pgsnapshot.arrow.ArrowSchemas;
import com.github.isuhorukov.osm.pgsnapshot.arrow.ArrowSpill;
import com.github.isuhorukov.osm.pgsnapshot.arrow.DuckDbParquetWriter;
import com.github.isuhorukov.osm.pgsnapshot.arrow.PartitionFiles;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.jdbc.PostgresqlLoader;
import com.github.isuhorukov.osm.pgsnapshot.jdbc.PostgresqlUpdater;
import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.MultipolygonTime;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import com.github.isuhorukov.osm.pgsnapshot.output.*;
import com.github.isuhorukov.osm.pgsnapshot.pbfextractor.ReplicationHeader;
import com.github.isuhorukov.osm.pgsnapshot.util.GeometryH33;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionEstimator;
import com.google.common.util.concurrent.MoreExecutors;
import com.uber.h3core.H3Core;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoder;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoderListener;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Pipeline {

    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);
    private static final int BLOCKS_IN_FLIGHT_PER_WORKER = 4;
    private static final int MULTIPOLYGON_OUTPUT_BUFFER = 1 << 16;

    private final CliParameters parameters;
    private final List<TagExtension> tagExtensions;
    private final MultipolygonTags multipolygonTags;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public Pipeline(CliParameters parameters) {
        this(parameters, Collections.emptyList());
    }

    public Pipeline(CliParameters parameters, List<TagExtension> tagExtensions) {
        this.parameters = parameters;
        this.tagExtensions = List.copyOf(tagExtensions);
        this.multipolygonTags = MultipolygonTags.of(this.tagExtensions);
    }

    public void run() throws IOException, InterruptedException {
        long commandStartTime = System.currentTimeMillis();

        File sourcePbfFile = new File(parameters.getSourceFilePath());
        if (!sourcePbfFile.exists() || sourcePbfFile.length() == 0) {
            throw new IllegalArgumentException("Input pbf should exists and should be non empty");
        }

        log.info("{}", parameters);
        initTagExtensions();
        ExternalProcessing externalProcessing = new ExternalProcessing(parameters.isInvokeDockerCommand());
        if (parameters.isAdminUnitsOnly()) {
            exportAdminUnitsOnly(externalProcessing, sourcePbfFile, commandStartTime);
        } else if (parameters.isUpdate()) {
            updatePostgresql(externalProcessing, sourcePbfFile, commandStartTime);
        } else if (parameters.isLoadPostgresqlJdbc()) {
            loadPostgresql(externalProcessing, sourcePbfFile, commandStartTime);
        } else {
            saveFiles(externalProcessing, sourcePbfFile, commandStartTime);
        }
    }

    private void updatePostgresql(ExternalProcessing externalProcessing, File sourcePbfFile, long commandStartTime)
            throws IOException, InterruptedException {
        try (PostgresqlUpdater updater = PostgresqlUpdater.start(parameters, sourcePbfFile, tagExtensions,
                multipolygonTags, externalProcessing, this::recordFailure)) {
            if (updater.isUpToDate()) {
                return;
            }
            long processingStartTime = System.currentTimeMillis();
            ProcessedBlocks processedBlocks = processBlocks(externalProcessing, sourcePbfFile, null,
                    updater.blockWriter(maxBlocksInFlight(), this::recordFailure), processingStartTime,
                    updater.getStoredVersions(), null);
            StatisticsWriter.printProcessingStatistics(processedBlocks.blockStatistics, processedBlocks.blockCount,
                    System.currentTimeMillis() - processingStartTime, processedBlocks.multipolygonCount());
            updater.finishDelta();
            try (MultipolygonExport multipolygonSubset = MultipolygonExport.start(
                    () -> updater.copyMultipolygonSubset(sourcePbfFile), updater::cancelMultipolygonSubset, true)) {
                updater.applyRows();
                multipolygonSubset.await();
            }
            updater.applyMultipolygons();
            updater.vacuum();
            log.info("update finished in {} ms", System.currentTimeMillis() - commandStartTime);
        } catch (SQLException e) {
            throw new IllegalStateException("Updating PostgreSQL failed", e);
        }
    }

    private void loadPostgresql(ExternalProcessing externalProcessing, File sourcePbfFile, long commandStartTime)
            throws IOException, InterruptedException {
        try (PostgresqlLoader loader = PostgresqlLoader.start(parameters, sourcePbfFile, tagExtensions,
                multipolygonTags, this::recordFailure)) {
            if (parameters.isUpdatable()) {
                loader.addImportState(importState(sourcePbfFile));
            }
            long processingStartTime = System.currentTimeMillis();
            ProcessedBlocks processedBlocks;
            long dataProcessingTime;
            MultipolygonTime multipolygonTime = new MultipolygonTime();
            try (MultipolygonExport multipolygonExport = MultipolygonExport.start(
                    () -> loader.copyMultipolygons(externalProcessing, sourcePbfFile), loader::cancelMultipolygons,
                    parameters.isParallelMultipolygonExport())) {
                processedBlocks = processBlocks(externalProcessing, sourcePbfFile, null,
                        loader.blockWriter(maxBlocksInFlight(), this::recordFailure), processingStartTime,
                        StoredVersions.NONE, null);
                dataProcessingTime = System.currentTimeMillis() - processingStartTime;
                StatisticsWriter.printProcessingStatistics(processedBlocks.blockStatistics, processedBlocks.blockCount,
                        dataProcessingTime, processedBlocks.multipolygonCount());
                loader.finishBlocks();
                multipolygonTime.setMultipolygonExportTime(multipolygonExport.await());
            }
            loader.finishImport();
            loader.saveStatistics(buildStatistics(processedBlocks.blockStatistics, processedBlocks.multipolygonCount(),
                    dataProcessingTime, processedBlocks.addLocationsToWaysTime, multipolygonTime,
                    System.currentTimeMillis() - commandStartTime));
        } catch (SQLException e) {
            throw new IllegalStateException("Loading into PostgreSQL failed", e);
        }
    }

    private void exportAdminUnitsOnly(ExternalProcessing externalProcessing, File sourcePbfFile,
                                      long commandStartTime) throws IOException, InterruptedException {
        AdminUnits adminUnits = AdminUnits.prepare(parameters, externalProcessing, sourcePbfFile);
        try (DuckDbParquetWriter exportWriter = exportParquetWriter()) {
            new AdminUnitsOutput(adminUnits, importStateJson(sourcePbfFile))
                    .export(new File(parameters.getAdminUnitsExport()), exportWriter)
                    .write();
        }
        log.info("admin unit directory exported in {} ms", System.currentTimeMillis() - commandStartTime);
    }

    private void saveFiles(ExternalProcessing externalProcessing, File sourcePbfFile, long commandStartTime)
            throws IOException, InterruptedException {
        File resultDirectory = prepareResultDirectory(sourcePbfFile);
        AdminUnits adminUnits = parameters.isAdminUnits()
                ? AdminUnits.prepare(parameters, externalProcessing, sourcePbfFile) : null;
        String importState = parameters.isSaveArrow() || adminUnits != null ? importStateJson(sourcePbfFile) : null;
        List<Partition> partitions = parameters.isSaveArrow()
                ? PartitionEstimator.estimatePartitions(sourcePbfFile, parameters.getThresholdPercentFromMaxPartition())
                : Collections.emptyList();
        try (DuckDbParquetWriter parquetWriter = parameters.isSaveArrow()
                ? DuckDbParquetWriter.open(parameters.getGeoParquetVersion(), parameters.getParquetCompression(),
                Set.of(ArrowSchemas.LINE_STRING_WKB, ArrowSchemas.WKB, AdminDirectoryArrow.GEOM))
                : null) {
            PartitionFiles partitionFiles = parquetWriter != null
                    ? partitionFiles(new File(resultDirectory, ResultLayout.ARROW_DIR), partitions, parquetWriter,
                    importState, adminUnits)
                    : null;
            try {
                saveFiles(externalProcessing, sourcePbfFile, resultDirectory, partitionFiles, adminUnits,
                        commandStartTime);
            } finally {
                if (partitionFiles != null) {
                    partitionFiles.deleteSpill();
                }
            }
            if (adminUnits != null) {
                saveAdminUnits(adminUnits, resultDirectory, partitionFiles, importState,
                        parameters.getArrowFormat() == ArrowFormat.PARQUET ? parquetWriter : null);
            }
        }
    }

    private void saveAdminUnits(AdminUnits adminUnits, File resultDirectory, PartitionFiles partitionFiles,
                                String importState, DuckDbParquetWriter parquetWriter)
            throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        AdminUnitsOutput output = new AdminUnitsOutput(adminUnits, importState);
        if (parameters.isSavePostgresqlTsv()) {
            output.database(new File(resultDirectory, AdminUnits.OUTPUT_DIRECTORY));
        }
        if (partitionFiles != null) {
            output.arrow(partitionFiles.getBlockWriter(),
                    new File(partitionFiles.getArrowDirectory(), AdminUnits.OUTPUT_DIRECTORY));
        }
        try (DuckDbParquetWriter exportWriter = parameters.getAdminUnitsExport() != null && parquetWriter == null
                ? exportParquetWriter() : null) {
            if (parameters.getAdminUnitsExport() != null) {
                output.export(new File(parameters.getAdminUnitsExport()),
                        parquetWriter != null ? parquetWriter : exportWriter);
            }
            output.write();
        }
        if (log.isInfoEnabled()) {
            log.info("admin unit directory written in {} ms: {}", System.currentTimeMillis() - start,
                    adminUnits.reportMetadataJson());
        }
    }

    private DuckDbParquetWriter exportParquetWriter() throws IOException {
        return DuckDbParquetWriter.open(parameters.getGeoParquetVersion(), parameters.getParquetCompression(),
                Set.of(AdminDirectoryArrow.GEOM));
    }

    private void saveFiles(ExternalProcessing externalProcessing, File sourcePbfFile, File resultDirectory,
                           PartitionFiles partitionFiles, AdminUnits adminUnits, long commandStartTime)
            throws IOException, InterruptedException {
        boolean saveImportScripts = !parameters.isCollectOnlyStat() && parameters.isSavePostgresqlTsv();
        boolean exportMultipolygons = saveImportScripts || parameters.isSaveArrow();
        AdminBinder adminBinder = adminUnits != null ? adminUnits.getBinder() : null;

        long processingStartTime = System.currentTimeMillis();
        try (MultipolygonExport multipolygonExport = MultipolygonExport.start(
                () -> exportMultipolygons(externalProcessing, sourcePbfFile, resultDirectory, saveImportScripts,
                        partitionFiles, adminBinder),
                exportMultipolygons && parameters.isParallelMultipolygonExport())) {
            ProcessedBlocks processedBlocks = processBlocksToFiles(externalProcessing, sourcePbfFile, resultDirectory,
                    partitionFiles, processingStartTime, adminBinder);
            long multipolygonCount = processedBlocks.multipolygonCount();
            long dataProcessingTime = System.currentTimeMillis() - processingStartTime;
            StatisticsWriter.printProcessingStatistics(processedBlocks.blockStatistics, processedBlocks.blockCount,
                    dataProcessingTime, multipolygonCount);
            if (partitionFiles != null) {
                writePartitionFiles(partitionFiles);
            }

            MultipolygonTime multipolygonTime = new MultipolygonTime();
            if (exportMultipolygons) {
                multipolygonTime.setMultipolygonExportTime(multipolygonExport.await());
            }
            if (saveImportScripts) {
                multipolygonTime.setSplitMultipolygonByPartsTime(writeImportScripts(externalProcessing,
                        resultDirectory, processedBlocks.blockStatistics, multipolygonCount));
            }

            StatisticsWriter.saveStatistics(resultDirectory, buildStatistics(processedBlocks.blockStatistics,
                    multipolygonCount, dataProcessingTime, processedBlocks.addLocationsToWaysTime, multipolygonTime,
                    System.currentTimeMillis() - commandStartTime));
            if (saveImportScripts && parameters.isUpdatable()) {
                ImportState.write(resultDirectory, importState(sourcePbfFile));
            }
        }
    }

    private PartitionFiles partitionFiles(File arrowDirectory, List<Partition> partitions,
                                          DuckDbParquetWriter parquetWriter, String importState,
                                          AdminUnits adminUnits) throws IOException {
        ArrowSpill arrowSpill = ArrowSpill.of(parameters.getSpillCompression(), parameters.getSpillZstdLevel(),
                adminUnits != null ? adminUnits.getColumns() : AdminColumns.NONE);
        Map<String, String> relationMetadata = Map.of(ImportState.METADATA_KEY, importState);
        Map<String, String> dataMetadata = new TreeMap<>(relationMetadata);
        if (adminUnits != null) {
            dataMetadata.put(AdminUnits.METADATA_KEY, adminUnits.metadataJson());
        }
        if (parameters.getArrowFormat() == ArrowFormat.ARROW_IPC) {
            return new PartitionFiles(arrowDirectory, partitions, arrowSpill,
                    new ArrowIpcWriter(parameters.getArrowIpcCompression(),
                            Set.of(ArrowSchemas.LINE_STRING_WKB, AdminDirectoryArrow.GEOM)),
                    parquetWriter.arrowIpcWriter(parameters.getArrowIpcCompression()), dataMetadata, relationMetadata);
        }
        return new PartitionFiles(arrowDirectory, partitions, arrowSpill, parquetWriter, parquetWriter, dataMetadata,
                relationMetadata);
    }

    private long exportMultipolygons(ExternalProcessing externalProcessing, File sourcePbfFile, File resultDirectory,
                                     boolean saveTsv, PartitionFiles partitionFiles, AdminBinder adminBinder)
            throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        MultipolygonBatchProcessor processor = new MultipolygonBatchProcessor(multipolygonTags, saveTsv,
                partitionFiles != null ? partitionFiles.getArrowSpill() : null, GeometryH33.instance(), adminBinder);
        try (PipedProcess export = externalProcessing.startMultipolygonExport(sourcePbfFile);
             OutputStream tsvOutput = saveTsv
                     ? new BufferedOutputStream(new FileOutputStream(new File(resultDirectory,
                     ExternalProcessing.MULTIPOLYGON_SOURCE_TSV)), MULTIPOLYGON_OUTPUT_BUFFER)
                     : OutputStream.nullOutputStream()) {
            long rows = MultipolygonRows.processRelations(export.getInputStream(), parameters.getWorkers(),
                    processor::process, result -> writeMultipolygons(result, tsvOutput, partitionFiles));
            export.waitForSuccess();
            log.info("{} multipolygon relations exported in {} ms", rows, System.currentTimeMillis() - start);
        }
        if (partitionFiles != null) {
            partitionFiles.writeMultipolygons();
        }
        return System.currentTimeMillis() - start;
    }

    private static void writeMultipolygons(MultipolygonBatchProcessor.Result result, OutputStream tsvOutput,
                                           PartitionFiles partitionFiles) throws IOException {
        ByteBuffer tsvRows = result.getTsvRows();
        if (tsvRows != null) {
            tsvOutput.write(tsvRows.array(), tsvRows.arrayOffset() + tsvRows.position(), tsvRows.remaining());
        }
        if (partitionFiles != null) {
            partitionFiles.appendMultipolygons(result.getSpillChunks());
        }
    }

    private void writePartitionFiles(PartitionFiles partitionFiles) throws IOException, InterruptedException {
        ExecutorService executorService = getExecutorService(parameters.getWorkers());
        try {
            partitionFiles.writeBlocks(executorService);
        } finally {
            executorService.shutdownNow();
        }
    }

    private byte[] importState(File sourcePbfFile) throws IOException {
        return ImportState.tsv(ImportState.LOAD, sourcePbfFile, ReplicationHeader.read(sourcePbfFile),
                ImportState.parametersJson(parameters, tagExtensions));
    }

    private String importStateJson(File sourcePbfFile) throws IOException {
        return ImportState.json(ImportState.LOAD, sourcePbfFile, ReplicationHeader.read(sourcePbfFile),
                ImportState.parametersJson(parameters, tagExtensions));
    }

    private void initTagExtensions() throws IOException {
        for (TagExtension tagExtension : tagExtensions) {
            log.info("Tag extension {}", tagExtension.getName());
            tagExtension.init();
        }
    }

    private File prepareResultDirectory(File sourcePbfFile) throws IOException {
        File resultDirectory = ResultLayout.prepareResultDirectories(
                new File(sourcePbfFile.getParent(), ResultLayout.resultDirectoryNameFromSource(sourcePbfFile)),
                parameters.isSavePostgresqlTsv(), parameters.isSaveArrow());

        ResourceCopier.copyOsmiumSettings(resultDirectory);
        if (parameters.isSavePostgresqlTsv()) {
            ResourceCopier.copyResources(resultDirectory, parameters.isColumnarStorage(), parameters.isUpdatable(),
                    adminColumns());
            DirectoryPostgresqlResources postgresqlResources = new DirectoryPostgresqlResources(resultDirectory);
            for (TagExtension tagExtension : tagExtensions) {
                tagExtension.writePostgresqlResources(postgresqlResources);
            }
        }
        return resultDirectory;
    }

    private AdminColumns adminColumns() {
        return AdminColumns.of(parameters.isAdminUnits(), parameters.isAdminUnitShares());
    }

    private int maxBlocksInFlight() {
        return Math.max(1, parameters.getWorkers()) * BLOCKS_IN_FLIGHT_PER_WORKER;
    }

    private ProcessedBlocks processBlocksToFiles(ExternalProcessing externalProcessing, File sourcePbfFile,
                                                 File resultDirectory, PartitionFiles partitionFiles,
                                                 long processingStartTime, AdminBinder adminBinder)
            throws IOException, InterruptedException {
        ExecutorService saveExecutorService = getExecutorService(parameters.getWorkers());
        File spillDirectory = partitionFiles != null ? partitionFiles.getSpillDirectory() : null;
        ArrowSpill arrowSpill = partitionFiles != null ? partitionFiles.getArrowSpill() : null;
        try {
            return processBlocks(externalProcessing, sourcePbfFile, arrowSpill, new TsvBlockWriter(
                    saveExecutorService, resultDirectory, spillDirectory, maxBlocksInFlight(), this::recordFailure),
                    processingStartTime, StoredVersions.NONE, adminBinder);
        } finally {
            saveExecutorService.shutdownNow();
        }
    }

    private ProcessedBlocks processBlocks(ExternalProcessing externalProcessing, File sourcePbfFile,
                                          ArrowSpill arrowSpill, OrderedBlockWriter blockWriter,
                                          long processingStartTime, StoredVersions storedVersions,
                                          AdminBinder adminBinder)
            throws IOException, InterruptedException {
        final H3Core h3Core = H3Core.newInstance();
        ThreadLocal<BlockProcessor> blockProcessors = ThreadLocal.withInitial(
                () -> new BlockProcessor(parameters, h3Core, tagExtensions, storedVersions, adminBinder));

        ExecutorService executorService = getExecutorService(parameters.getWorkers());

        Map<Long, BlockStat> blockStat = new ConcurrentHashMap<>();
        int blockCount;
        long addLocationsToWaysTime;
        try (PipedProcess addLocationsToWays = externalProcessing.startAddLocationsToWays(sourcePbfFile,
                parameters.isPreserveAllNodes())) {
            blockCount = Splitter.splitPbfByBlocks(addLocationsToWays.getInputStream(), (rawBlob, blockNumber) ->
                    submitBlock(executorService, blockWriter, blockNumber, () -> processBlock(rawBlob,
                            blockNumber, blockProcessors, arrowSpill, blockStat, blockWriter)));
            addLocationsToWays.waitForSuccess();
            addLocationsToWaysTime = System.currentTimeMillis() - processingStartTime;
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.DAYS);
        } finally {
            executorService.shutdownNow();
        }
        throwIfFailed();
        return new ProcessedBlocks(new ArrayList<>(blockStat.values()), blockCount, addLocationsToWaysTime);
    }

    private long writeImportScripts(ExternalProcessing externalProcessing, File resultDirectory,
                                    List<BlockStat> blockStatistics, long multipolygonCount)
            throws IOException, InterruptedException {
        PartitioningScriptWriter.savePartitioningScripts(resultDirectory, parameters.getScriptCount(),
                parameters.getThresholdPercentFromMaxPartition(), blockStatistics, parameters.isColumnarStorage(),
                parameters.isUpdatable(), adminColumns());
        ResourceCopier.copyImportScripts(resultDirectory, parameters.isColumnarStorage(), parameters.isUpdatable(),
                adminColumns());
        return externalProcessing.splitMultipolygonsAndWriteScripts(resultDirectory, parameters.getScriptCount(),
                multipolygonCount);
    }

    private static PbfStatistics buildStatistics(List<BlockStat> blockStatistics, long multipolygonCount,
                                                 long dataProcessingTime, long addLocationsToWaysTime,
                                                 MultipolygonTime multipolygonTime, long totalTime) {
        PbfStatistics statistics = new PbfStatistics(blockStatistics);
        statistics.setMultipolygonCount(multipolygonCount);
        statistics.setDataProcessingTime(dataProcessingTime);
        statistics.setAddLocationsToWaysTime(addLocationsToWaysTime);
        statistics.setPbfSplitTime(0);
        statistics.setMultipolygonExportTime(multipolygonTime.getMultipolygonExportTime());
        statistics.setSplitMultipolygonByPartsTime(multipolygonTime.getSplitMultipolygonByPartsTime());
        statistics.setTotalTime(totalTime);
        return statistics;
    }

    private void submitBlock(ExecutorService executorService, OrderedBlockWriter blockWriter, long blockNumber,
                             Runnable block) {
        throwIfFailed();
        try {
            blockWriter.reserve();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for a free block slot", e);
        }
        CompletableFuture.runAsync(block, executorService).exceptionally(error -> {
            recordFailure(new IllegalStateException("Block " + blockNumber + " processing failed", unwrap(error)));
            return null;
        });
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    private void processBlock(RawBlob rawBlob, long blockNumber, ThreadLocal<BlockProcessor> blockProcessors,
                              ArrowSpill arrowSpill, Map<Long, BlockStat> blockStat, OrderedBlockWriter blockWriter) {
        long threadStart = System.currentTimeMillis();
        if (blockNumber % 1000 == 0) {
            log.info("{}", blockNumber);
        }
        EncodedBlock encodedBlock = EncodedBlock.EMPTY;
        try {
            BlockResult result = decodeBlock(rawBlob, blockNumber, blockProcessors.get(), threadStart);
            blockStat.put(blockNumber, result.getBlockStat());
            if (!parameters.isCollectOnlyStat()) {
                encodedBlock = persistBlock(result, arrowSpill);
            }
            result.getBlockStat().setThreadTime(System.currentTimeMillis() - threadStart);
        } finally {
            blockWriter.complete(blockNumber, encodedBlock);
        }
    }

    private static BlockResult decodeBlock(RawBlob rawBlob, long blockNumber, BlockProcessor blockProcessor,
                                           long threadStart) {
        BlockListener blockListener = new BlockListener(blockProcessor, blockNumber, threadStart);
        new PbfBlobDecoder(rawBlob, blockListener).run();
        return blockListener.getResult();
    }

    private EncodedBlock persistBlock(BlockResult result, ArrowSpill arrowSpill) {
        BlockStat blockStatistic = result.getBlockStat();
        Map<Short, SpillChunk> spillChunks = Collections.emptyMap();
        if (arrowSpill != null) {
            long startSaveTime = System.currentTimeMillis();
            spillChunks = arrowSpill.encode(result.getArrowNodeOrWays(), result.getArrowRelations(),
                    blockStatistic.getId());
            blockStatistic.setSaveTime(System.currentTimeMillis() - startSaveTime);
        }
        if (!parameters.isSavePostgresqlTsv() && spillChunks.isEmpty()) {
            return EncodedBlock.EMPTY;
        }
        long startEncodeTime = System.currentTimeMillis();
        BlockType blockType = BlockType.of(result.getNodeCount(), result.getWayCount(), result.getRelationCount());
        EncodedBlock encodedBlock = EncodedBlock.encode(blockType, result.getCsvResultPerH33(), blockStatistic)
                .withSpillChunks(spillChunks);
        blockStatistic.setSaveTime(blockStatistic.getSaveTime() + (System.currentTimeMillis() - startEncodeTime));
        return encodedBlock;
    }

    private void recordFailure(Throwable cause) {
        failure.compareAndSet(null, cause);
        log.error("Block processing failed", cause);
    }

    private void throwIfFailed() {
        Throwable cause = failure.get();
        if (cause != null) {
            throw new IllegalStateException("Block processing failed", cause);
        }
    }

    private static ExecutorService getExecutorService(int workers) {
        if (workers > 0) {
            return Executors.newFixedThreadPool(workers);
        }
        return MoreExecutors.newDirectExecutorService();
    }

    private static final class ProcessedBlocks {
        private final List<BlockStat> blockStatistics;
        private final int blockCount;
        private final long addLocationsToWaysTime;

        private ProcessedBlocks(List<BlockStat> blockStatistics, int blockCount, long addLocationsToWaysTime) {
            this.blockStatistics = blockStatistics;
            this.blockCount = blockCount;
            this.addLocationsToWaysTime = addLocationsToWaysTime;
        }

        private long multipolygonCount() {
            return blockStatistics.stream().map(BlockStat::getMultipolygonCount).mapToLong(Long::longValue).sum();
        }
    }

    private static final class BlockListener implements PbfBlobDecoderListener {
        private final BlockProcessor blockProcessor;
        private final long blockNumber;
        private final long threadStart;
        private BlockResult result;
        private RuntimeException processingFailure;
        private boolean decodingFailed;

        private BlockListener(BlockProcessor blockProcessor, long blockNumber, long threadStart) {
            this.blockProcessor = blockProcessor;
            this.blockNumber = blockNumber;
            this.threadStart = threadStart;
        }

        @Override
        public void complete(List<EntityContainer> decodedEntities) {
            long blockStartTime = System.currentTimeMillis();
            try {
                result = blockProcessor.process(decodedEntities, blockNumber, threadStart);
                result.getBlockStat().setProcessingTime(System.currentTimeMillis() - blockStartTime);
            } catch (RuntimeException e) {
                processingFailure = e;
            }
        }

        @Override
        public void error() {
            decodingFailed = true;
        }

        private BlockResult getResult() {
            if (processingFailure != null) {
                throw processingFailure;
            }
            if (decodingFailed || result == null) {
                throw new IllegalStateException("Failed to decode block " + blockNumber);
            }
            return result;
        }
    }
}
