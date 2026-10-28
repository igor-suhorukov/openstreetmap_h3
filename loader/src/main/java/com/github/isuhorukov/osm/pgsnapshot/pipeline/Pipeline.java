package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.ArrowFormat;
import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.ExternalProcessing;
import com.github.isuhorukov.osm.pgsnapshot.PipedProcess;
import com.github.isuhorukov.osm.pgsnapshot.Splitter;
import com.github.isuhorukov.osm.pgsnapshot.arrow.DuckDbParquetWriter;
import com.github.isuhorukov.osm.pgsnapshot.arrow.NodeWayArrowWriter;
import com.github.isuhorukov.osm.pgsnapshot.arrow.ParquetWriter;
import com.github.isuhorukov.osm.pgsnapshot.arrow.RelationArrowWriter;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.jdbc.PostgresqlLoader;
import com.github.isuhorukov.osm.pgsnapshot.jdbc.PostgresqlUpdater;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.MultipolygonTime;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import com.github.isuhorukov.osm.pgsnapshot.output.*;
import com.github.isuhorukov.osm.pgsnapshot.pbfextractor.ReplicationHeader;
import com.google.common.util.concurrent.MoreExecutors;
import com.uber.h3core.H3Core;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoder;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoderListener;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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

    private final CliParameters parameters;
    private final List<TagExtension> tagExtensions;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public Pipeline(CliParameters parameters) {
        this(parameters, Collections.emptyList());
    }

    public Pipeline(CliParameters parameters, List<TagExtension> tagExtensions) {
        this.parameters = parameters;
        this.tagExtensions = List.copyOf(tagExtensions);
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
        if (parameters.isUpdate()) {
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
                multipolygonTagKeys(), externalProcessing, this::recordFailure)) {
            if (updater.isUpToDate()) {
                return;
            }
            long processingStartTime = System.currentTimeMillis();
            ProcessedBlocks processedBlocks = processBlocks(externalProcessing, sourcePbfFile, null,
                    updater.blockWriter(maxBlocksInFlight(), this::recordFailure), processingStartTime,
                    updater.getStoredVersions());
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
                multipolygonTagKeys(), this::recordFailure)) {
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
                        StoredVersions.NONE);
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

    private void saveFiles(ExternalProcessing externalProcessing, File sourcePbfFile, long commandStartTime)
            throws IOException, InterruptedException {
        File resultDirectory = prepareResultDirectory(sourcePbfFile);

        boolean saveImportScripts = !parameters.isCollectOnlyStat() && parameters.isSavePostgresqlTsv();
        boolean exportMultipolygons = saveImportScripts || parameters.isSaveArrow();
        if (exportMultipolygons && !saveImportScripts) {
            ResultLayout.checkAndMakeMultipolygonDirectory(resultDirectory);
        }

        long processingStartTime = System.currentTimeMillis();
        try (DuckDbParquetWriter parquetWriter = parameters.isSaveArrow() ? DuckDbParquetWriter.open() : null;
             MultipolygonExport multipolygonExport = MultipolygonExport.start(
                () -> externalProcessing.exportMultipolygons(sourcePbfFile, resultDirectory, parquetWriter),
                exportMultipolygons && parameters.isParallelMultipolygonExport())) {
            ProcessedBlocks processedBlocks = processBlocksToFiles(externalProcessing, sourcePbfFile, resultDirectory,
                    arrowWriters(resultDirectory, parquetWriter), processingStartTime);
            long multipolygonCount = processedBlocks.multipolygonCount();
            long dataProcessingTime = System.currentTimeMillis() - processingStartTime;
            StatisticsWriter.printProcessingStatistics(processedBlocks.blockStatistics, processedBlocks.blockCount,
                    dataProcessingTime, multipolygonCount);

            MultipolygonTime multipolygonTime = new MultipolygonTime();
            if (exportMultipolygons) {
                multipolygonTime.setMultipolygonExportTime(multipolygonExport.await());
            }
            if (saveImportScripts) {
                multipolygonTime.setSplitMultipolygonByPartsTime(writeImportScripts(externalProcessing,
                        resultDirectory, processedBlocks.blockStatistics, multipolygonCount));
            } else if (parameters.isSaveArrow()) {
                deleteMultipolygonDirectory(resultDirectory);
            }

            StatisticsWriter.saveStatistics(resultDirectory, buildStatistics(processedBlocks.blockStatistics,
                    multipolygonCount, dataProcessingTime, processedBlocks.addLocationsToWaysTime, multipolygonTime,
                    System.currentTimeMillis() - commandStartTime));
            if (saveImportScripts && parameters.isUpdatable()) {
                ImportState.write(resultDirectory, importState(sourcePbfFile));
            }
        }
    }

    private byte[] importState(File sourcePbfFile) throws IOException {
        return ImportState.tsv(ImportState.LOAD, sourcePbfFile, ReplicationHeader.read(sourcePbfFile),
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
                    multipolygonTagKeys());
            DirectoryPostgresqlResources postgresqlResources = new DirectoryPostgresqlResources(resultDirectory);
            for (TagExtension tagExtension : tagExtensions) {
                tagExtension.writePostgresqlResources(postgresqlResources);
            }
        }
        return resultDirectory;
    }

    private Set<String> multipolygonTagKeys() {
        Set<String> multipolygonTagKeys = new TreeSet<>();
        for (TagExtension tagExtension : tagExtensions) {
            multipolygonTagKeys.addAll(tagExtension.multipolygonTagKeys());
        }
        return multipolygonTagKeys;
    }

    private int maxBlocksInFlight() {
        return Math.max(1, parameters.getWorkers()) * BLOCKS_IN_FLIGHT_PER_WORKER;
    }

    private ArrowWriters arrowWriters(File resultDirectory, ParquetWriter parquetWriter) {
        if (!parameters.isSaveArrow()) {
            return null;
        }
        return new ArrowWriters(new File(resultDirectory, ResultLayout.ARROW_DIR), parameters.getArrowFormat(),
                parquetWriter);
    }

    private ProcessedBlocks processBlocksToFiles(ExternalProcessing externalProcessing, File sourcePbfFile,
                                                 File resultDirectory, ArrowWriters arrowWriters,
                                                 long processingStartTime)
            throws IOException, InterruptedException {
        ExecutorService saveExecutorService = getExecutorService(parameters.getWorkers());
        try {
            return processBlocks(externalProcessing, sourcePbfFile, arrowWriters, new TsvBlockWriter(
                    saveExecutorService, resultDirectory, maxBlocksInFlight(), this::recordFailure), processingStartTime,
                    StoredVersions.NONE);
        } finally {
            saveExecutorService.shutdownNow();
        }
    }

    private ProcessedBlocks processBlocks(ExternalProcessing externalProcessing, File sourcePbfFile,
                                          ArrowWriters arrowWriters, OrderedBlockWriter blockWriter,
                                          long processingStartTime, StoredVersions storedVersions)
            throws IOException, InterruptedException {
        final H3Core h3Core = H3Core.newInstance();
        ThreadLocal<BlockProcessor> blockProcessors =
                ThreadLocal.withInitial(() -> new BlockProcessor(parameters, h3Core, tagExtensions, storedVersions));

        ExecutorService executorService = getExecutorService(parameters.getWorkers());

        Map<Long, BlockStat> blockStat = new ConcurrentHashMap<>();
        int blockCount;
        long addLocationsToWaysTime;
        try (PipedProcess addLocationsToWays = externalProcessing.startAddLocationsToWays(sourcePbfFile,
                parameters.isPreserveAllNodes())) {
            blockCount = Splitter.splitPbfByBlocks(addLocationsToWays.getInputStream(), (rawBlob, blockNumber) ->
                    submitBlock(executorService, blockWriter, blockNumber, () -> processBlock(rawBlob,
                            blockNumber, blockProcessors, arrowWriters, blockStat, blockWriter)));
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
                parameters.isUpdatable());
        ResourceCopier.copyImportScripts(resultDirectory, parameters.isColumnarStorage(), parameters.isUpdatable());
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
                              ArrowWriters arrowWriters, Map<Long, BlockStat> blockStat, OrderedBlockWriter blockWriter) {
        long threadStart = System.currentTimeMillis();
        if (blockNumber % 1000 == 0) {
            log.info("{}", blockNumber);
        }
        EncodedBlock encodedBlock = EncodedBlock.EMPTY;
        try {
            BlockResult result = decodeBlock(rawBlob, blockNumber, blockProcessors.get(), threadStart);
            blockStat.put(blockNumber, result.getBlockStat());
            if (!parameters.isCollectOnlyStat()) {
                encodedBlock = persistBlock(result, arrowWriters);
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

    private EncodedBlock persistBlock(BlockResult result, ArrowWriters arrowWriters) {
        BlockStat blockStatistic = result.getBlockStat();
        if (arrowWriters != null) {
            long startSaveTime = System.currentTimeMillis();
            arrowWriters.write(result, blockStatistic.getId());
            blockStatistic.setSaveTime(System.currentTimeMillis() - startSaveTime);
        }
        if (!parameters.isSavePostgresqlTsv()) {
            return EncodedBlock.EMPTY;
        }
        long startEncodeTime = System.currentTimeMillis();
        BlockType blockType = BlockType.of(result.getNodeCount(), result.getWayCount(), result.getRelationCount());
        EncodedBlock encodedBlock = EncodedBlock.encode(blockType, result.getCsvResultPerH33(), blockStatistic);
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

    private static void deleteMultipolygonDirectory(File resultDirectory) {
        File multipolygonDirectory = new File(resultDirectory, ResultLayout.MULTIPOLYGON_DIR);
        File[] multipolygonFiles = multipolygonDirectory.listFiles();
        if (multipolygonFiles != null) {
            for (File f : multipolygonFiles) {
                try {
                    Files.delete(f.toPath());
                } catch (IOException e) {
                    log.warn("Failed to delete: {}", f, e);
                }
            }
        }
        try {
            Files.delete(multipolygonDirectory.toPath());
        } catch (IOException e) {
            log.warn("Failed to delete directory: {}", multipolygonDirectory, e);
        }
    }

    private static ExecutorService getExecutorService(int workers) {
        if (workers > 0) {
            return Executors.newFixedThreadPool(workers);
        }
        return MoreExecutors.newDirectExecutorService();
    }

    private static final class ArrowWriters {
        private final NodeWayArrowWriter nodeWayArrowWriter;
        private final RelationArrowWriter relationArrowWriter;

        private ArrowWriters(File arrowDirectory, ArrowFormat arrowFormat, ParquetWriter parquetWriter) {
            nodeWayArrowWriter = new NodeWayArrowWriter(arrowDirectory, arrowFormat, parquetWriter);
            relationArrowWriter = new RelationArrowWriter(arrowDirectory, arrowFormat, parquetWriter);
        }

        private void write(BlockResult result, long blockNumber) {
            if (!result.getArrowNodeOrWays().isEmpty()) {
                nodeWayArrowWriter.write(result.getArrowNodeOrWays(), blockNumber);
            }
            if (!result.getArrowRelations().isEmpty()) {
                relationArrowWriter.write(result.getArrowRelations(), blockNumber);
            }
        }
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
