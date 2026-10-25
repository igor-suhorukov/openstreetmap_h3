package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.ExternalProcessing;
import com.github.isuhorukov.osm.pgsnapshot.PipedProcess;
import com.github.isuhorukov.osm.pgsnapshot.Splitter;
import com.github.isuhorukov.osm.pgsnapshot.arrow.NodeWayArrowWriter;
import com.github.isuhorukov.osm.pgsnapshot.arrow.RelationArrowWriter;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.MultipolygonTime;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import com.github.isuhorukov.osm.pgsnapshot.output.*;
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
        File resultDirectory = prepareResultDirectory(sourcePbfFile);

        boolean saveImportScripts = !parameters.isCollectOnlyStat() && parameters.isSavePostgresqlTsv();
        boolean exportMultipolygons = saveImportScripts || parameters.isSaveArrow();
        if (exportMultipolygons && !saveImportScripts) {
            ResultLayout.checkAndMakeMultipolygonDirectory(resultDirectory);
        }

        long processingStartTime = System.currentTimeMillis();
        try (MultipolygonExport multipolygonExport = MultipolygonExport.start(
                () -> externalProcessing.exportMultipolygons(sourcePbfFile, resultDirectory, parameters.isSaveArrow()),
                exportMultipolygons && parameters.isParallelMultipolygonExport())) {
            ProcessedBlocks processedBlocks = processBlocks(externalProcessing, sourcePbfFile, resultDirectory,
                    processingStartTime);
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
        }
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
            Set<String> multipolygonTagKeys = new TreeSet<>();
            for (TagExtension tagExtension : tagExtensions) {
                multipolygonTagKeys.addAll(tagExtension.multipolygonTagKeys());
            }
            ResourceCopier.copyResources(resultDirectory, parameters.isColumnarStorage(), multipolygonTagKeys);
            File staticDirectory = new File(resultDirectory, ResultLayout.STATIC_DIR);
            File sqlDirectory = new File(resultDirectory, ResultLayout.SQL_DIR);
            for (TagExtension tagExtension : tagExtensions) {
                tagExtension.writePostgresqlResources(staticDirectory, sqlDirectory);
            }
        }
        return resultDirectory;
    }

    private ProcessedBlocks processBlocks(ExternalProcessing externalProcessing, File sourcePbfFile,
                                          File resultDirectory, long processingStartTime)
            throws IOException, InterruptedException {
        final H3Core h3Core = H3Core.newInstance();
        ThreadLocal<BlockProcessor> blockProcessors =
                ThreadLocal.withInitial(() -> new BlockProcessor(parameters, h3Core, tagExtensions));

        ExecutorService saveExecutorService = getExecutorService(parameters.getWorkers());
        ExecutorService executorService = getExecutorService(parameters.getWorkers());
        TsvBlockWriter tsvBlockWriter = new TsvBlockWriter(saveExecutorService,
                Math.max(1, parameters.getWorkers()) * BLOCKS_IN_FLIGHT_PER_WORKER, this::recordFailure);

        Map<Long, BlockStat> blockStat = new ConcurrentHashMap<>();
        int blockCount;
        long addLocationsToWaysTime;
        try (PipedProcess addLocationsToWays = externalProcessing.startAddLocationsToWays(sourcePbfFile,
                parameters.isPreserveAllNodes())) {
            blockCount = Splitter.splitPbfByBlocks(addLocationsToWays.getInputStream(), (rawBlob, blockNumber) ->
                    submitBlock(executorService, tsvBlockWriter, blockNumber, () -> processBlock(rawBlob,
                            blockNumber, blockProcessors, resultDirectory, blockStat, tsvBlockWriter)));
            addLocationsToWays.waitForSuccess();
            addLocationsToWaysTime = System.currentTimeMillis() - processingStartTime;
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.DAYS);
        } finally {
            executorService.shutdownNow();
            saveExecutorService.shutdownNow();
        }
        throwIfFailed();
        return new ProcessedBlocks(new ArrayList<>(blockStat.values()), blockCount, addLocationsToWaysTime);
    }

    private long writeImportScripts(ExternalProcessing externalProcessing, File resultDirectory,
                                    List<BlockStat> blockStatistics, long multipolygonCount)
            throws IOException, InterruptedException {
        PartitioningScriptWriter.savePartitioningScripts(resultDirectory, parameters.getScriptCount(),
                parameters.getThresholdPercentFromMaxPartition(), blockStatistics, parameters.isColumnarStorage());
        ResourceCopier.copyImportScripts(resultDirectory, parameters.isColumnarStorage());
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

    private void submitBlock(ExecutorService executorService, TsvBlockWriter tsvBlockWriter, long blockNumber,
                             Runnable block) {
        throwIfFailed();
        try {
            tsvBlockWriter.reserve();
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
                              File resultDirectory, Map<Long, BlockStat> blockStat, TsvBlockWriter tsvBlockWriter) {
        long threadStart = System.currentTimeMillis();
        if (blockNumber % 1000 == 0) {
            log.info("{}", blockNumber);
        }
        EncodedBlock encodedBlock = EncodedBlock.EMPTY;
        try {
            BlockResult result = decodeBlock(rawBlob, blockNumber, blockProcessors.get(), threadStart);
            blockStat.put(blockNumber, result.getBlockStat());
            if (!parameters.isCollectOnlyStat()) {
                encodedBlock = persistBlock(result, resultDirectory);
            }
            result.getBlockStat().setThreadTime(System.currentTimeMillis() - threadStart);
        } finally {
            tsvBlockWriter.complete(blockNumber, encodedBlock);
        }
    }

    private static BlockResult decodeBlock(RawBlob rawBlob, long blockNumber, BlockProcessor blockProcessor,
                                           long threadStart) {
        BlockListener blockListener = new BlockListener(blockProcessor, blockNumber, threadStart);
        new PbfBlobDecoder(rawBlob, blockListener).run();
        return blockListener.getResult();
    }

    private EncodedBlock persistBlock(BlockResult result, File resultDirectory) {
        BlockStat blockStatistic = result.getBlockStat();
        if (parameters.isSaveArrow()) {
            long startSaveTime = System.currentTimeMillis();
            File arrowDir = new File(resultDirectory, ResultLayout.ARROW_DIR);
            if (!result.getArrowNodeOrWays().isEmpty()) {
                new NodeWayArrowWriter(arrowDir, parameters.getArrowFormat()).write(result.getArrowNodeOrWays(), blockStatistic.getId());
            }
            if (!result.getArrowRelations().isEmpty()) {
                new RelationArrowWriter(arrowDir, parameters.getArrowFormat()).write(result.getArrowRelations(), blockStatistic.getId());
            }
            blockStatistic.setSaveTime(System.currentTimeMillis() - startSaveTime);
        }
        if (!parameters.isSavePostgresqlTsv()) {
            return EncodedBlock.EMPTY;
        }
        long startEncodeTime = System.currentTimeMillis();
        File blockDirectory = ResultLayout.blockResultDirectory(result.getNodeCount(), result.getWayCount(),
                result.getRelationCount(), resultDirectory);
        EncodedBlock encodedBlock = EncodedBlock.encode(blockDirectory, result.getCsvResultPerH33(), blockStatistic);
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
