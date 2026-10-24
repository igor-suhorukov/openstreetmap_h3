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
import com.github.isuhorukov.osm.pgsnapshot.util.LimitedQueue;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Pipeline {

    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

    private final CliParameters parameters;
    private final List<TagExtension> tagExtensions;

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
        for (TagExtension tagExtension : tagExtensions) {
            log.info("Tag extension {}", tagExtension.getName());
            tagExtension.init();
        }
        ExternalProcessing externalProcessing = new ExternalProcessing(parameters.isInvokeDockerCommand());

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

        long processingStartTime = System.currentTimeMillis();
        final H3Core h3Core = H3Core.newInstance();

        ExecutorService saveExecutorService = getExecutorService(parameters.getWorkers());
        ExecutorService executorService = getBlockingExecutorService(parameters.getWorkers());

        Map<Long, BlockStat> blockStat = new ConcurrentHashMap<>();
        AtomicInteger currentBlockToSave = new AtomicInteger(0);
        int blockCount;
        long addLocationsToWaysTime;
        try (PipedProcess addLocationsToWays = externalProcessing.startAddLocationsToWays(sourcePbfFile,
                parameters.isPreserveAllNodes())) {
            blockCount = Splitter.splitPbfByBlocks(addLocationsToWays.getInputStream(), (rawBlob, blockNumber) ->
                    executorService.submit(() -> processBlock(rawBlob, blockNumber, h3Core, resultDirectory,
                            blockStat, currentBlockToSave, saveExecutorService)));
            addLocationsToWays.waitForSuccess();
            addLocationsToWaysTime = System.currentTimeMillis() - processingStartTime;
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.DAYS);
        } finally {
            executorService.shutdownNow();
            saveExecutorService.shutdownNow();
        }

        List<BlockStat> blockStatistics = new ArrayList<>(blockStat.values());
        long multipolygonCount = blockStatistics.stream().map(BlockStat::getMultipolygonCount).mapToLong(Long::longValue).sum();
        long dataProcessingTime = System.currentTimeMillis() - processingStartTime;
        StatisticsWriter.printProcessingStatistics(blockStatistics, blockCount, dataProcessingTime, multipolygonCount);

        if (!parameters.isCollectOnlyStat() && parameters.isSavePostgresqlTsv()) {
            PartitioningScriptWriter.savePartitioningScripts(resultDirectory, parameters.getScriptCount(),
                    parameters.getThresholdPercentFromMaxPartition(), blockStatistics, parameters.isColumnarStorage());
        }

        MultipolygonTime multipolygonTime = new MultipolygonTime();
        if (!parameters.isCollectOnlyStat() && parameters.isSavePostgresqlTsv()) {
            multipolygonTime = externalProcessing.prepareMultipolygonDataAndScripts(sourcePbfFile,
                    resultDirectory, parameters.getScriptCount(), multipolygonCount, parameters.isSaveArrow());
        } else if (parameters.isSaveArrow()) {
            cleanupArrowMultipolygons(externalProcessing, sourcePbfFile, resultDirectory);
        }

        PbfStatistics statistics = buildStatistics(blockStatistics, multipolygonCount, dataProcessingTime,
                addLocationsToWaysTime, multipolygonTime, System.currentTimeMillis() - commandStartTime);

        StatisticsWriter.saveStatistics(resultDirectory, statistics);
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

    private void processBlock(RawBlob rawBlob, Long blockNumber, H3Core h3Core, File resultDirectory,
                              Map<Long, BlockStat> blockStat,
                              AtomicInteger currentBlockToSave, ExecutorService saveExecutorService) {
        long threadStart = System.currentTimeMillis();
        if (blockNumber % 1000 == 0) {
            log.info("{}", blockNumber);
        }

        BlockProcessor blockProcessor = new BlockProcessor(parameters, h3Core, tagExtensions);
        TsvBlockWriter tsvBlockWriter = new TsvBlockWriter(currentBlockToSave, saveExecutorService, resultDirectory);

        PbfBlobDecoder blobDecoder = new PbfBlobDecoder(rawBlob, new PbfBlobDecoderListener() {
            @Override
            public void complete(List<EntityContainer> decodedEntities) {
                long blockStartTime = System.currentTimeMillis();
                BlockResult result = blockProcessor.process(decodedEntities, blockNumber, threadStart);
                result.getBlockStat().setProcessingTime(System.currentTimeMillis() - blockStartTime);
                blockStat.put(blockNumber, result.getBlockStat());
                if (!parameters.isCollectOnlyStat()) {
                    persistBlock(blockNumber, result, resultDirectory, tsvBlockWriter);
                }
            }

            @Override
            public void error() {
                log.error("ERROR in block {}", blockNumber);
            }
        });
        blobDecoder.run();
        blockStat.get(blockNumber).setThreadTime(System.currentTimeMillis() - threadStart);
    }

    private void persistBlock(Long blockNumber, BlockResult result, File resultDirectory,
                              TsvBlockWriter tsvBlockWriter) {
        BlockStat blockStatistic = result.getBlockStat();
        if (parameters.isSaveArrow()) {
            long startSaveTime = System.currentTimeMillis();
            File arrowDir = new File(resultDirectory, ResultLayout.ARROW_DIR);
            if (!result.getArrowNodeOrWays().isEmpty()) {
                new NodeWayArrowWriter(arrowDir, parameters.getArrowFormat()).write(result.getArrowNodeOrWays(), blockNumber);
            }
            if (!result.getArrowRelations().isEmpty()) {
                new RelationArrowWriter(arrowDir, parameters.getArrowFormat()).write(result.getArrowRelations(), blockNumber);
            }
            blockStatistic.setSaveTime(System.currentTimeMillis() - startSaveTime);
        }
        if (parameters.isSavePostgresqlTsv()) {
            tsvBlockWriter.saveDataOnlyInOneThread(result.getCsvResultPerH33(),
                    result.getNodeCount(), result.getWayCount(),
                    blockStatistic, result.getRelationCount(), blockNumber);
        }
    }

    private void cleanupArrowMultipolygons(ExternalProcessing externalProcessing,
                                           File sourcePbfFile, File resultDirectory)
            throws IOException, InterruptedException {
        String resultDirName = resultDirectory.getName();
        String basePath = resultDirectory.getParent();
        String indexType = ExternalProcessing.getIndexType(sourcePbfFile);
        File multipolygonDirectory = ResultLayout.checkAndMakeMultipolygonDirectory(resultDirectory);
        externalProcessing.executeMultipolygonExport(sourcePbfFile, resultDirName, basePath, indexType);
        ExternalProcessing.transformMultipolygonToParquet(resultDirectory);
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

    private static ExecutorService getBlockingExecutorService(int workers) {
        if (workers > 0) {
            return new ThreadPoolExecutor(workers, workers, 0L, TimeUnit.MILLISECONDS, new LimitedQueue<>(workers));
        }
        return MoreExecutors.newDirectExecutorService();
    }
}
