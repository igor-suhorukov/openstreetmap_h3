package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.ExternalProcessing;
import com.github.isuhorukov.osm.pgsnapshot.Splitter;
import com.github.isuhorukov.osm.pgsnapshot.arrow.NodeWayArrowWriter;
import com.github.isuhorukov.osm.pgsnapshot.arrow.RelationArrowWriter;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.MultipolygonTime;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import com.github.isuhorukov.osm.pgsnapshot.output.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.uber.h3core.H3Core;
import org.apache.commons.io.IOUtils;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoder;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoderListener;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Pipeline {

    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

    private final CliParameters parameters;

    public Pipeline(CliParameters parameters) {
        this.parameters = parameters;
    }

    public void run() throws Exception {
        long commandStartTime = System.currentTimeMillis();

        File sourcePbfFile = new File(parameters.getSourceFilePath());
        if (!sourcePbfFile.exists() || sourcePbfFile.length() == 0) {
            throw new IllegalArgumentException("Input pbf should exists and should be non empty");
        }

        log.info("{}", parameters);
        ExternalProcessing externalProcessing = new ExternalProcessing(parameters.isInvokeDockerCommand());
        Splitter.Blocks blocks = externalProcessing.enrichSourcePbfAndSplitIt(sourcePbfFile, parameters.isPreserveAllNodes());

        File inputDirectory = new File(blocks.getDirectory());
        File[] files = inputDirectory.listFiles();
        Arrays.sort(Objects.requireNonNull(files));
        File resultDirectory = ResultLayout.prepareResultDirectories(
                new File(inputDirectory.getParent(), ResultLayout.resultDirectoryNameFromSource(inputDirectory)),
                parameters.isSavePostgresqlTsv(), parameters.isSaveArrow());

        ResourceCopier.copyOsmiumSettings(resultDirectory);
        if (parameters.isSavePostgresqlTsv()) {
            ResourceCopier.copyResources(resultDirectory, parameters.isColumnarStorage());
        }

        long processingStartTime = System.currentTimeMillis();
        final H3Core h3Core = H3Core.newInstance();

        ExecutorService saveExecutorService = getExecutorService(parameters.getWorkers());
        ExecutorService executorService = getExecutorService(parameters.getWorkers());

        Map<Long, BlockStat> blockStat = new ConcurrentHashMap<>();
        AtomicInteger currentBlockToSave = new AtomicInteger(0);
        for (File blockFile : files) {
            executorService.submit(() -> processBlockFile(blockFile, h3Core, resultDirectory,
                    blockStat, currentBlockToSave, saveExecutorService));
        }
        executorService.shutdown();
        executorService.awaitTermination(2, TimeUnit.DAYS);
        saveExecutorService.shutdown();

        List<BlockStat> blockStatistics = new ArrayList<>(blockStat.values());
        long multipolygonCount = blockStatistics.stream().map(BlockStat::getMultipolygonCount).mapToLong(Long::longValue).sum();
        long dataProcessingTime = System.currentTimeMillis() - processingStartTime;
        StatisticsWriter.printProcessingStatistics(blockStatistics, files.length, dataProcessingTime, multipolygonCount);

        if (!parameters.isCollectOnlyStat() && parameters.isSavePostgresqlTsv()) {
            PartitioningScriptWriter.savePartitioningScripts(resultDirectory, parameters.getScriptCount(),
                    parameters.getThresholdPercentFromMaxPartition(), blockStatistics, parameters.isColumnarStorage());
        }

        MultipolygonTime multipolygonTime = runMultipolygonPostProcessing(externalProcessing, sourcePbfFile,
                resultDirectory, multipolygonCount);

        PbfStatistics statistics = new PbfStatistics(blockStatistics);
        statistics.setMultipolygonCount(multipolygonCount);
        statistics.setDataProcessingTime(dataProcessingTime);
        statistics.setAddLocationsToWaysTime(blocks.getAddLocationsToWaysTime());
        statistics.setPbfSplitTime(blocks.getPbfSplitTime());
        statistics.setMultipolygonExportTime(multipolygonTime.getMultipolygonExportTime());
        statistics.setSplitMultipolygonByPartsTime(multipolygonTime.getSplitMultipolygonByPartsTime());
        statistics.setTotalTime(System.currentTimeMillis() - commandStartTime);

        StatisticsWriter.saveStatistics(resultDirectory, statistics);
    }

    private void processBlockFile(File blockFile, H3Core h3Core, File resultDirectory,
                                   Map<Long, BlockStat> blockStat,
                                   AtomicInteger currentBlockToSave, ExecutorService saveExecutorService) {
        long threadStart = System.currentTimeMillis();
        Long blockNumber = Long.parseLong(blockFile.getName());
        if (blockNumber % 1000 == 0) {
            log.info("{}", blockNumber);
        }
        RawBlob rawBlob;
        try (FileInputStream blobInputStream = new FileInputStream(blockFile)) {
            rawBlob = new RawBlob("OSMData", IOUtils.toByteArray(blobInputStream));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        BlockProcessor blockProcessor = new BlockProcessor(parameters, h3Core);
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

    private MultipolygonTime runMultipolygonPostProcessing(ExternalProcessing externalProcessing,
                                                            File sourcePbfFile, File resultDirectory,
                                                            long multipolygonCount)
            throws IOException, InterruptedException {
        if (!parameters.isCollectOnlyStat() && parameters.isSavePostgresqlTsv()) {
            return externalProcessing.prepareMultipolygonDataAndScripts(sourcePbfFile,
                    resultDirectory, parameters.getScriptCount(), multipolygonCount, parameters.isSaveArrow());
        }
        if (parameters.isSaveArrow()) {
            String resultDirName = resultDirectory.getName();
            String basePath = resultDirectory.getParent();
            String indexType = ExternalProcessing.getIndexType(sourcePbfFile);
            File multipolygonDirectory = ResultLayout.checkAndMakeMultipolygonDirectory(resultDirectory);
            externalProcessing.executeMultipolygonExport(sourcePbfFile, resultDirName, basePath, indexType);
            ExternalProcessing.transformMultipolygonToParquet(resultDirectory);
            File[] multipolygonFiles = multipolygonDirectory.listFiles();
            if (multipolygonFiles != null) {
                for (File f : multipolygonFiles) {
                    if (!f.delete()) {
                        log.warn("Failed to delete: {}", f);
                    }
                }
            }
            if (!multipolygonDirectory.delete()) {
                log.warn("Failed to delete directory: {}", multipolygonDirectory);
            }
        }
        return new MultipolygonTime();
    }

    private static ExecutorService getExecutorService(int workers) {
        if (workers > 0) {
            return Executors.newFixedThreadPool(workers);
        }
        return MoreExecutors.newDirectExecutorService();
    }
}
