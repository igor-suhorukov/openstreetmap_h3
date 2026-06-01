package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.Serializer;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class StatisticsWriter {

    private static final Logger log = LoggerFactory.getLogger(StatisticsWriter.class);

    private StatisticsWriter() {
    }

    public static void printProcessingStatistics(List<BlockStat> blockStatistics, int fileCount,
                                                 long dataProcessingTime, long multipolygonCount) {
        log.info("{}  time {}", fileCount, dataProcessingTime);
        log.info("diff between total and processing {}", blockStatistics.stream().map(b -> b.getThreadTime() - b.getProcessingTime()).mapToLong(Long::longValue).sum());
        log.info("total thread time {}", blockStatistics.stream().map(BlockStat::getThreadTime).mapToLong(Long::longValue).sum());
        log.info("total processing time {}", blockStatistics.stream().map(BlockStat::getProcessingTime).mapToLong(Long::longValue).sum());
        log.info("total save time {}", blockStatistics.stream().map(BlockStat::getSaveTime).mapToLong(Long::longValue).sum());
        log.info("total waiting for save time {}", blockStatistics.stream().map(BlockStat::getWaitingForSaveTime).mapToLong(Long::longValue).sum());
        log.info("thread max time {}", blockStatistics.stream().map(BlockStat::getThreadTime).mapToLong(Long::longValue).max().orElse(0));
        log.info("processing max time {}", blockStatistics.stream().map(BlockStat::getProcessingTime).mapToLong(Long::longValue).max().orElse(0));
        log.info("nodes {}", blockStatistics.stream().map(BlockStat::getNodeCount).mapToLong(Long::longValue).sum());
        log.info("ways {}", blockStatistics.stream().map(BlockStat::getWayCount).mapToLong(Long::longValue).sum());
        log.info("relations {}", blockStatistics.stream().map(BlockStat::getRelationCount).mapToLong(Long::longValue).sum());
        log.info("relation members {}", blockStatistics.stream().map(BlockStat::getRelationMembersCount).mapToLong(Long::longValue).sum());
        log.info("multipolygon count {}", multipolygonCount);
    }

    public static void saveStatistics(File resultDirectory, PbfStatistics statistics) {
        File currentBlockTypeDir = new File(resultDirectory, ResultLayout.IMPORT_RELATED_METADATA_DIR);
        savePbfStatistics(statistics, currentBlockTypeDir);
        savePbfBlockStatistic(statistics, currentBlockTypeDir);
        savePbfBlockDataByHashStatistics(statistics, currentBlockTypeDir);
    }

    private static void savePbfBlockDataByHashStatistics(PbfStatistics pbfStatistics, File currentBlockTypeDir) {
        Serializer serializer = new Serializer();
        StringBuilder csvString = new StringBuilder();
        for (BlockStat blockStatistics : pbfStatistics.getBlockStatistics()) {
            serializer.serializeBlockContent(csvString, blockStatistics);
            if (csvString.length() > 0) {
                writeStatFile(currentBlockTypeDir, csvString, "osm_file_block_content.tsv");
            }
            csvString.setLength(0);
        }
    }

    private static void savePbfBlockStatistic(PbfStatistics pbfStatistics, File currentBlockTypeDir) {
        StringBuilder csvString = new StringBuilder();
        new Serializer().serializeBlockStat(csvString, pbfStatistics.getBlockStatistics());
        writeStatFile(currentBlockTypeDir, csvString, "osm_file_block.tsv");
    }

    private static void savePbfStatistics(PbfStatistics pbfStatistics, File currentBlockTypeDir) {
        StringBuilder csvString = new StringBuilder();
        new Serializer().serializePbfStat(csvString, pbfStatistics);
        writeStatFile(currentBlockTypeDir, csvString, "osm_file_statistics.tsv");
    }

    private static void writeStatFile(File currentBlockTypeDir, StringBuilder csvString, String statName) {
        try (OutputStream dataExport =
                     new FileOutputStream(new File(currentBlockTypeDir, statName), true)) {
            IOUtils.write(csvString, dataExport, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
