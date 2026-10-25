package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.Serializer;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StatisticsWriter {

    private static final Logger log = LoggerFactory.getLogger(StatisticsWriter.class);

    public static final String OSM_FILE_STATISTICS_TSV = "osm_file_statistics.tsv";
    public static final String OSM_FILE_BLOCK_TSV = "osm_file_block.tsv";
    public static final String OSM_FILE_BLOCK_CONTENT_TSV = "osm_file_block_content.tsv";

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
        for (Map.Entry<String, byte[]> statisticsFile : statisticsFiles(statistics).entrySet()) {
            writeStatFile(currentBlockTypeDir, statisticsFile.getValue(), statisticsFile.getKey());
        }
    }

    public static Map<String, byte[]> statisticsFiles(PbfStatistics statistics) {
        Map<String, byte[]> statisticsFiles = new LinkedHashMap<>();
        Serializer serializer = new Serializer();
        StringBuilder csvString = new StringBuilder();
        serializer.serializePbfStat(csvString, statistics);
        statisticsFiles.put(OSM_FILE_STATISTICS_TSV, utf8(csvString));
        csvString.setLength(0);
        serializer.serializeBlockStat(csvString, statistics.getBlockStatistics());
        statisticsFiles.put(OSM_FILE_BLOCK_TSV, utf8(csvString));
        csvString.setLength(0);
        for (BlockStat blockStatistics : statistics.getBlockStatistics()) {
            serializer.serializeBlockContent(csvString, blockStatistics);
        }
        statisticsFiles.put(OSM_FILE_BLOCK_CONTENT_TSV, utf8(csvString));
        return statisticsFiles;
    }

    private static byte[] utf8(StringBuilder csvString) {
        return csvString.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void writeStatFile(File currentBlockTypeDir, byte[] content, String statName) {
        try (OutputStream dataExport =
                     new FileOutputStream(new File(currentBlockTypeDir, statName), true)) {
            dataExport.write(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
