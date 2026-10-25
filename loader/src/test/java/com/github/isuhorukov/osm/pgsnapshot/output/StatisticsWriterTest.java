package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.Stat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StatisticsWriterTest {

    @TempDir
    Path tempDir;

    private static PbfStatistics statistics() {
        BlockStat nodeBlock = new BlockStat(0);
        nodeBlock.setNodeCount(2);
        Stat stat = new Stat((short) 25764);
        stat.incrementCount();
        stat.incrementCount();
        stat.incrementSize(52);
        stat.updateIdStat(10);
        stat.updateIdStat(20);
        stat.updateLastModified(1000);
        nodeBlock.setNodeStat(Map.of((short) 25764, stat));
        BlockStat relationBlock = new BlockStat(1);
        relationBlock.setRelationCount(1);
        relationBlock.setMultipolygonCount(1);
        PbfStatistics statistics = new PbfStatistics(List.of(nodeBlock, relationBlock));
        statistics.setMultipolygonCount(1);
        statistics.setDataProcessingTime(5);
        statistics.setAddLocationsToWaysTime(4);
        statistics.setMultipolygonExportTime(3);
        statistics.setSplitMultipolygonByPartsTime(2);
        statistics.setTotalTime(9);
        return statistics;
    }

    private static String utf8(byte[] content) {
        return new String(content, StandardCharsets.UTF_8);
    }

    @Test
    void statisticsFiles_tsvContentForPostgresqlCopy() {
        Map<String, byte[]> files = StatisticsWriter.statisticsFiles(statistics());

        assertEquals(List.of(StatisticsWriter.OSM_FILE_STATISTICS_TSV, StatisticsWriter.OSM_FILE_BLOCK_TSV,
                StatisticsWriter.OSM_FILE_BLOCK_CONTENT_TSV), List.copyOf(files.keySet()));
        assertEquals("1\t5\t0\t4\t3\t2\t9\n", utf8(files.get(StatisticsWriter.OSM_FILE_STATISTICS_TSV)));
        assertEquals("0\t2\t0\t0\t0\t0\t0\t0\t0\t0\t0\n1\t0\t0\t1\t0\t1\t0\t0\t0\t0\t0\n",
                utf8(files.get(StatisticsWriter.OSM_FILE_BLOCK_TSV)));
        assertEquals("N\t25764\t25764\t2\t52\t10\t20\t1000\n", utf8(files.get(StatisticsWriter.OSM_FILE_BLOCK_CONTENT_TSV)));
    }

    @Test
    void saveStatistics_filesInImportRelatedMetadata() throws Exception {
        File resultDirectory = tempDir.toFile();
        ResultLayout.prepareResultDirectories(resultDirectory, false, false);
        PbfStatistics statistics = statistics();

        StatisticsWriter.saveStatistics(resultDirectory, statistics);

        for (Map.Entry<String, byte[]> file : StatisticsWriter.statisticsFiles(statistics).entrySet()) {
            assertArrayEquals(file.getValue(), Files.readAllBytes(
                    tempDir.resolve(ResultLayout.IMPORT_RELATED_METADATA_DIR).resolve(file.getKey())), file.getKey());
        }
    }
}
