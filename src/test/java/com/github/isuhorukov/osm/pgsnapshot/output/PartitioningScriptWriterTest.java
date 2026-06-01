package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.Stat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PartitioningScriptWriterTest {

    @TempDir
    Path tempDir;

    private File prepareResultDir() {
        File resultDir = tempDir.toFile();
        ResultLayout.prepareResultDirectories(resultDir, true, false);
        return resultDir;
    }

    private BlockStat blockWithNodes(long id, Map<Short, Stat> nodeStat) {
        BlockStat b = new BlockStat(id);
        b.setNodeCount(10);
        b.setNodeStat(nodeStat);
        return b;
    }

    private BlockStat blockWithWays(long id, Map<Short, Stat> wayStat) {
        BlockStat b = new BlockStat(id);
        b.setWayCount(10);
        b.setWayStat(wayStat);
        return b;
    }

    private Stat singleStat(short h33) {
        Stat s = new Stat(h33);
        s.incrementCount();
        s.incrementSize(1000000);
        return s;
    }

    @Test
    void savePartitioningScripts_relationBlock_isSkipped() {
        File resultDir = prepareResultDir();

        BlockStat relBlock = new BlockStat(0L);
        relBlock.setRelationCount(5);

        // must include a way block so distributeH33ByPartitionsForWays doesn't fail
        BlockStat wayBlock = blockWithWays(1L, Collections.singletonMap((short) 100, singleStat((short) 100)));

        // relation block should be silently skipped — no exception
        assertDoesNotThrow(() -> PartitioningScriptWriter.savePartitioningScripts(
                resultDir, 1, 0.48, List.of(relBlock, wayBlock), false));
    }

    @Test
    void savePartitioningScripts_mixedNodeAndWayBlock_throwsIllegalState() {
        File resultDir = prepareResultDir();

        BlockStat mixedBlock = new BlockStat(0L);
        mixedBlock.setNodeCount(5);
        mixedBlock.setWayCount(5);

        assertThrows(IllegalStateException.class, () ->
                PartitioningScriptWriter.savePartitioningScripts(
                        resultDir, 1, 0.48, List.of(mixedBlock), false));
    }

    @Test
    void savePartitioningScripts_nodeBlockWithNullStat_isSkipped() {
        File resultDir = prepareResultDir();

        BlockStat nodeBlockNullStat = new BlockStat(0L);
        nodeBlockNullStat.setNodeCount(5);
        // nodeStat is null — should be skipped without exception

        // must include a way block so distributeH33ByPartitionsForWays doesn't fail
        BlockStat wayBlock = blockWithWays(1L, Collections.singletonMap((short) 100, singleStat((short) 100)));

        assertDoesNotThrow(() -> PartitioningScriptWriter.savePartitioningScripts(
                resultDir, 1, 0.48, List.of(nodeBlockNullStat, wayBlock), false));
    }

    @Test
    void savePartitioningScripts_wayBlockWithNullStat_isSkipped() {
        File resultDir = prepareResultDir();

        // wayStat is null — should be skipped; the non-null way block provides the required stats
        BlockStat wayBlockNullStat = new BlockStat(0L);
        wayBlockNullStat.setWayCount(5);

        BlockStat wayBlock = blockWithWays(1L, Collections.singletonMap((short) 100, singleStat((short) 100)));

        assertDoesNotThrow(() -> PartitioningScriptWriter.savePartitioningScripts(
                resultDir, 1, 0.48, List.of(wayBlockNullStat, wayBlock), false));
    }

    @Test
    void savePartitioningScripts_blockWithNoContent_throwsIllegalState() {
        File resultDir = prepareResultDir();

        BlockStat emptyBlock = new BlockStat(0L);
        // nodeCount=0, wayCount=0, relationCount=0 — should throw

        assertThrows(IllegalStateException.class, () ->
                PartitioningScriptWriter.savePartitioningScripts(
                        resultDir, 1, 0.48, List.of(emptyBlock), false));
    }

    @Test
    void savePartitioningScripts_nodeBlocks_createsSqlScripts() {
        File resultDir = prepareResultDir();

        Map<Short, Stat> nodeStat = new HashMap<>();
        nodeStat.put((short) 100, singleStat((short) 100));
        nodeStat.put((short) 200, singleStat((short) 200));
        BlockStat nodeBlock = blockWithNodes(0L, nodeStat);

        // must include a way block so distributeH33ByPartitionsForWays doesn't fail
        BlockStat wayBlock = blockWithWays(1L, Collections.singletonMap((short) 100, singleStat((short) 100)));

        assertDoesNotThrow(() -> PartitioningScriptWriter.savePartitioningScripts(
                resultDir, 1, 0.48, List.of(nodeBlock, wayBlock), false));

        File[] sqlFiles = new File(resultDir, ResultLayout.SQL_DIR).listFiles(
                (d, name) -> name.startsWith("nodes_import_"));
        assertNotNull(sqlFiles);
        assertTrue(sqlFiles.length > 0);
    }

    @Test
    void savePartitioningScripts_wayBlocks_createsSqlScripts() {
        File resultDir = prepareResultDir();

        Map<Short, Stat> wayStat = new HashMap<>();
        wayStat.put((short) 100, singleStat((short) 100));
        BlockStat wayBlock = blockWithWays(0L, wayStat);

        assertDoesNotThrow(() -> PartitioningScriptWriter.savePartitioningScripts(
                resultDir, 1, 0.48, List.of(wayBlock), false));

        File[] sqlFiles = new File(resultDir, ResultLayout.SQL_DIR).listFiles(
                (d, name) -> name.startsWith("ways_import_"));
        assertNotNull(sqlFiles);
        assertTrue(sqlFiles.length > 0);
    }

    @Test
    void savePartitioningScripts_columnar_scriptContainsColumnarKeyword() {
        File resultDir = prepareResultDir();

        Map<Short, Stat> wayStat = new HashMap<>();
        wayStat.put((short) 100, singleStat((short) 100));
        BlockStat wayBlock = blockWithWays(0L, wayStat);

        assertDoesNotThrow(() -> PartitioningScriptWriter.savePartitioningScripts(
                resultDir, 1, 0.48, List.of(wayBlock), true));
    }
}
