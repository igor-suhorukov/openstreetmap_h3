package com.github.isuhorukov.osm.pgsnapshot.util;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class PartitionSplitterTest {

    @TempDir
    Path tempDir;

    private static Partition partition(int id, long size, short... h33Regions) {
        Partition partition = new Partition(id, (short) (id * 10), (short) (id * 10 + 10), size);
        List<Short> regions = new ArrayList<>();
        for (short h33 : h33Regions) {
            regions.add(h33);
        }
        partition.setH33RegionsInside(regions);
        return partition;
    }

    private static List<List<Integer>> ids(List<List<Partition>> bins) {
        return bins.stream().map(bin -> bin.stream().map(Partition::getId).collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    @Test
    void balanceBySize_largestPartitionsSpreadAcrossScripts() {
        List<Partition> partitions = List.of(partition(0, 10), partition(1, 50), partition(2, 20),
                partition(3, 30), partition(4, 40));

        assertEquals(List.of(List.of(0, 1, 2), List.of(3, 4)), ids(PartitionSplitter.balanceBySize(partitions, 2)));
    }

    @Test
    void balanceBySize_moreScriptsThanPartitions_onePartitionPerScript() {
        List<Partition> partitions = List.of(partition(0, 10), partition(1, 50), partition(2, 20));

        assertEquals(List.of(List.of(1), List.of(2), List.of(0)), ids(PartitionSplitter.balanceBySize(partitions, 8)));
    }

    @Test
    void balanceBySize_noPartitions_noScripts() {
        assertTrue(PartitionSplitter.balanceBySize(List.of(), 4).isEmpty());
    }

    @Test
    void balanceBySize_zeroScriptCount_singleScript() {
        List<Partition> partitions = List.of(partition(0, 10), partition(1, 50));

        assertEquals(List.of(List.of(0, 1)), ids(PartitionSplitter.balanceBySize(partitions, 0)));
    }

    @Test
    void createWaysScript_copiesWithFreezeAndAnalyzesEachPartition() throws Exception {
        File resultDir = tempDir.toFile();
        ResultLayout.prepareResultDirectories(resultDir, true, false);

        int scripts = PartitionSplitter.createWaysScript(resultDir, 4,
                List.of(partition(0, 10, (short) 1, (short) 2), partition(1, 50, (short) 12)), false);

        assertEquals(2, scripts);
        String largest = Files.readString(new File(resultDir, "sql/ways_import_000.sql").toPath());
        assertTrue(largest.contains("CREATE TABLE \"ways_001\" (like ways) ;"), largest);
        assertTrue(largest.contains("FROM '/input/ways/00012.tsv' DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV FREEZE;"), largest);
        assertTrue(largest.contains("ANALYZE \"ways_001\";\nALTER TABLE  ways ATTACH PARTITION  \"ways_001\" FOR VALUES FROM (10) TO (20);"), largest);
        assertTrue(largest.endsWith("COMMIT;"), largest);
        String smallest = Files.readString(new File(resultDir, "sql/ways_import_001.sql").toPath());
        assertTrue(smallest.contains("'/input/ways/00001.tsv'"), smallest);
        assertTrue(smallest.contains("'/input/ways/00002.tsv'"), smallest);
        assertTrue(smallest.contains("ANALYZE \"ways_000\";"), smallest);
    }

    @Test
    void createNodesScript_columnar_withoutFreeze() throws Exception {
        File resultDir = tempDir.toFile();
        ResultLayout.prepareResultDirectories(resultDir, true, false);

        PartitionSplitter.createNodesScript(resultDir, 1, List.of(partition(0, 10, (short) 1)), true);

        String script = Files.readString(new File(resultDir, "sql/nodes_import_000.sql").toPath());
        assertTrue(script.contains("(like nodes) USING COLUMNAR;"), script);
        assertTrue(script.contains("NULL '\\N' CSV;"), script);
        assertFalse(script.contains("FREEZE"), script);
        assertTrue(script.contains("ANALYZE \"nodes_000\";"), script);
    }
}
