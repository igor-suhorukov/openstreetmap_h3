package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.output.BlockType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class CopyTargetsTest {

    private final CopyWorker firstWorker = mock(CopyWorker.class);
    private final CopyWorker secondWorker = mock(CopyWorker.class);
    private final CopyWorker scriptWorker = mock(CopyWorker.class);

    private CopyTargets targets() {
        CopyTargets targets = new CopyTargets();
        targets.addPartition(new Partition(0, Short.MIN_VALUE, (short) 25648, 10), firstWorker);
        targets.addPartition(new Partition(1, (short) 25648, (short) 32695, 20), secondWorker);
        targets.addInput("/input/relations/00000.tsv", scriptWorker);
        targets.addInput("/input/ways/32767.tsv", scriptWorker);
        return targets;
    }

    @Test
    void target_nodesAndWays_partitionTablesByH33Range() {
        CopyTargets targets = targets();

        assertEquals("nodes_000", targets.target(BlockType.NODES, Short.MIN_VALUE));
        assertEquals("nodes_000", targets.target(BlockType.NODES, (short) 25647));
        assertEquals("nodes_001", targets.target(BlockType.NODES, (short) 25648));
        assertEquals("ways_000", targets.target(BlockType.WAYS, (short) -31636));
        assertEquals("ways_001", targets.target(BlockType.WAYS, (short) 32694));
    }

    @Test
    void target_waysCrossingCellsAndRelations_inputPaths() {
        CopyTargets targets = targets();

        assertEquals("/input/ways/32767.tsv", targets.target(BlockType.WAYS, Short.MAX_VALUE));
        assertEquals("/input/relations/00000.tsv", targets.target(BlockType.RELATIONS, (short) 0));
        assertEquals("/input/relations/00001.tsv", targets.target(BlockType.RELATIONS, (short) 1));
    }

    @Test
    void target_outsideOfPartitions_throws() {
        CopyTargets targets = targets();

        assertThrows(IllegalStateException.class, () -> targets.target(BlockType.NODES, Short.MAX_VALUE));
        assertThrows(IllegalStateException.class, () -> targets.target(BlockType.WAYS, (short) 32700));
    }

    @Test
    void worker_byTarget() {
        CopyTargets targets = targets();

        assertSame(firstWorker, targets.worker("nodes_000"));
        assertSame(firstWorker, targets.worker("ways_000"));
        assertSame(secondWorker, targets.worker("ways_001"));
        assertSame(scriptWorker, targets.worker("/input/ways/32767.tsv"));
        assertThrows(IllegalStateException.class, () -> targets.worker("/input/relations/00001.tsv"));
    }
}
