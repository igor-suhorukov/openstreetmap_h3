package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.output.BlockType;
import com.github.isuhorukov.osm.pgsnapshot.output.EncodedBlock;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PgCopyBlockWriterTest {

    private final CopyWorker partitionWorker = mock(CopyWorker.class);
    private final CopyWorker scriptWorker = mock(CopyWorker.class);
    private final List<Throwable> failures = new CopyOnWriteArrayList<>();

    private PgCopyBlockWriter writer() {
        CopyTargets targets = new CopyTargets();
        targets.addPartition(new Partition(0, Short.MIN_VALUE, (short) 25648, 10), partitionWorker);
        targets.addPartition(new Partition(1, (short) 25648, (short) 32695, 20), partitionWorker);
        targets.addInput("/input/ways/32767.tsv", scriptWorker);
        return new PgCopyBlockWriter(targets, 4, failures::add);
    }

    private static EncodedBlock block(BlockType type, long blockNumber, short... h33s) {
        Map<Short, StringBuilder> csv = new HashMap<>();
        for (short h33 : h33s) {
            csv.put(h33, new StringBuilder(h33 + "\n"));
        }
        return EncodedBlock.encode(type, csv, new BlockStat(blockNumber));
    }

    private static String content(CopyChunk chunk) {
        return chunk.getContents().stream().map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .collect(Collectors.joining());
    }

    @Test
    void complete_wayBlock_chunksPerTargetInH33Order() throws Exception {
        writer().complete(0, block(BlockType.WAYS, 0, (short) 25700, Short.MAX_VALUE, (short) 24898, (short) -31636));

        ArgumentCaptor<CopyChunk> partitionChunks = ArgumentCaptor.forClass(CopyChunk.class);
        verify(partitionWorker, times(2)).submit(partitionChunks.capture());
        assertEquals(List.of("ways_000", "ways_001"),
                partitionChunks.getAllValues().stream().map(CopyChunk::getTarget).collect(Collectors.toList()));
        assertEquals("-31636\n24898\n", content(partitionChunks.getAllValues().get(0)));
        assertEquals("25700\n", content(partitionChunks.getAllValues().get(1)));
        ArgumentCaptor<CopyChunk> scriptChunks = ArgumentCaptor.forClass(CopyChunk.class);
        verify(scriptWorker).submit(scriptChunks.capture());
        assertEquals("/input/ways/32767.tsv", scriptChunks.getValue().getTarget());
        assertEquals("32767\n", content(scriptChunks.getValue()));
        assertTrue(failures.isEmpty());
    }

    @Test
    void complete_blocksOutOfOrder_submittedInBlockOrder() throws Exception {
        List<String> submitted = new ArrayList<>();
        doAnswer(invocation -> submitted.add(content(invocation.getArgument(0))))
                .when(partitionWorker).submit(any(CopyChunk.class));
        PgCopyBlockWriter writer = writer();

        writer.complete(2, block(BlockType.NODES, 2, (short) 3));
        writer.complete(1, block(BlockType.NODES, 1, (short) 2));
        writer.complete(0, block(BlockType.NODES, 0, (short) 1));

        assertEquals(List.of("1\n", "2\n", "3\n"), submitted);
    }

    @Test
    void complete_targetWithoutWorker_reportedAsFailure() throws Exception {
        writer().complete(0, block(BlockType.RELATIONS, 0, (short) 0));

        assertEquals(1, failures.size());
        assertInstanceOf(IllegalStateException.class, failures.get(0));
        verify(scriptWorker, never()).submit(any(CopyChunk.class));
    }
}
