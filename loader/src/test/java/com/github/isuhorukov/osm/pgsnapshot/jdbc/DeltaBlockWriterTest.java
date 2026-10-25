package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.output.BlockType;
import com.github.isuhorukov.osm.pgsnapshot.output.EncodedBlock;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DeltaBlockWriterTest {

    private final CopyWorker worker = mock(CopyWorker.class);
    private final List<Throwable> failures = new CopyOnWriteArrayList<>();

    private static EncodedBlock block(BlockType type, long blockNumber, short... h33s) {
        Map<Short, StringBuilder> csv = new HashMap<>();
        for (short h33 : h33s) {
            csv.put(h33, new StringBuilder(h33 + "\n"));
        }
        return EncodedBlock.encode(type, csv, new BlockStat(blockNumber));
    }

    @Test
    void target_allPartitionsOfTypeIntoOneDeltaTable() {
        assertEquals(DeltaBlockWriter.NODES, DeltaBlockWriter.target(BlockType.NODES, (short) -31636));
        assertEquals(DeltaBlockWriter.WAYS, DeltaBlockWriter.target(BlockType.WAYS, (short) 24898));
        assertEquals(DeltaBlockWriter.WAYS, DeltaBlockWriter.target(BlockType.WAYS, Short.MAX_VALUE));
        assertEquals(DeltaBlockWriter.RELATIONS, DeltaBlockWriter.target(BlockType.RELATIONS, (short) 0));
        assertEquals(DeltaBlockWriter.RELATION_MEMBERS, DeltaBlockWriter.target(BlockType.RELATIONS, (short) 1));
    }

    @Test
    void complete_wayBlock_oneChunkWithAllPartitions() throws Exception {
        DeltaBlockWriter writer = new DeltaBlockWriter(worker, 4, failures::add);

        writer.complete(0, block(BlockType.WAYS, 0, (short) 25700, Short.MAX_VALUE, (short) -31636));
        writer.complete(1, block(BlockType.RELATIONS, 1, (short) 0, (short) 1));

        ArgumentCaptor<CopyChunk> chunks = ArgumentCaptor.forClass(CopyChunk.class);
        verify(worker, times(3)).submit(chunks.capture());
        assertEquals(List.of(DeltaBlockWriter.WAYS, DeltaBlockWriter.RELATIONS, DeltaBlockWriter.RELATION_MEMBERS),
                chunks.getAllValues().stream().map(CopyChunk::getTarget).collect(Collectors.toList()));
        assertEquals("-31636\n25700\n32767\n", chunks.getAllValues().get(0).getContents().stream()
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8)).collect(Collectors.joining()));
        assertTrue(failures.isEmpty());
    }

    @Test
    void complete_emptyBlock_nothingSubmitted() throws Exception {
        new DeltaBlockWriter(worker, 4, failures::add).complete(0, block(BlockType.NODES, 0));

        verify(worker, never()).submit(any());
    }
}
