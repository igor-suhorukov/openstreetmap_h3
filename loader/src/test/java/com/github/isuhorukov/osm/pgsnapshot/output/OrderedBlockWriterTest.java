package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class OrderedBlockWriterTest {

    private final List<Throwable> failures = new CopyOnWriteArrayList<>();

    private static EncodedBlock block(long blockNumber) {
        return EncodedBlock.encode(BlockType.NODES,
                Collections.singletonMap((short) 0, new StringBuilder("row" + blockNumber)), new BlockStat(blockNumber));
    }

    private static final class FailingWriter extends OrderedBlockWriter {
        private final List<Long> written = new ArrayList<>();

        private FailingWriter(List<Throwable> failures) {
            super(4, failures::add);
        }

        @Override
        protected void write(EncodedBlock block) throws InterruptedException, ExecutionException {
            long blockNumber = block.getBlockStat().getId();
            if (blockNumber == 0) {
                throw new IllegalStateException("runtime failure");
            }
            if (blockNumber == 1) {
                throw new ExecutionException(new IllegalArgumentException("execution failure"));
            }
            if (blockNumber == 2) {
                throw new InterruptedException("interrupted");
            }
            written.add(blockNumber);
        }
    }

    @Test
    void complete_writeFailures_reportedAndLaterBlocksWritten() {
        FailingWriter writer = new FailingWriter(failures);

        writer.complete(3, block(3));
        writer.complete(2, block(2));
        writer.complete(1, block(1));
        writer.complete(0, block(0));

        assertEquals(List.of(3L), writer.written);
        assertEquals(3, failures.size());
        assertEquals("runtime failure", failures.get(0).getMessage());
        assertEquals("execution failure", failures.get(1).getMessage());
        assertInstanceOf(InterruptedException.class, failures.get(2));
        assertTrue(Thread.interrupted());
    }

    @Test
    void complete_emptyBlock_notWritten() {
        FailingWriter writer = new FailingWriter(failures);

        writer.complete(0, EncodedBlock.EMPTY);
        writer.complete(1, EncodedBlock.EMPTY);
        writer.complete(2, EncodedBlock.EMPTY);
        writer.complete(3, block(3));

        assertEquals(List.of(3L), writer.written);
        assertTrue(failures.isEmpty());
    }
}
