package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TsvBlockWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void blocksAreSavedInBlockNumberOrderRegardlessOfStartOrder() throws Exception {
        int blocks = 8;
        AtomicInteger currentBlockToSave = new AtomicInteger(0);
        ExecutorService saveExecutorService = Executors.newFixedThreadPool(2);
        ExecutorService workers = Executors.newFixedThreadPool(blocks);
        try {
            File resultDir = tempDir.toFile();
            ResultLayout.prepareResultDirectories(resultDir, true, false);
            TsvBlockWriter writer = new TsvBlockWriter(currentBlockToSave, saveExecutorService, resultDir);

            List<Long> saveCompletionOrder = new CopyOnWriteArrayList<>();
            CountDownLatch ready = new CountDownLatch(blocks);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(blocks);

            for (long blockNumber = blocks - 1; blockNumber >= 0; blockNumber--) {
                final long block = blockNumber;
                workers.submit(() -> {
                    Map<Short, StringBuilder> csv =
                            Collections.singletonMap((short) 0, new StringBuilder("block-" + block + "\n"));
                    BlockStat stat = new BlockStat(block);
                    ready.countDown();
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    writer.saveDataOnlyInOneThread(csv, 1, 0, stat, 0, block);
                    saveCompletionOrder.add(block);
                    done.countDown();
                });
            }

            assertTrue(ready.await(5, TimeUnit.SECONDS), "workers did not start");
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS), "blocks were not all saved in time");

            for (int i = 0; i < blocks; i++) {
                assertEquals((long) i, saveCompletionOrder.get(i));
            }
            assertEquals(blocks, currentBlockToSave.get());

            File tsv = new File(new File(resultDir, "nodes"), "00000.tsv");
            String content = new String(Files.readAllBytes(tsv.toPath()), StandardCharsets.UTF_8);
            StringBuilder expected = new StringBuilder();
            for (int i = 0; i < blocks; i++) {
                expected.append("block-").append(i).append('\n');
            }
            assertEquals(expected.toString(), content);
        } finally {
            workers.shutdownNow();
            saveExecutorService.shutdownNow();
        }
    }
}
