package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TsvBlockWriterTest {

    @TempDir
    Path tempDir;

    private ExecutorService saveExecutorService;
    private final List<Throwable> failures = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startSaveExecutor() {
        saveExecutorService = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void stopSaveExecutor() {
        saveExecutorService.shutdownNow();
    }

    private File nodesDir() {
        File resultDir = tempDir.toFile();
        ResultLayout.prepareResultDirectories(resultDir, true, false);
        return new File(resultDir, ResultLayout.NODES_DIR);
    }

    private static EncodedBlock block(File directory, long blockNumber, String line) {
        return EncodedBlock.encode(directory,
                Collections.singletonMap((short) 0, new StringBuilder(line + blockNumber + "\n")), new BlockStat(blockNumber));
    }

    private static String content(File file) throws Exception {
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }

    @Test
    void complete_blocksInReverseOrder_writtenInBlockOrderWhenFirstBlockArrives() throws Exception {
        File nodesDir = nodesDir();
        TsvBlockWriter writer = new TsvBlockWriter(saveExecutorService, 8, failures::add);
        File tsv = new File(nodesDir, "00000.tsv");

        for (long blockNumber = 3; blockNumber > 0; blockNumber--) {
            writer.complete(blockNumber, block(nodesDir, blockNumber, "block-"));
        }
        assertFalse(tsv.exists());
        writer.complete(0, block(nodesDir, 0, "block-"));

        assertEquals("block-0\nblock-1\nblock-2\nblock-3\n", content(tsv));
        assertTrue(failures.isEmpty());
    }

    @Test
    void complete_concurrentWorkers_contentInBlockOrder() throws Exception {
        int blocks = 64;
        File nodesDir = nodesDir();
        TsvBlockWriter writer = new TsvBlockWriter(saveExecutorService, blocks, failures::add);
        ExecutorService workers = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Long> order = new ArrayList<>();
            for (long blockNumber = 0; blockNumber < blocks; blockNumber++) {
                order.add(blockNumber);
            }
            Collections.shuffle(order, new Random(7));
            for (long blockNumber : order) {
                workers.submit(() -> {
                    start.await();
                    writer.complete(blockNumber, block(nodesDir, blockNumber, "block-"));
                    return null;
                });
            }
            start.countDown();
            workers.shutdown();
            assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            workers.shutdownNow();
        }

        StringBuilder expected = new StringBuilder();
        for (int i = 0; i < blocks; i++) {
            expected.append("block-").append(i).append('\n');
        }
        assertEquals(expected.toString(), content(new File(nodesDir, "00000.tsv")));
        assertTrue(failures.isEmpty());
    }

    @Test
    void complete_emptyBlock_advancesToNextBlock() throws Exception {
        File nodesDir = nodesDir();
        TsvBlockWriter writer = new TsvBlockWriter(saveExecutorService, 4, failures::add);

        writer.complete(1, block(nodesDir, 1, "after-empty-"));
        writer.complete(0, EncodedBlock.EMPTY);

        assertEquals("after-empty-1\n", content(new File(nodesDir, "00000.tsv")));
    }

    @Test
    void complete_savedBlock_setsSaveStatistics() {
        File nodesDir = nodesDir();
        TsvBlockWriter writer = new TsvBlockWriter(saveExecutorService, 4, failures::add);
        EncodedBlock first = block(nodesDir, 0, "x");
        first.getBlockStat().setSaveTime(5);

        writer.complete(0, first);

        assertTrue(first.getBlockStat().getSaveTime() >= 5);
        assertTrue(first.getBlockStat().getWaitingForSaveTime() >= 0);
    }

    @Test
    void reserve_allSlotsTaken_waitsUntilBlockSaved() throws Exception {
        File nodesDir = nodesDir();
        TsvBlockWriter writer = new TsvBlockWriter(saveExecutorService, 1, failures::add);
        writer.reserve();
        CountDownLatch reserved = new CountDownLatch(1);
        Thread second = new Thread(() -> {
            try {
                writer.reserve();
                reserved.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        second.start();

        assertFalse(reserved.await(200, TimeUnit.MILLISECONDS));
        writer.complete(0, block(nodesDir, 0, "slot-"));
        assertTrue(reserved.await(5, TimeUnit.SECONDS));
        second.join(5000);
    }

    @Test
    void complete_missingDirectory_reportsFailureAndKeepsOrder() throws Exception {
        File nodesDir = nodesDir();
        TsvBlockWriter writer = new TsvBlockWriter(saveExecutorService, 4, failures::add);

        writer.complete(0, block(new File(tempDir.toFile(), "missing"), 0, "lost-"));
        writer.complete(1, block(nodesDir, 1, "saved-"));

        assertEquals(1, failures.size());
        assertInstanceOf(UncheckedIOException.class, failures.get(0));
        assertEquals("saved-1\n", content(new File(nodesDir, "00000.tsv")));
    }

    @Test
    void encode_nonLatinText_utf8Bytes() {
        EncodedBlock encoded = EncodedBlock.encode(tempDir.toFile(),
                Map.of((short) 5, new StringBuilder("ถนน\n")), new BlockStat(0));

        assertArrayEquals("ถนน\n".getBytes(StandardCharsets.UTF_8), encoded.getFileContents().get((short) 5));
        assertEquals(tempDir.toFile(), encoded.getDirectory());
        assertTrue(EncodedBlock.EMPTY.getFileContents().isEmpty());
    }
}
