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
import java.util.HashMap;
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

    private TsvBlockWriter writer(int maxBlocksInFlight) {
        return new TsvBlockWriter(saveExecutorService, tempDir.toFile(), maxBlocksInFlight, failures::add);
    }

    private static EncodedBlock block(BlockType type, long blockNumber, String line) {
        return EncodedBlock.encode(type,
                Collections.singletonMap((short) 0, new StringBuilder(line + blockNumber + "\n")), new BlockStat(blockNumber));
    }

    private static String content(File file) throws Exception {
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    }

    @Test
    void complete_blocksInReverseOrder_writtenInBlockOrderWhenFirstBlockArrives() throws Exception {
        File nodesDir = nodesDir();
        TsvBlockWriter writer = writer(8);
        File tsv = new File(nodesDir, "00000.tsv");

        for (long blockNumber = 3; blockNumber > 0; blockNumber--) {
            writer.complete(blockNumber, block(BlockType.NODES, blockNumber, "block-"));
        }
        assertFalse(tsv.exists());
        writer.complete(0, block(BlockType.NODES, 0, "block-"));

        assertEquals("block-0\nblock-1\nblock-2\nblock-3\n", content(tsv));
        assertTrue(failures.isEmpty());
    }

    @Test
    void complete_concurrentWorkers_contentInBlockOrder() throws Exception {
        int blocks = 64;
        File nodesDir = nodesDir();
        TsvBlockWriter writer = writer(blocks);
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
                    writer.complete(blockNumber, block(BlockType.NODES, blockNumber, "block-"));
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
        TsvBlockWriter writer = writer(4);

        writer.complete(1, block(BlockType.NODES, 1, "after-empty-"));
        writer.complete(0, EncodedBlock.EMPTY);

        assertEquals("after-empty-1\n", content(new File(nodesDir, "00000.tsv")));
    }

    @Test
    void complete_blockTypes_writtenToTypeDirectories() throws Exception {
        nodesDir();
        TsvBlockWriter writer = writer(4);

        writer.complete(0, block(BlockType.NODES, 0, "node-"));
        writer.complete(1, block(BlockType.WAYS, 1, "way-"));
        writer.complete(2, block(BlockType.RELATIONS, 2, "relation-"));

        assertEquals("node-0\n", content(new File(tempDir.toFile(), "nodes/00000.tsv")));
        assertEquals("way-1\n", content(new File(tempDir.toFile(), "ways/00000.tsv")));
        assertEquals("relation-2\n", content(new File(tempDir.toFile(), "relations/00000.tsv")));
        assertTrue(failures.isEmpty());
    }

    @Test
    void complete_savedBlock_setsSaveStatistics() {
        nodesDir();
        TsvBlockWriter writer = writer(4);
        EncodedBlock first = block(BlockType.NODES, 0, "x");
        first.getBlockStat().setSaveTime(5);

        writer.complete(0, first);

        assertTrue(first.getBlockStat().getSaveTime() >= 5);
        assertTrue(first.getBlockStat().getWaitingForSaveTime() >= 0);
    }

    @Test
    void reserve_allSlotsTaken_waitsUntilBlockSaved() throws Exception {
        nodesDir();
        TsvBlockWriter writer = writer(1);
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
        writer.complete(0, block(BlockType.NODES, 0, "slot-"));
        assertTrue(reserved.await(5, TimeUnit.SECONDS));
        second.join(5000);
    }

    @Test
    void complete_missingDirectory_reportsFailureAndKeepsOrder() throws Exception {
        File nodesDir = nodesDir();
        Files.delete(new File(tempDir.toFile(), ResultLayout.WAYS_DIR).toPath());
        TsvBlockWriter writer = writer(4);

        writer.complete(0, block(BlockType.WAYS, 0, "lost-"));
        writer.complete(1, block(BlockType.NODES, 1, "saved-"));

        assertEquals(1, failures.size());
        assertInstanceOf(UncheckedIOException.class, failures.get(0));
        assertEquals("saved-1\n", content(new File(nodesDir, "00000.tsv")));
    }

    @Test
    void encode_nonLatinText_utf8Bytes() {
        EncodedBlock encoded = EncodedBlock.encode(BlockType.RELATIONS,
                Map.of((short) 5, new StringBuilder("ถนน\n")), new BlockStat(0));

        assertArrayEquals("ถนน\n".getBytes(StandardCharsets.UTF_8), encoded.getContents().get((short) 5));
        assertEquals(BlockType.RELATIONS, encoded.getType());
        assertTrue(EncodedBlock.EMPTY.getContents().isEmpty());
    }

    @Test
    void encode_contentsOrderedByH33() {
        Map<Short, StringBuilder> csv = new HashMap<>();
        for (short h33 : new short[]{25764, -31636, 0, 32767, 24898}) {
            csv.put(h33, new StringBuilder(h33 + "\n"));
        }

        EncodedBlock encoded = EncodedBlock.encode(BlockType.WAYS, csv, new BlockStat(0));

        assertEquals(List.of((short) -31636, (short) 0, (short) 24898, (short) 25764, (short) 32767),
                new ArrayList<>(encoded.getContents().keySet()));
    }
}
