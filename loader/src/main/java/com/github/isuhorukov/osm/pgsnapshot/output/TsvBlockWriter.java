package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class TsvBlockWriter {

    private final ExecutorService saveExecutorService;
    private final Semaphore blocksInFlight;
    private final Consumer<Throwable> failureListener;
    private final Map<Long, ReadyBlock> readyBlocks = new HashMap<>();
    private long nextBlockToSave;
    private boolean saving;

    public TsvBlockWriter(ExecutorService saveExecutorService, int maxBlocksInFlight,
                          Consumer<Throwable> failureListener) {
        this.saveExecutorService = saveExecutorService;
        this.blocksInFlight = new Semaphore(maxBlocksInFlight);
        this.failureListener = failureListener;
    }

    public void reserve() throws InterruptedException {
        blocksInFlight.acquire();
    }

    public void complete(long blockNumber, EncodedBlock block) {
        synchronized (readyBlocks) {
            readyBlocks.put(blockNumber, new ReadyBlock(block, System.currentTimeMillis()));
            if (saving) {
                return;
            }
            saving = true;
        }
        ReadyBlock next = takeNextBlockToSave();
        while (next != null) {
            try {
                save(next);
            } finally {
                blocksInFlight.release();
            }
            next = takeNextBlockToSave();
        }
    }

    private ReadyBlock takeNextBlockToSave() {
        synchronized (readyBlocks) {
            ReadyBlock next = readyBlocks.remove(nextBlockToSave);
            if (next == null) {
                saving = false;
            } else {
                nextBlockToSave++;
            }
            return next;
        }
    }

    private void save(ReadyBlock readyBlock) {
        EncodedBlock block = readyBlock.block;
        long saveStartTime = System.currentTimeMillis();
        try {
            List<Future<?>> saveFutures = new ArrayList<>();
            for (Map.Entry<Short, byte[]> fileContent : block.getFileContents().entrySet()) {
                saveFutures.add(saveExecutorService.submit(() ->
                        append(block.getDirectory(), fileContent.getKey(), fileContent.getValue())));
            }
            for (Future<?> future : saveFutures) {
                future.get();
            }
        } catch (ExecutionException e) {
            failureListener.accept(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failureListener.accept(e);
        } finally {
            BlockStat blockStat = block.getBlockStat();
            if (blockStat != null) {
                blockStat.setWaitingForSaveTime(saveStartTime - readyBlock.readyTime);
                blockStat.setSaveTime(blockStat.getSaveTime() + (System.currentTimeMillis() - saveStartTime));
            }
        }
    }

    private static void append(File directory, short h33, byte[] content) {
        try (OutputStream dataExport = new FileOutputStream(new File(directory, String.format("%05d.tsv", h33)), true)) {
            dataExport.write(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final class ReadyBlock {
        private final EncodedBlock block;
        private final long readyTime;

        private ReadyBlock(EncodedBlock block, long readyTime) {
            this.block = block;
            this.readyTime = readyTime;
        }
    }
}
