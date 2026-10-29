package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public abstract class OrderedBlockWriter {

    private final Semaphore blocksInFlight;
    private final Consumer<Throwable> failureListener;
    private final Map<Long, ReadyBlock> readyBlocks = new HashMap<>();
    private long nextBlockToSave;
    private boolean saving;

    protected OrderedBlockWriter(int maxBlocksInFlight, Consumer<Throwable> failureListener) {
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

    protected abstract void write(EncodedBlock block) throws InterruptedException, ExecutionException;

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
            if (!block.isEmpty()) {
                write(block);
            }
        } catch (ExecutionException e) {
            failureListener.accept(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failureListener.accept(e);
        } catch (RuntimeException e) {
            failureListener.accept(e);
        } finally {
            BlockStat blockStat = block.getBlockStat();
            if (blockStat != null) {
                blockStat.setWaitingForSaveTime(saveStartTime - readyBlock.readyTime);
                blockStat.setSaveTime(blockStat.getSaveTime() + (System.currentTimeMillis() - saveStartTime));
            }
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
