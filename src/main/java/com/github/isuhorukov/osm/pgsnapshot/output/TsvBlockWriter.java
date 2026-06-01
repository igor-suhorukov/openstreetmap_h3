package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class TsvBlockWriter {

    private static final Logger log = LoggerFactory.getLogger(TsvBlockWriter.class);

    private final AtomicInteger currentBlockToSave;
    private final ExecutorService saveExecutorService;
    private final File resultDir;

    public TsvBlockWriter(AtomicInteger currentBlockToSave, ExecutorService saveExecutorService, File resultDir) {
        this.currentBlockToSave = currentBlockToSave;
        this.saveExecutorService = saveExecutorService;
        this.resultDir = resultDir;
    }

    public void saveDataOnlyInOneThread(Map<Short, StringBuilder> csvResultPerH33, long nodeRecords,
                                        long wayRecords, BlockStat blockStatistic, long relationCount,
                                        Long blockNumber) {
        long waitSaveTime = System.currentTimeMillis();
        while (true) {
            if (currentBlockToSave.get() == blockNumber) {
                blockStatistic.setWaitingForSaveTime(System.currentTimeMillis() - waitSaveTime);
                long startSaveTime = System.currentTimeMillis();
                try {
                    File currentBlockTypeDir =
                            ResultLayout.blockResultDirectory(nodeRecords, wayRecords, relationCount, resultDir);
                    saveBlockData(csvResultPerH33, currentBlockTypeDir);
                } finally {
                    blockStatistic.setSaveTime(blockStatistic.getSaveTime() + (System.currentTimeMillis() - startSaveTime));
                    currentBlockToSave.incrementAndGet();
                }
                return;
            }
            Thread.yield();
        }
    }

    private void saveBlockData(Map<Short, StringBuilder> csvResultPerH33, File currentBlockTypeDir) {
        List<Future<?>> saveFutures = new ArrayList<>();
        for (Map.Entry<Short, StringBuilder> result : csvResultPerH33.entrySet()) {
            saveFutures.add(saveExecutorService.submit(() -> {
                try (OutputStream dataExport =
                             new FileOutputStream(new File(currentBlockTypeDir, String.format("%05d.tsv", result.getKey())), true)) {
                    IOUtils.write(result.getValue(), dataExport, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }));
        }
        for (Future<?> future : saveFutures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("interrupted while saving block data", e);
            } catch (ExecutionException e) {
                log.error("failed to save block data", e);
            }
        }
    }
}
