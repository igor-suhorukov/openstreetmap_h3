package com.github.isuhorukov.osm.pgsnapshot.output;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class TsvBlockWriter extends OrderedBlockWriter {

    private static final int H33_OFFSET = -Short.MIN_VALUE;
    private static final int H33_VALUES = 1 << 16;

    private final ExecutorService saveExecutorService;
    private final File resultDirectory;
    private final File spillDirectory;
    private final Map<BlockType, long[]> lastSpilledIds = new EnumMap<>(BlockType.class);

    public TsvBlockWriter(ExecutorService saveExecutorService, File resultDirectory, int maxBlocksInFlight,
                          Consumer<Throwable> failureListener) {
        this(saveExecutorService, resultDirectory, null, maxBlocksInFlight, failureListener);
    }

    public TsvBlockWriter(ExecutorService saveExecutorService, File resultDirectory, File spillDirectory,
                          int maxBlocksInFlight, Consumer<Throwable> failureListener) {
        super(maxBlocksInFlight, failureListener);
        this.saveExecutorService = saveExecutorService;
        this.resultDirectory = resultDirectory;
        this.spillDirectory = spillDirectory;
    }

    @Override
    protected void write(EncodedBlock block) throws InterruptedException, ExecutionException {
        List<Future<?>> saveFutures = new ArrayList<>();
        if (!block.getContents().isEmpty()) {
            File directory = new File(resultDirectory, block.getType().getDirectoryName());
            for (Map.Entry<Short, byte[]> content : block.getContents().entrySet()) {
                File tsv = new File(directory, String.format("%05d.tsv", content.getKey()));
                saveFutures.add(saveExecutorService.submit(() -> append(tsv, content.getValue())));
            }
        }
        if (!block.getSpillChunks().isEmpty()) {
            checkIdOrder(block);
            File directory = new File(spillDirectory, block.getType().getDirectoryName());
            for (Map.Entry<Short, SpillChunk> chunk : block.getSpillChunks().entrySet()) {
                File spill = new File(directory, ResultLayout.spillFileName(chunk.getKey()));
                saveFutures.add(saveExecutorService.submit(() -> append(spill, chunk.getValue().getBytes())));
            }
        }
        for (Future<?> future : saveFutures) {
            future.get();
        }
    }

    private void checkIdOrder(EncodedBlock block) {
        long[] lastIds = lastSpilledIds.computeIfAbsent(block.getType(), type -> {
            long[] ids = new long[H33_VALUES];
            Arrays.fill(ids, Long.MIN_VALUE);
            return ids;
        });
        for (Map.Entry<Short, SpillChunk> chunk : block.getSpillChunks().entrySet()) {
            int cell = chunk.getKey() + H33_OFFSET;
            if (chunk.getValue().getFirstId() <= lastIds[cell]) {
                throw SpillChunk.outOfOrder(block.getType().getDirectoryName(), chunk.getKey(),
                        chunk.getValue().getFirstId(), lastIds[cell]);
            }
        }
        for (Map.Entry<Short, SpillChunk> chunk : block.getSpillChunks().entrySet()) {
            lastIds[chunk.getKey() + H33_OFFSET] = chunk.getValue().getLastId();
        }
    }

    private static void append(File file, byte[] content) {
        try (OutputStream dataExport = new FileOutputStream(file, true)) {
            dataExport.write(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
