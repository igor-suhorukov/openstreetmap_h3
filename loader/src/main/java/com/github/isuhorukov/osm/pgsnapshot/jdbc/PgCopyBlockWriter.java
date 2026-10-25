package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.output.EncodedBlock;
import com.github.isuhorukov.osm.pgsnapshot.output.OrderedBlockWriter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

final class PgCopyBlockWriter extends OrderedBlockWriter {

    private final CopyTargets targets;

    PgCopyBlockWriter(CopyTargets targets, int maxBlocksInFlight, Consumer<Throwable> failureListener) {
        super(maxBlocksInFlight, failureListener);
        this.targets = targets;
    }

    @Override
    protected void write(EncodedBlock block) throws InterruptedException {
        Map<String, List<byte[]>> contentsByTarget = new LinkedHashMap<>();
        for (Map.Entry<Short, byte[]> content : block.getContents().entrySet()) {
            contentsByTarget.computeIfAbsent(targets.target(block.getType(), content.getKey()),
                    target -> new ArrayList<>()).add(content.getValue());
        }
        for (Map.Entry<String, List<byte[]>> targetContents : contentsByTarget.entrySet()) {
            targets.worker(targetContents.getKey()).submit(new CopyChunk(targetContents.getKey(), targetContents.getValue()));
        }
    }
}
