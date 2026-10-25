package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.output.BlockType;
import com.github.isuhorukov.osm.pgsnapshot.output.EncodedBlock;
import com.github.isuhorukov.osm.pgsnapshot.output.OrderedBlockWriter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

final class DeltaBlockWriter extends OrderedBlockWriter {

    static final String NODES = InputFiles.INPUT_DIRECTORY + "delta/nodes.tsv";
    static final String WAYS = InputFiles.INPUT_DIRECTORY + "delta/ways.tsv";
    static final String RELATIONS = InputFiles.INPUT_DIRECTORY + "delta/relations.tsv";
    static final String RELATION_MEMBERS = InputFiles.INPUT_DIRECTORY + "delta/relation_members.tsv";
    static final String CHANGES = InputFiles.INPUT_DIRECTORY + "delta/changes.tsv";

    private final CopyWorker worker;

    DeltaBlockWriter(CopyWorker worker, int maxBlocksInFlight, Consumer<Throwable> failureListener) {
        super(maxBlocksInFlight, failureListener);
        this.worker = worker;
    }

    static String target(BlockType type, short h33) {
        switch (type) {
            case NODES:
                return NODES;
            case WAYS:
                return WAYS;
            default:
                return h33 == 0 ? RELATIONS : RELATION_MEMBERS;
        }
    }

    @Override
    protected void write(EncodedBlock block) throws InterruptedException {
        Map<String, List<byte[]>> contentsByTarget = new LinkedHashMap<>();
        for (Map.Entry<Short, byte[]> content : block.getContents().entrySet()) {
            contentsByTarget.computeIfAbsent(target(block.getType(), content.getKey()),
                    target -> new ArrayList<>()).add(content.getValue());
        }
        for (Map.Entry<String, List<byte[]>> targetContents : contentsByTarget.entrySet()) {
            worker.submit(new CopyChunk(targetContents.getKey(), targetContents.getValue()));
        }
    }
}
