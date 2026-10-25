package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import java.util.List;

final class CopyChunk {

    private final String target;
    private final List<byte[]> contents;

    CopyChunk(String target, List<byte[]> contents) {
        this.target = target;
        this.contents = contents;
    }

    String getTarget() {
        return target;
    }

    List<byte[]> getContents() {
        return contents;
    }
}
