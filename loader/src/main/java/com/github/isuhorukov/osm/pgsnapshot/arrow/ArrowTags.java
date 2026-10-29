package com.github.isuhorukov.osm.pgsnapshot.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;

import java.nio.charset.StandardCharsets;
import java.util.Map;

final class ArrowTags {

    private ArrowTags() {
    }

    static void write(BufferAllocator allocator, UnionMapWriter mapWriter, int idx, Map<String, String> tags) {
        mapWriter.setPosition(idx);
        if (tags != null && !tags.isEmpty()) {
            mapWriter.startMap();
            tags.forEach((key, value) -> {
                if (value.isEmpty()) {
                    value = "-";
                }
                if (key.isEmpty()) {
                    key = "-";
                }
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                try (
                        ArrowBuf keyBuf = allocator.buffer(keyBytes.length);
                        ArrowBuf valueBuf = allocator.buffer(valueBytes.length)
                ) {
                    mapWriter.startEntry();
                    keyBuf.writeBytes(keyBytes);
                    valueBuf.writeBytes(valueBytes);
                    mapWriter.key().varChar().writeVarChar(0, keyBytes.length, keyBuf);
                    mapWriter.value().varChar().writeVarChar(0, valueBytes.length, valueBuf);
                    mapWriter.endEntry();
                }
            });
            mapWriter.endMap();
        } else {
            mapWriter.writeNull();
        }
    }
}
