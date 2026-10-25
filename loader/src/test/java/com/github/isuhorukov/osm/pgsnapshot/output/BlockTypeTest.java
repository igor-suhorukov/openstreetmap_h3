package com.github.isuhorukov.osm.pgsnapshot.output;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BlockTypeTest {

    @Test
    void of_nodes() {
        assertEquals(BlockType.NODES, BlockType.of(1, 0, 0));
        assertEquals(ResultLayout.NODES_DIR, BlockType.NODES.getDirectoryName());
    }

    @Test
    void of_ways() {
        assertEquals(BlockType.WAYS, BlockType.of(0, 5, 0));
        assertEquals(ResultLayout.WAYS_DIR, BlockType.WAYS.getDirectoryName());
    }

    @Test
    void of_relations() {
        assertEquals(BlockType.RELATIONS, BlockType.of(0, 0, 3));
        assertEquals(ResultLayout.RELATIONS_DIR, BlockType.RELATIONS.getDirectoryName());
    }

    @Test
    void of_allZero_throws() {
        assertThrows(IllegalArgumentException.class, () -> BlockType.of(0, 0, 0));
    }
}
