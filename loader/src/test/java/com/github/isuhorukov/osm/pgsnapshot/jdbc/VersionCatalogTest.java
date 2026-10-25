package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class VersionCatalogTest {

    private static VersionCatalog.Table table(char type, boolean withHash, long[]... rows) {
        VersionCatalog.Table table = new VersionCatalog.Table(type, rows.length, withHash);
        for (long[] row : rows) {
            table.add(row[0], (int) row[1], withHash ? row[2] : 0);
        }
        return table;
    }

    private static String changes(VersionCatalog catalog) {
        return catalog.changes().stream().map(chunk -> new String(chunk, StandardCharsets.US_ASCII))
                .collect(Collectors.joining());
    }

    @Test
    void unchanged_versionAndHashCompared_changesListCreatedUpdatedDeleted() {
        VersionCatalog catalog = new VersionCatalog(
                table('N', false, new long[]{1, 1}, new long[]{2, 4}, new long[]{3, 1}),
                table('W', true, new long[]{10, 2, 77}, new long[]{11, 2, 88}, new long[]{12, 1, 5}),
                table('R', false, new long[]{100, 1}));

        assertTrue(catalog.unchangedNode(1, 1));
        assertFalse(catalog.unchangedNode(2, 5));
        assertFalse(catalog.unchangedNode(7, 1));
        assertTrue(catalog.unchangedWay(10, 2, 77));
        assertFalse(catalog.unchangedWay(11, 2, 89));
        assertFalse(catalog.unchangedWay(12, 2, 5));
        assertFalse(catalog.unchangedWay(15, 1, 0));
        assertFalse(catalog.unchangedWay(13, 1, 0));
        assertFalse(catalog.unchangedRelation(101, 1));

        assertEquals("N\t2\tU\nN\t3\tD\nN\t7\tC\n"
                + "W\t11\tU\nW\t12\tU\nW\t13\tC\nW\t15\tC\n"
                + "R\t100\tD\nR\t101\tC\n", changes(catalog));
    }

    @Test
    void changes_nothingChanged_empty() {
        VersionCatalog catalog = new VersionCatalog(table('N', false, new long[]{1, 1}),
                table('W', true), table('R', false));

        assertTrue(catalog.unchangedNode(1, 1));
        assertTrue(catalog.changes().isEmpty());
    }

    @Test
    void changes_manyRows_splitIntoChunksOfWholeLines() {
        int ways = 200_000;
        VersionCatalog.Table wayTable = new VersionCatalog.Table('W', ways, true);
        for (int id = 0; id < ways; id++) {
            wayTable.add(id, 1, id);
        }
        VersionCatalog catalog = new VersionCatalog(table('N', false), wayTable, table('R', false));

        List<byte[]> chunks = catalog.changes();

        assertTrue(chunks.size() > 1, String.valueOf(chunks.size()));
        for (byte[] chunk : chunks) {
            assertEquals('\n', chunk[chunk.length - 1]);
        }
        assertEquals(ways, changes(catalog).split("\n").length);
    }

    @Test
    void unchanged_concurrentLookups_everyObjectMarked() {
        int nodes = 100_000;
        VersionCatalog.Table nodeTable = new VersionCatalog.Table('N', nodes, false);
        for (int id = 1; id <= nodes; id++) {
            nodeTable.add(id * 2L, 1, 0);
        }
        VersionCatalog catalog = new VersionCatalog(nodeTable, table('W', true), table('R', false));

        IntStream.rangeClosed(1, nodes).parallel().forEach(id -> {
            catalog.unchangedNode(id * 2L, id % 10 == 0 ? 2 : 1);
            catalog.unchangedNode(id * 2L + 1, 1);
        });

        String changes = changes(catalog);
        assertFalse(changes.contains("\tD\n"));
        assertEquals(nodes / 10, changes.split("\tU\n", -1).length - 1);
        assertEquals(nodes, changes.split("\tC\n", -1).length - 1);
    }

    @Test
    void add_idsNotAscending_throws() {
        VersionCatalog.Table table = new VersionCatalog.Table('N', 3, false);
        table.add(5, 1, 0);

        assertThrows(IllegalStateException.class, () -> table.add(5, 1, 0));
        assertThrows(IllegalStateException.class, () -> table.add(4, 1, 0));
    }

    @Test
    void add_moreRowsThanCounted_throws() {
        VersionCatalog.Table table = new VersionCatalog.Table('R', 1, false);
        table.add(1, 1, 0);

        assertThrows(IllegalStateException.class, () -> table.add(2, 1, 0));
    }

    @Test
    void read_binaryCopy_rowsAdded() throws IOException {
        VersionCatalog.Table table = new VersionCatalog.Table('W', 2, true);

        table.read(new BinaryCopyInput(new ByteArrayInputStream(
                BinaryCopyInputTest.binaryCopy(new long[][]{{4, 2, -9}, {8, 1, 3}}))));

        assertEquals(2, table.size());
        assertTrue(table.unchanged(4, 2, -9));
        assertFalse(table.unchanged(8, 1, 4));
    }

    @Test
    void read_unexpectedColumnCount_throws() throws IOException {
        VersionCatalog.Table table = new VersionCatalog.Table('N', 1, false);
        BinaryCopyInput input = new BinaryCopyInput(new ByteArrayInputStream(
                BinaryCopyInputTest.binaryCopy(new long[][]{{4, 2, -9}})));

        assertThrows(IOException.class, () -> table.read(input));
    }
}
