package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.pipeline.StoredVersions;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongArray;

final class VersionCatalog implements StoredVersions {

    private static final Logger log = LoggerFactory.getLogger(VersionCatalog.class);
    private static final int CHANGES_CHUNK_SIZE = 1 << 20;

    static final TableQuery NODE_QUERY = new TableQuery('N', "SELECT count(*) FROM nodes",
            "COPY (SELECT id, version FROM nodes ORDER BY id) TO STDOUT (FORMAT binary)", false);
    static final TableQuery WAY_QUERY = new TableQuery('W', "SELECT count(*) FROM ways",
            "COPY (SELECT id, version, geom_hash FROM ways ORDER BY id) TO STDOUT (FORMAT binary)", true);
    static final TableQuery RELATION_QUERY = new TableQuery('R', "SELECT count(*) FROM relations",
            "COPY (SELECT id, version FROM relations ORDER BY id) TO STDOUT (FORMAT binary)", false);

    private final Table nodes;
    private final Table ways;
    private final Table relations;

    VersionCatalog(Table nodes, Table ways, Table relations) {
        this.nodes = nodes;
        this.ways = ways;
        this.relations = relations;
    }

    static VersionCatalog read(OpenedConnections connections) throws IOException, InterruptedException, SQLException {
        long start = System.currentTimeMillis();
        Table[] tables = new Table[3];
        Map<String, ParallelTasks.Task> tasks = new LinkedHashMap<>();
        tasks.put("catalog-nodes", () -> tables[0] = read(connections, NODE_QUERY));
        tasks.put("catalog-ways", () -> tables[1] = read(connections, WAY_QUERY));
        tasks.put("catalog-relations", () -> tables[2] = read(connections, RELATION_QUERY));
        ParallelTasks.run(tasks);
        log.info("version catalog read in {} ms: {} nodes, {} ways, {} relations", System.currentTimeMillis() - start,
                tables[0].size(), tables[1].size(), tables[2].size());
        return new VersionCatalog(tables[0], tables[1], tables[2]);
    }

    private static Table read(OpenedConnections connections, TableQuery query) throws SQLException, IOException {
        try (Connection connection = connections.open("catalog " + query.type)) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setReadOnly(true);
            long count;
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(query.countSql)) {
                resultSet.next();
                count = resultSet.getLong(1);
            }
            Table table = new Table(query.type, Math.toIntExact(count), query.withHash);
            try (InputStream copy = new PGCopyInputStream(connection.unwrap(PGConnection.class), query.copySql)) {
                table.read(new BinaryCopyInput(copy));
            }
            connection.commit();
            if (table.size() != count) {
                throw new IllegalStateException(query.type + " catalog has " + table.size() + " rows instead of " + count);
            }
            return table;
        }
    }

    @Override
    public boolean unchangedNode(long id, int version) {
        return nodes.unchanged(id, version, 0);
    }

    @Override
    public boolean unchangedWay(long id, int version, long geomHash) {
        return ways.unchanged(id, version, geomHash);
    }

    @Override
    public boolean unchangedRelation(long id, int version) {
        return relations.unchanged(id, version, 0);
    }

    List<byte[]> changes() {
        ChangeChunks chunks = new ChangeChunks();
        for (Table table : List.of(nodes, ways, relations)) {
            table.writeChanges(chunks);
        }
        return chunks.finish();
    }

    static final class TableQuery {
        private final char type;
        private final String countSql;
        private final String copySql;
        private final boolean withHash;

        private TableQuery(char type, String countSql, String copySql, boolean withHash) {
            this.type = type;
            this.countSql = countSql;
            this.copySql = copySql;
            this.withHash = withHash;
        }
    }

    static final class Table {
        private final char type;
        private final long[] ids;
        private final int[] versions;
        private final long[] hashes;
        private int size;
        private final AtomicLongArray seen;
        private final AtomicLongArray changed;
        private long[] created = new long[16];
        private int createdCount;

        Table(char type, int capacity, boolean withHash) {
            this.type = type;
            this.ids = new long[capacity];
            this.versions = new int[capacity];
            this.hashes = withHash ? new long[capacity] : null;
            this.seen = new AtomicLongArray((capacity >>> 6) + 1);
            this.changed = new AtomicLongArray((capacity >>> 6) + 1);
        }

        void read(BinaryCopyInput input) throws IOException {
            int fieldCount = hashes != null ? 3 : 2;
            for (int fields = input.nextTuple(); fields != BinaryCopyInput.END_OF_DATA; fields = input.nextTuple()) {
                if (fields != fieldCount) {
                    throw new IOException("Expected " + fieldCount + " columns in " + type + " catalog, got " + fields);
                }
                long id = input.readLongField();
                int version = input.readIntField();
                add(id, version, hashes != null ? input.readLongField() : 0);
            }
        }

        void add(long id, int version, long hash) {
            if (size == ids.length) {
                throw new IllegalStateException(type + " catalog has more rows than counted: " + ids.length);
            }
            if (size > 0 && id <= ids[size - 1]) {
                throw new IllegalStateException(type + " catalog ids should strictly ascend: " + ids[size - 1] + ", " + id);
            }
            ids[size] = id;
            versions[size] = version;
            if (hashes != null) {
                hashes[size] = hash;
            }
            size++;
        }

        int size() {
            return size;
        }

        boolean unchanged(long id, int version, long hash) {
            int index = Arrays.binarySearch(ids, 0, size, id);
            if (index < 0) {
                addCreated(id);
                return false;
            }
            mark(seen, index);
            if (versions[index] == version && (hashes == null || hashes[index] == hash)) {
                return true;
            }
            mark(changed, index);
            return false;
        }

        private synchronized void addCreated(long id) {
            if (createdCount == created.length) {
                created = Arrays.copyOf(created, createdCount * 2);
            }
            created[createdCount++] = id;
        }

        private static void mark(AtomicLongArray bits, int index) {
            long mask = 1L << (index & 63);
            int word = index >>> 6;
            long current = bits.get(word);
            while ((current & mask) == 0 && !bits.compareAndSet(word, current, current | mask)) {
                current = bits.get(word);
            }
        }

        private static boolean isSet(AtomicLongArray bits, int index) {
            return (bits.get(index >>> 6) & (1L << (index & 63))) != 0;
        }

        synchronized void writeChanges(ChangeChunks chunks) {
            long updated = 0;
            long deleted = 0;
            for (int i = 0; i < size; i++) {
                if (!isSet(seen, i)) {
                    chunks.add(type, ids[i], 'D');
                    deleted++;
                } else if (isSet(changed, i)) {
                    chunks.add(type, ids[i], 'U');
                    updated++;
                }
            }
            long[] sortedCreated = Arrays.copyOf(created, createdCount);
            Arrays.sort(sortedCreated);
            for (long id : sortedCreated) {
                chunks.add(type, id, 'C');
            }
            log.info("{}: {} created, {} updated, {} deleted", type, createdCount, updated, deleted);
        }
    }

    static final class ChangeChunks {
        private final List<byte[]> chunks = new ArrayList<>();
        private final StringBuilder chunk = new StringBuilder();

        void add(char type, long id, char operation) {
            chunk.append(type).append('\t').append(id).append('\t').append(operation).append('\n');
            if (chunk.length() >= CHANGES_CHUNK_SIZE) {
                flush();
            }
        }

        private void flush() {
            if (chunk.length() > 0) {
                chunks.add(chunk.toString().getBytes(StandardCharsets.US_ASCII));
                chunk.setLength(0);
            }
        }

        List<byte[]> finish() {
            flush();
            return chunks;
        }
    }
}
