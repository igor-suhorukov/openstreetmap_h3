package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.util.GeometryH33;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonFixtures.cell;
import static com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonFixtures.ewkbHex;
import static com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonFixtures.square;
import static org.junit.jupiter.api.Assertions.*;

class MultipolygonRowsTest {

    private static final String CHIANG_MAI = ewkbHex(square(98.99, 18.79, 0.001));
    private static final String BANGKOK = ewkbHex(square(100.50, 13.75, 0.001));
    private static final String ACROSS_CELLS = ewkbHex(square(98.0, 18.0, 2.0));
    private static final short CHIANG_MAI_CELL = cell(18.79, 98.99);
    private static final short BANGKOK_CELL = cell(13.75, 100.50);

    private static final String OSMIUM_OUTPUT = CHIANG_MAI + "\tway\t1\t\"amenity\"=>\"cafe\"\n"
            + CHIANG_MAI + "\trelation\t2\t\"landuse\"=>\"forest\"\n"
            + ACROSS_CELLS + "\trelation\t3\t\"amenity\"=>\"school\"\n"
            + BANGKOK + "\trelation\t4\t\n"
            + CHIANG_MAI + "\trelation\t5\t\"amenity\"=>\"cafe\",\"name\"=>\"a\\tb\"";

    private static final MultipolygonTags TEST_TAGS = MultipolygonTags.of(List.of(new TestTagExtension("test", "is.test", "v")));

    private static String acrossCellsColumns() {
        short[] cells = GeometryH33.instance().cells(square(98.0, 18.0, 2.0));
        StringBuilder regions = new StringBuilder();
        for (short cell : cells) {
            regions.append(regions.length() == 0 ? "{" : ",").append(cell);
        }
        return "\t32767\t" + regions + "}\n";
    }

    private static String importRows(boolean enriched) {
        String extensionTag = enriched ? ",\"is.test\"=>\"v-Relation\"" : "";
        return CHIANG_MAI + "\trelation\t2\t\"landuse\"=>\"forest\"\t" + CHIANG_MAI_CELL + "\t\\N\n"
                + ACROSS_CELLS + "\trelation\t3\t\"amenity\"=>\"school\"" + extensionTag + acrossCellsColumns()
                + BANGKOK + "\trelation\t4\t\t" + BANGKOK_CELL + "\t\\N\n"
                + CHIANG_MAI + "\trelation\t5\t\"amenity\"=>\"cafe\",\"name\"=>\"a\\tb\"" + extensionTag + "\t"
                + CHIANG_MAI_CELL + "\t\\N\n";
    }

    private static InputStream input(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    private static String rows(int count) {
        StringBuilder rows = new StringBuilder();
        for (int id = 1; id <= count; id++) {
            rows.append(id % 7 == 0 ? BANGKOK : CHIANG_MAI).append('\t').append(id % 5 == 0 ? "way" : "relation")
                    .append('\t').append(id).append('\t')
                    .append(id % 3 == 0 ? "\"amenity\"=>\"cafe\"" : "\"landuse\"=>\"forest\"").append('\n');
        }
        return rows.toString();
    }

    private static List<Long> ids(MultipolygonRows.Batch batch) {
        List<Long> ids = new ArrayList<>();
        for (int row = 0; row < batch.size(); row++) {
            ids.add(MultipolygonTags.relationId(batch.bytes(), batch.start(row), batch.end(row)));
        }
        return ids;
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 4})
    void copyRelations_extensionTags_appendedToMatchingRelationRowsWithCells(int workers) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        assertEquals(4, MultipolygonRows.copyRelations(input(OSMIUM_OUTPUT), output::write, TEST_TAGS, workers));

        assertEquals(importRows(true), output.toString(StandardCharsets.UTF_8));
    }

    @Test
    void copyRelations_extensionTagsInSmallReads_sameRows() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        InputStream smallReads = new FilterInputStream(input(OSMIUM_OUTPUT)) {
            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                return super.read(buffer, offset, Math.min(length, 3));
            }
        };

        assertEquals(4, MultipolygonRows.copyRelations(smallReads, output::write, TEST_TAGS, 2));

        assertEquals(importRows(true), output.toString(StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 3})
    void copyRelations_noMultipolygonTags_relationRowsWithCells(int workers) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        assertEquals(4, MultipolygonRows.copyRelations(input(OSMIUM_OUTPUT), output::write, MultipolygonTags.NONE,
                workers));

        assertEquals(importRows(false), output.toString(StandardCharsets.UTF_8));
    }

    @Test
    void copyRelations_manyRowsInParallel_sameBytesAsOneThread() throws IOException {
        String osmiumOutput = rows(20_000);
        ByteArrayOutputStream oneThread = new ByteArrayOutputStream();
        ByteArrayOutputStream parallel = new ByteArrayOutputStream();

        long rows = MultipolygonRows.copyRelations(input(osmiumOutput), oneThread::write, TEST_TAGS, 0);

        assertEquals(16_000, rows);
        assertEquals(rows, MultipolygonRows.copyRelations(input(osmiumOutput), parallel::write, TEST_TAGS, 4));
        assertEquals(oneThread.toString(StandardCharsets.UTF_8), parallel.toString(StandardCharsets.UTF_8));
        assertTrue(oneThread.toString(StandardCharsets.UTF_8)
                .contains("\t3\t\"amenity\"=>\"cafe\",\"is.test\"=>\"v-Relation\"\t" + CHIANG_MAI_CELL + "\t\\N\n"));
    }

    @Test
    void processRelations_firstBatchFinishesLast_resultsInInputOrder() throws IOException {
        CountDownLatch laterBatchProcessed = new CountDownLatch(1);
        List<Long> results = new ArrayList<>();

        long rows = MultipolygonRows.processRelations(input(rows(500)), 4, 64, 3, batch -> {
            List<Long> ids = ids(batch);
            if (ids.contains(1L)) {
                assertTrue(await(laterBatchProcessed));
            } else {
                laterBatchProcessed.countDown();
            }
            return ids;
        }, results::addAll);

        assertEquals(400, rows);
        List<Long> expected = new ArrayList<>();
        for (long id = 1; id <= 500; id++) {
            if (id % 5 != 0) {
                expected.add(id);
            }
        }
        assertEquals(expected, results);
    }

    @Test
    void processRelations_rowLongerThanBatchLimit_batchOfOneRow() throws IOException {
        List<Integer> batchSizes = new ArrayList<>();

        MultipolygonRows.processRelations(input("0106" + "AB".repeat(100) + "\trelation\t1\t\n"
                        + "0106\trelation\t2\t\n" + "0106\trelation\t3\t\n"), 2, 32, 10,
                MultipolygonRows.Batch::size, batchSizes::add);

        assertEquals(List.of(1, 2), batchSizes);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    void processRelations_processorFails_sameException(int workers) {
        InputStream osmiumOutput = input(rows(100));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> MultipolygonRows.processRelations(osmiumOutput, workers, 64, 2, batch -> {
                    if (ids(batch).contains(42L)) {
                        throw new IllegalStateException("bad row 42");
                    }
                    return batch.size();
                }, size -> { }));

        assertEquals("bad row 42", exception.getMessage());
    }

    @Test
    void copyRelations_extensionErrorInWorker_sameError() {
        MultipolygonTags failing = MultipolygonTags.of(List.of(new TestTagExtension("test", "is.test", "error")));
        InputStream osmiumOutput = input(OSMIUM_OUTPUT);

        LinkageError error = assertThrows(LinkageError.class,
                () -> MultipolygonRows.copyRelations(osmiumOutput, (buffer, offset, length) -> { }, failing, 2));

        assertEquals("test extension error", error.getMessage());
    }

    @Test
    void processRelations_sinkFails_stopsWithSinkException() {
        InputStream osmiumOutput = input(rows(10_000));

        IOException exception = assertTimeoutPreemptively(Duration.ofSeconds(20), () -> assertThrows(IOException.class,
                () -> MultipolygonRows.processRelations(osmiumOutput, 4, 64, 2, MultipolygonRows.Batch::size,
                        size -> {
                            throw new IOException("disk full");
                        })));

        assertEquals("disk full", exception.getMessage());
    }

    @Test
    void processRelations_interruptedWhileWaitingForWorker_interruptedIOException() throws InterruptedException {
        CountDownLatch processing = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> interrupted = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            try {
                MultipolygonRows.processRelations(input(rows(100)), 1, 64, 1, batch -> {
                    processing.countDown();
                    await(neverReleased);
                    return batch.size();
                }, size -> { });
            } catch (IOException | RuntimeException e) {
                failure.set(e);
            }
            interrupted.set(Thread.currentThread().isInterrupted());
        });
        reader.start();
        assertTrue(processing.await(20, TimeUnit.SECONDS));

        reader.interrupt();
        reader.join(TimeUnit.SECONDS.toMillis(20));

        assertFalse(reader.isAlive());
        assertInstanceOf(InterruptedIOException.class, failure.get());
        assertEquals("multipolygon processing interrupted", failure.get().getMessage());
        assertTrue(interrupted.get());
    }

    @Test
    void processRelations_emptyInput_noBatches() throws IOException {
        List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());

        assertEquals(0, MultipolygonRows.processRelations(input(""), 2, MultipolygonRows.Batch::size, batchSizes::add));

        assertEquals(List.of(), batchSizes);
    }

    @Test
    void isRelation_secondColumnOnly() {
        assertTrue(isRelation("geom\trelation\t1\t\\N"));
        assertFalse(isRelation("geom\tway\t1\t\\N"));
        assertFalse(isRelation("geom\trelations\t1"));
        assertFalse(isRelation("geom\trelation"));
        assertFalse(isRelation("no tabs"));
        assertFalse(isRelation("a\tb\trelation\t"));
    }

    private static boolean isRelation(String line) {
        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        return MultipolygonRows.isRelation(bytes, 0, bytes.length);
    }

    private static boolean await(CountDownLatch latch) {
        try {
            return latch.await(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
