package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.IpcCompression;
import com.github.isuhorukov.osm.pgsnapshot.arrow.ArrowSchemas;
import com.github.isuhorukov.osm.pgsnapshot.arrow.ArrowSpill;
import com.github.isuhorukov.osm.pgsnapshot.util.CompactH3;
import com.github.isuhorukov.osm.pgsnapshot.util.GeometryH33;
import com.uber.h3core.H3Core;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MultipolygonBatchProcessorTest {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final Geometry CHIANG_MAI_SQUARE = multipolygon(98.99, 18.79, 0.001);
    private static final Geometry LARGE_SQUARE = multipolygon(97.0, 16.0, 5.0);
    private static final MultipolygonTags TEST_TAGS = MultipolygonTags.of(List.of(new TestTagExtension("test", "is.test", "v")));
    private static final ArrowSpill SPILL = ArrowSpill.of(IpcCompression.NONE, 0);

    private static H3Core h3Core;
    private static short chiangMai;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void createH3() throws IOException {
        h3Core = H3Core.newInstance();
        chiangMai = CompactH3.serialize3(h3Core.latLngToCell(18.79, 98.99, 3));
    }

    private static Geometry multipolygon(double longitude, double latitude, double size) {
        Polygon square = GEOMETRY_FACTORY.createPolygon(new Coordinate[]{
                new Coordinate(longitude, latitude), new Coordinate(longitude + size, latitude),
                new Coordinate(longitude + size, latitude + size), new Coordinate(longitude, latitude + size),
                new Coordinate(longitude, latitude)});
        return GEOMETRY_FACTORY.createMultiPolygon(new Polygon[]{square});
    }

    private static String ewkbHex(Geometry geometry) {
        Geometry withSrid = geometry.copy();
        withSrid.setSRID(4326);
        return WKBWriter.toHex(new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN, true).write(withSrid));
    }

    private static byte[] isoWkb(Geometry geometry) {
        return new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN).write(geometry);
    }

    private static String osmiumOutput() {
        return ewkbHex(CHIANG_MAI_SQUARE) + "\tway\t7\t\"building\"=>\"yes\"\n"
                + ewkbHex(CHIANG_MAI_SQUARE) + "\trelation\t30\t\"amenity\"=>\"school\",\"name\"=>\"a\\tb\"\n"
                + ewkbHex(LARGE_SQUARE) + "\trelation\t20\t\"landuse\"=>\"forest\",\"empty\"=>\"\"\n"
                + ewkbHex(CHIANG_MAI_SQUARE) + "\trelation\t10\t\n";
    }

    private static List<MultipolygonBatchProcessor.Result> process(MultipolygonBatchProcessor processor, String rows)
            throws IOException {
        List<MultipolygonBatchProcessor.Result> results = new ArrayList<>();
        MultipolygonRows.processRelations(new ByteArrayInputStream(rows.getBytes(StandardCharsets.UTF_8)), 0,
                processor::process, results::add);
        return results;
    }

    private static String text(ByteBuffer rows) {
        return new String(rows.array(), rows.arrayOffset() + rows.position(), rows.remaining(), StandardCharsets.UTF_8);
    }

    @Test
    void process_tsvAndArrow_enrichedRowsAndIsoWkbChunksPerCell() throws IOException {
        List<MultipolygonBatchProcessor.Result> results = process(
                new MultipolygonBatchProcessor(TEST_TAGS, true, SPILL, GeometryH33.instance()), osmiumOutput());

        assertEquals(1, results.size());
        ByteArrayOutputStream expectedTsv = new ByteArrayOutputStream();
        MultipolygonRows.copyRelations(new ByteArrayInputStream(osmiumOutput().getBytes(StandardCharsets.UTF_8)),
                expectedTsv::write, TEST_TAGS, 0);
        assertEquals(expectedTsv.toString(StandardCharsets.UTF_8), text(results.get(0).getTsvRows()));
        assertTrue(text(results.get(0).getTsvRows()).contains("\trelation\t30\t\"amenity\"=>\"school\",\"name\"=>\"a\\tb\","
                + "\"is.test\"=>\"v-Relation\"\t" + chiangMai + "\t\\N\n"));
        assertTrue(text(results.get(0).getTsvRows()).contains("\trelation\t20\t\"landuse\"=>\"forest\",\"empty\"=>\"\"\t32767\t{"));
        Map<Short, byte[]> chunks = results.get(0).getSpillChunks();
        assertEquals(List.of(chiangMai, GeometryH33.MULTIPLE_CELLS), new ArrayList<>(chunks.keySet()));
        assertEquals(List.of("30 " + chiangMai + " " + WKBWriter.toHex(isoWkb(CHIANG_MAI_SQUARE))
                        + " {amenity=school, is.test=v-Relation, name=a\tb}",
                "10 " + chiangMai + " " + WKBWriter.toHex(isoWkb(CHIANG_MAI_SQUARE)) + " {}"), rows(chunks.get(chiangMai)));
        assertEquals(List.of("20 32767 " + WKBWriter.toHex(isoWkb(LARGE_SQUARE)) + " {landuse=forest}"),
                rows(chunks.get(GeometryH33.MULTIPLE_CELLS)));
    }

    @Test
    void process_tsvOnly_rowsWithCellsNoChunks() throws IOException {
        List<MultipolygonBatchProcessor.Result> results = process(
                new MultipolygonBatchProcessor(MultipolygonTags.NONE, true, null, GeometryH33.instance()), osmiumOutput());

        ByteArrayOutputStream expectedTsv = new ByteArrayOutputStream();
        MultipolygonRows.copyRelations(new ByteArrayInputStream(osmiumOutput().getBytes(StandardCharsets.UTF_8)),
                expectedTsv::write, MultipolygonTags.NONE, 0);
        assertEquals(expectedTsv.toString(StandardCharsets.UTF_8), text(results.get(0).getTsvRows()));
        assertTrue(text(results.get(0).getTsvRows()).endsWith("\trelation\t10\t\t" + chiangMai + "\t\\N\n"));
        assertEquals(Map.of(), results.get(0).getSpillChunks());
    }

    @Test
    void process_arrowOnly_noTsvRows() throws IOException {
        List<MultipolygonBatchProcessor.Result> results = process(
                new MultipolygonBatchProcessor(TEST_TAGS, false, SPILL, GeometryH33.instance()), osmiumOutput());

        assertNull(results.get(0).getTsvRows());
        assertEquals(2, results.get(0).getSpillChunks().size());
    }

    @Test
    void process_invalidGeometry_throwsWithRelationId() {
        MultipolygonBatchProcessor processor = new MultipolygonBatchProcessor(TEST_TAGS, false, SPILL, GeometryH33.instance());

        assertEquals("Invalid WKB of multipolygon 77", assertThrows(IllegalStateException.class,
                () -> process(processor, "010203\trelation\t77\t\n")).getMessage());
        assertEquals("Invalid hex WKB of multipolygon 78", assertThrows(IllegalStateException.class,
                () -> process(processor, "01XY\trelation\t78\t\n")).getMessage());
        assertEquals("Odd length of hex WKB of multipolygon 79", assertThrows(IllegalStateException.class,
                () -> process(processor, "010\trelation\t79\t\n")).getMessage());
    }

    @Test
    void hexBytes_upperAndLowerCaseDigits_decoded() {
        byte[] hex = "09afAF".getBytes(StandardCharsets.US_ASCII);

        assertArrayEquals(new byte[]{0x09, (byte) 0xaf, (byte) 0xaf}, MultipolygonRows.hexBytes(hex, 0, 6, 1));
    }

    private List<String> rows(byte[] chunk) throws IOException {
        File file = tempDir.resolve("chunk" + System.nanoTime() + ".arrows").toFile();
        Files.write(file.toPath(), chunk);
        List<String> rows = new ArrayList<>();
        try (BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = ArrowSpill.reader(allocator, ArrowSchemas.getMultipolygonSchema(), List.of(file))) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                BigIntVector ids = (BigIntVector) root.getVector(ArrowSchemas.ID);
                SmallIntVector h33 = (SmallIntVector) root.getVector(ArrowSchemas.H33);
                VarBinaryVector wkb = (VarBinaryVector) root.getVector(ArrowSchemas.WKB);
                UnionMapReader tags = ((MapVector) root.getVector(ArrowSchemas.TAGS)).getReader();
                for (int row = 0; row < root.getRowCount(); row++) {
                    List<String> pairs = new ArrayList<>();
                    tags.setPosition(row);
                    while (tags.next()) {
                        pairs.add(tags.key().readText() + "=" + tags.value().readText());
                    }
                    rows.add(ids.get(row) + " " + h33.get(row) + " " + WKBWriter.toHex(wkb.get(row)) + " {"
                            + String.join(", ", pairs) + "}");
                }
            }
        }
        return rows;
    }
}
