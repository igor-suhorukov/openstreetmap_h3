package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.github.isuhorukov.osm.pgsnapshot.admin.AdminFixture.box;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdminFormatsTest {

    private static AdminUnits adminUnits;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void build() {
        adminUnits = new AdminFixture()
                .unit(1, 2, box(0, 0, 1, 1), "r2@subarea")
                .unit(2, 4, box(0, 0, 0.5, 1))
                .unitWithoutPolygon(3, 4)
                .build(AdminOptions.defaults(AdminColumns.UNITS));
    }

    @Test
    void binding_appendsArraysOrNullsForTheEnabledColumns() {
        AdminBinding binding = new AdminBinding(new long[]{5, 7}, new float[]{0.75f, 0.25f});
        StringBuilder row = new StringBuilder("x");
        AdminBinding.appendTsv(row, binding, AdminColumns.UNITS_AND_SHARES);
        AdminBinding.appendTsv(row, null, AdminColumns.UNITS_AND_SHARES);
        AdminBinding.appendTsv(row, binding, AdminColumns.UNITS);
        AdminBinding.appendTsv(row, binding, AdminColumns.NONE);
        assertEquals("x\t{5,7}\t{0.75,0.25}\t\\N\t\\N\t{5,7}", row.toString());
        assertThrows(IllegalArgumentException.class, () -> new AdminBinding(new long[]{1}, new float[0]));
    }

    @Test
    void columns_sqlFragments() {
        assertEquals("", AdminColumns.NONE.copyColumns());
        assertEquals(",admin_units", AdminColumns.UNITS.copyColumns());
        assertEquals(",admin_units,admin_unit_shares", AdminColumns.UNITS_AND_SHARES.copyColumns());
        assertEquals(",\n    admin_units bigint[],\n    admin_unit_shares real[]",
                AdminColumns.UNITS_AND_SHARES.columnDefinitions());
        assertEquals(", admin_units bigint[]", AdminColumns.UNITS.inlineColumnDefinitions());
        assertEquals("", AdminColumns.NONE.columnDefinitions() + AdminColumns.NONE.inlineColumnDefinitions()
                + AdminColumns.NONE.columnComments());
        assertTrue(AdminColumns.UNITS_AND_SHARES.columnComments().contains("COMMENT ON COLUMN multipolygon.admin_unit_shares"));
        assertFalse(AdminColumns.UNITS.columnComments().contains("admin_unit_shares"));
        assertEquals(AdminColumns.NONE, AdminColumns.of(false, true));
        assertEquals(AdminColumns.UNITS_AND_SHARES, AdminColumns.of(true, true));
    }

    @Test
    void names_followNameLangThenFallbacks() {
        Map<String, String> tags = new TreeMap<>(Map.of("name", "local", "name:en", "English", "name:th", "Thai",
                "official_name", "official", "int_name", "international", "ref", "R"));
        assertEquals("Thai", AdminNames.name(tags, List.of("th", "en")));
        assertEquals("local", AdminNames.name(tags, List.of("name", "en")));
        assertEquals("English", AdminNames.name(tags, List.of("de", "en")));
        assertEquals("local", AdminNames.name(tags, List.of("de")));
        assertEquals("international", AdminNames.name(Map.of("int_name", "international"), List.of("de")));
        assertEquals("A", AdminNames.name(Map.of("name:zh", "B", "name:ar", "A"), List.of("de")));
        assertNull(AdminNames.name(Map.of("ref", "R"), List.of("en")));
        assertEquals(List.of("int_name", "name", "name:en", "name:th", "official_name"),
                List.copyOf(AdminNames.names(tags).keySet()));
    }

    @Test
    void source_parsesOplTagsMembersAndEscapes() throws Exception {
        String opl = "n1 v1 x98 y18\n"
                + "r7 v1 Tboundary=administrative,admin_level=6,name=Ban%20%Mai%2c%x Mr8@subarea,w1@outer,n2@label\n"
                + "r8 v1 Tboundary=administrative,admin_level=8 M\n"
                + "r9 v1 Tboundary=administrative,admin_level=x M\n"
                + "r10 v1 Tboundary=land_area,admin_level=4 M\n";
        AdminSource source = AdminSource.readRelations(new ByteArrayInputStream(opl.getBytes(StandardCharsets.UTF_8)),
                MessageDigest.getInstance("SHA-256"));
        List<AdminUnit> units = source.getUnits();
        assertEquals(2, units.size());
        assertEquals("Ban Mai,x", units.get(0).getTags().get("name"));
        assertEquals(0, units.get(1).getSubareaParent());
        assertNull(AdminSource.parseLevel("100"));
        assertNull(AdminSource.parseLevel(""));
        assertEquals(10, AdminSource.parseLevel("10"));
        assertEquals("100%", AdminSource.oplUnescape("100%25%"));
        assertEquals("a%b", AdminSource.oplUnescape("a%b"));
    }

    @Test
    void source_readsOnlyRelationPolygonsOfUnits() throws Exception {
        AdminSource source = AdminSource.readRelations(new ByteArrayInputStream(
                "r5 v1 Tboundary=administrative,admin_level=4 M\n".getBytes(StandardCharsets.UTF_8)),
                MessageDigest.getInstance("SHA-256"));
        String polygons = "0101000000\tway\t5\t\n00\trelation\t6\t\n0102\trelation\t5\t\"a\"=>\"b\"\nbroken\n";
        source.readPolygons(new ByteArrayInputStream(polygons.getBytes(StandardCharsets.UTF_8)),
                MessageDigest.getInstance("SHA-256"));
        assertArrayEquals(new byte[]{1, 2}, source.getUnits().get(0).getEwkb());
    }

    @Test
    void databaseFiles_writeUnitRegionAndStateRows() throws IOException {
        AdminDatabaseFiles.write(adminUnits, tempDir.toFile());
        List<String> units = Files.readAllLines(tempDir.resolve(AdminDatabaseFiles.UNIT_TSV));
        assertEquals(3, units.size());
        String[] country = units.get(0).split("\t");
        assertEquals(List.of("1", "2", "\\N", "\\N", "\\N", "{1}", "0", "1", "\"unit1\""),
                List.of(country).subList(0, 9));
        assertTrue(country[12].startsWith("0106000020E6100000"));
        String[] province = units.get(1).split("\t");
        assertEquals(List.of("2", "4", "1", "S", "1.0", "{1,2}", "1", "0"), List.of(province).subList(0, 8));
        assertEquals("{}", province[11]);
        String[] withoutPolygon = units.get(2).split("\t");
        assertEquals(List.of("\\N", "\\N", "\\N"), List.of(withoutPolygon).subList(12, 15));
        List<String> regions = Files.readAllLines(tempDir.resolve(AdminDatabaseFiles.UNIT_REGION_TSV));
        assertTrue(regions.get(0).startsWith("1\t1\t2\t0103000020E6100000"));
        assertTrue(regions.get(regions.size() - 1).startsWith("2\t1\t4\t"));
        String state = Files.readString(tempDir.resolve(AdminDatabaseFiles.STATE_TSV));
        assertTrue(state.startsWith("load\t\\N\t\"{\\\"version\\\":1"));
        assertTrue(state.endsWith("\t3\t0\n"));
    }

    @Test
    void export_writesJsonLinesGeoJsonAndState() throws IOException {
        AdminExport.write(adminUnits, tempDir.toFile(), "{\"operation\":\"load\"}", "openstreetmap_h3:import_state");
        List<String> units = Files.readAllLines(tempDir.resolve(AdminExport.UNITS_JSONL));
        assertEquals(3, units.size());
        assertTrue(units.get(1).startsWith("{\"id\":2,\"admin_level\":4,\"parent_id\":1,\"parent_source\":\"S\","
                + "\"parent_share\":1.0,\"path\":[1,2],\"depth\":1,\"children\":0,\"name\":\"unit2\","
                + "\"names\":{\"name\":\"unit2\",\"name:en\":\"Unit 2\"},\"tags\":{"));
        assertTrue(units.get(2).endsWith("\"has_geometry\":false,\"area_m2\":null,\"own_area_m2\":null,\"h3_3\":[]}"));
        List<String> features = Files.readAllLines(tempDir.resolve(AdminExport.UNITS_GEOJSONL));
        assertEquals(2, features.size());
        assertTrue(features.get(0).contains("\"geometry\":{\"type\":\"MultiPolygon\",\"coordinates\":[[[[98,18],[99,18],"));
        List<String> regions = Files.readAllLines(tempDir.resolve(AdminExport.REGIONS_GEOJSONL));
        assertTrue(regions.get(0).startsWith("{\"type\":\"Feature\",\"properties\":{\"unit_id\":1,\"part_no\":1,"
                + "\"admin_level\":2},\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[["));
        String state = Files.readString(tempDir.resolve(AdminExport.STATE_JSON));
        assertTrue(state.startsWith("{\"openstreetmap_h3:import_state\":{\"operation\":\"load\"},"
                + "\"openstreetmap_h3:admin_units\":{\"version\":1,"));
        assertEquals("98.1234567", AdminExport.coordinate(98.12345670000001));
        assertEquals("-0.5", AdminExport.coordinate(-0.5));
        StringBuilder json = new StringBuilder();
        Point point = GridGeometry.FACTORY.createPoint();
        assertThrows(IllegalArgumentException.class, () -> AdminExport.geoJson(json, point));
    }

    @Test
    void cellCollector_growsAndPropagatesToAncestors() {
        AdminCellCollector.LongSet set = new AdminCellCollector.LongSet();
        for (int i = 0; i < 1000; i++) {
            set.add(i % 300);
        }
        assertEquals(300, set.values().length);
        assertEquals((7L << 16) | 0xFFFF, AdminCellCollector.key(7, (short) -1));
    }

    @Test
    void gridGeometry_clipSubdividePreservesArea() {
        StringBuilder wkt = new StringBuilder("POLYGON ((");
        int vertices = 3000;
        for (int i = 0; i <= vertices; i++) {
            double angle = 2 * Math.PI * (i % vertices) / vertices;
            wkt.append(i == 0 ? "" : ", ").append(Math.round((98.5 + 0.4 * Math.cos(angle)) * 1e7) / 1e7).append(' ')
                    .append(Math.round((18.5 + 0.4 * Math.sin(angle)) * 1e7) / 1e7);
        }
        Polygon circle = (Polygon) AdminFixture.read(wkt.append("))").toString());
        List<Polygon> pieces = GridGeometry.clipSubdivide(circle);
        assertTrue(pieces.size() > 10);
        double area = 0;
        for (Polygon piece : pieces) {
            assertTrue(piece.getNumPoints() <= GridGeometry.MAX_VERTICES + 8);
            area += piece.getArea();
        }
        assertEquals(circle.getArea(), area, circle.getArea() * 1e-6);
        assertEquals(GridGeometry.areaM2(circle), pieces.stream().mapToDouble(GridGeometry::areaM2).sum(), 1e3);
        assertTrue(GridGeometry.lengthM(circle.getExteriorRing()) > 270_000);
    }

    @Test
    void packedTree_findsBoxesOfALargeTree() {
        int count = 5000;
        int[] boxes = new int[4 * count];
        for (int i = 0; i < count; i++) {
            boxes[4 * i] = i * 10;
            boxes[4 * i + 1] = 0;
            boxes[4 * i + 2] = i * 10 + 5;
            boxes[4 * i + 3] = 5;
        }
        PackedTree tree = new PackedTree(boxes);
        PackedTree.Scratch scratch = new PackedTree.Scratch();
        assertEquals(1, tree.search(42_003, 1, 42_004, 2, scratch));
        assertEquals(4200, scratch.candidates[0]);
        assertEquals(0, tree.search(42_006, 1, 42_008, 2, scratch));
        assertEquals(0, new PackedTree(new int[0]).search(0, 0, 1, 1, scratch));
    }
}
