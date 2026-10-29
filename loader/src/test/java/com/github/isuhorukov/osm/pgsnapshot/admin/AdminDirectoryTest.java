package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.isuhorukov.osm.pgsnapshot.admin.AdminFixture.LAT;
import static com.github.isuhorukov.osm.pgsnapshot.admin.AdminFixture.LON;
import static com.github.isuhorukov.osm.pgsnapshot.admin.AdminFixture.box;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdminDirectoryTest {

    private static final List<Tag> BUILDING = List.of(new Tag("building", "yes"));
    private static final AdminOptions OPTIONS = AdminOptions.defaults(AdminColumns.UNITS_AND_SHARES);

    private static AdminUnits adminUnits;

    @TempDir
    Path tempDir;

    static AdminFixture standard() {
        return new AdminFixture()
                .unit(1, 2, box(0, 0, 2, 2), "r10@subarea", "r11@subarea")
                .unit(10, 4, box(0, 0, 1, 2), "r60@subarea")
                .unit(11, 4, box(1, 0, 2, 2))
                .unit(20, 6, box(0, 1, 1, 2), "r30@subarea")
                .unit(21, 6, box(0, 0, 1, 1), "r31@subarea", "r50@subarea")
                .unit(22, 6, box(1, 0, 2, 1.5))
                .unit(30, 8, box(0.2, 1.2, 0.6, 1.6))
                .unit(31, 8, box(0.2, 0.2, 0.6, 0.6))
                .unit(40, 7, box(0.5, 1.3, 0.8, 1.5))
                .unit(50, 8, box(0.3, 0.7, 1.3, 0.8))
                .unit(60, 6, box(1.6, 1.6, 1.9, 1.9))
                .unitWithoutPolygon(70, 4, "r71@subarea")
                .unit(71, 6, box(2.5, 0, 3, 0.5));
    }

    @BeforeAll
    static void buildStandard() {
        adminUnits = standard().build(OPTIONS);
    }

    private static Map<Long, AdminUnit> unitsById(AdminUnits units) {
        Map<Long, AdminUnit> byId = new HashMap<>();
        for (AdminUnit unit : units.getDirectory().getUnits()) {
            byId.put(unit.getId(), unit);
        }
        return byId;
    }

    private static long parentId(AdminUnits units, long id) {
        AdminUnit parent = units.getDirectory().parent(unitsById(units).get(id));
        return parent == null ? -1 : parent.getId();
    }

    private static long[] node(AdminUnits units, double x, double y) {
        AdminBinding binding = units.getBinder().node((short) 0, LON + x, LAT + y, BUILDING);
        return binding == null ? null : binding.getUnitIds();
    }

    private static AdminBinding area(AdminUnits units, String wkt) {
        return units.getBinder().multipolygon((short) 0, AdminFixture.read(wkt), BUILDING);
    }

    private static List<WayNode> wayNodes(double... xy) {
        List<WayNode> nodes = new ArrayList<>();
        for (int i = 0; i < xy.length; i += 2) {
            nodes.add(new WayNode(i, LAT + xy[i + 1], LON + xy[i]));
        }
        return nodes;
    }

    @Test
    void tree_followsGeometryAndConfirmedSubareaLinks() {
        assertEquals(-1, parentId(adminUnits, 1));
        assertEquals(1, parentId(adminUnits, 10));
        assertEquals(AdminUnit.PARENT_SUBAREA, unitsById(adminUnits).get(10L).getParentSource());
        assertEquals(10, parentId(adminUnits, 20));
        assertEquals(AdminUnit.PARENT_GEOMETRY, unitsById(adminUnits).get(20L).getParentSource());
        assertEquals(11, parentId(adminUnits, 22));
        assertEquals(20, parentId(adminUnits, 30));
        assertEquals(AdminUnit.PARENT_SUBAREA, unitsById(adminUnits).get(30L).getParentSource());
        assertEquals(20, parentId(adminUnits, 40));
        assertArrayEquals(new long[]{1, 10, 20, 30}, adminUnits.getDirectory().pathIds(unitsById(adminUnits).get(30L)));
        assertEquals(3, unitsById(adminUnits).get(30L).getDepth());
        assertEquals(2, unitsById(adminUnits).get(20L).getChildren());
    }

    @Test
    void tree_childCutByDistrictsTakesItsSubareaParent() {
        AdminUnit tambon = unitsById(adminUnits).get(50L);
        assertEquals(21, parentId(adminUnits, 50));
        assertEquals(AdminUnit.PARENT_SUBAREA, tambon.getParentSource());
        assertEquals(0.7, tambon.getParentShare(), 1e-6);
    }

    @Test
    void tree_staleSubareaLinkIsIgnored() {
        assertEquals(11, parentId(adminUnits, 60));
        assertEquals(AdminUnit.PARENT_GEOMETRY, unitsById(adminUnits).get(60L).getParentSource());
    }

    @Test
    void tree_parentWithoutPolygonComesFromSubarea() {
        assertEquals(70, parentId(adminUnits, 71));
        assertNull(adminUnits.getDirectory().parentShare(unitsById(adminUnits).get(71L)));
        assertTrue(!unitsById(adminUnits).get(70L).hasGeometry());
    }

    @Test
    void node_leafUnitOfItsOwnTerritory() {
        assertArrayEquals(new long[]{30}, node(adminUnits, 0.3, 1.3));
        assertArrayEquals(new long[]{30, 40}, node(adminUnits, 0.55, 1.4));
        assertArrayEquals(new long[]{40}, node(adminUnits, 0.7, 1.4));
        assertArrayEquals(new long[]{20}, node(adminUnits, 0.1, 1.1));
        assertArrayEquals(new long[]{11}, node(adminUnits, 1.5, 1.7));
        assertArrayEquals(new long[]{60}, node(adminUnits, 1.7, 1.7));
        assertArrayEquals(new long[]{71}, node(adminUnits, 2.7, 0.2));
        assertArrayEquals(new long[]{50, 22}, node(adminUnits, 1.2, 0.75));
        assertNull(node(adminUnits, 3.5, 0));
    }

    @Test
    void node_exactlyOnSharedBoundaryGetsBothUnits() {
        assertArrayEquals(new long[]{21, 22}, node(adminUnits, 1.0, 0.5));
    }

    @Test
    void area_splitByBoundaryGetsSharesInDescendingOrder() {
        AdminBinding straddling = area(adminUnits, box(0.95, 0.4, 1.05, 0.5));
        assertArrayEquals(new long[]{21, 22}, straddling.getUnitIds());
        assertEquals(0.5f, straddling.getShares()[0], 1e-4);
        assertEquals(0.5f, straddling.getShares()[1], 1e-4);

        AdminBinding reserve = area(adminUnits, box(0.65, 0.9, 0.75, 1.35));
        assertArrayEquals(new long[]{20, 21, 40}, reserve.getUnitIds());
        assertEquals(0.3f / 0.45f, reserve.getShares()[0], 1e-4);
        assertEquals(0.1f / 0.45f, reserve.getShares()[1], 1e-4);
        assertEquals(0.05f / 0.45f, reserve.getShares()[2], 1e-4);
    }

    @Test
    void area_smallPieceBelowThresholdsIsDropped() {
        AdminBinding sliver = area(adminUnits, box(0.99, 0.3, 1.00004, 0.301));
        assertArrayEquals(new long[]{21}, sliver.getUnitIds());
        assertTrue(sliver.getShares()[0] < 1f);
    }

    @Test
    void area_insideOneUnitHasShareOne() {
        AdminBinding inside = area(adminUnits, box(0.3, 0.3, 0.4, 0.4));
        assertArrayEquals(new long[]{31}, inside.getUnitIds());
        assertEquals(1f, inside.getShares()[0]);
        assertNull(area(adminUnits, box(3.2, 0, 3.3, 0.1)));
    }

    @Test
    void way_lineAcrossAndAlongBoundary() {
        AdminBinding across = adminUnits.getBinder().way((short) 0, wayNodes(0.9, 0.4, 1.1, 0.4), BUILDING);
        assertArrayEquals(new long[]{21, 22}, across.getUnitIds());
        assertEquals(0.5f, across.getShares()[0], 1e-4);

        AdminBinding along = adminUnits.getBinder().way((short) 0, wayNodes(1.0, 0.1, 1.0, 0.3), BUILDING);
        assertArrayEquals(new long[]{21, 22}, along.getUnitIds());
        assertEquals(1f, along.getShares()[0], 1e-4);
        assertEquals(1f, along.getShares()[1], 1e-4);

        AdminBinding closed = adminUnits.getBinder().way((short) 0,
                wayNodes(0.3, 0.3, 0.4, 0.3, 0.4, 0.4, 0.3, 0.4, 0.3, 0.3), BUILDING);
        assertArrayEquals(new long[]{31}, closed.getUnitIds());
        AdminBinding single = adminUnits.getBinder().way((short) 0, wayNodes(0.3, 0.3), BUILDING);
        assertArrayEquals(new long[]{31}, single.getUnitIds());
    }

    @Test
    void binder_skipsObjectsOutsideTheList() {
        AdminBinder binder = adminUnits.getBinder();
        assertNull(binder.node((short) 0, LON + 0.3, LAT + 1.3, List.of(new Tag("highway", "traffic_signals"))));
        assertNull(binder.way((short) 0, wayNodes(0.9, 0.4, 1.1, 0.4),
                List.of(new Tag("boundary", "administrative"), new Tag("is.poi", "x"))));
        assertNull(binder.way((short) 0, List.of(), BUILDING));
        assertNull(binder.multipolygon((short) 0, null, BUILDING));
    }

    @Test
    void protrudingChild_objectInProtrusionGetsBothBranches() {
        AdminUnits units = new AdminFixture()
                .unit(1, 4, box(0, 0, 1, 1))
                .unit(2, 4, box(1, 0, 2, 1))
                .unit(3, 6, box(0.1, 0.1, 1.05, 0.9))
                .build(OPTIONS);
        assertEquals(1, parentId(units, 3));
        assertArrayEquals(new long[]{3, 2}, node(units, 1.02, 0.5));
        assertArrayEquals(new long[]{3}, node(units, 0.5, 0.5));
        assertArrayEquals(new long[]{1}, node(units, 0.05, 0.5));
    }

    @Test
    void objectEnclosingForeignIsland_getsExactShares() {
        String withHole = "POLYGON ((" + LON + " " + LAT + ", " + (LON + 1) + " " + LAT + ", " + (LON + 1) + " "
                + (LAT + 1) + ", " + LON + " " + (LAT + 1) + ", " + LON + " " + LAT + "), ("
                + (LON + 0.4) + " " + (LAT + 0.4) + ", " + (LON + 0.4) + " " + (LAT + 0.6) + ", "
                + (LON + 0.6) + " " + (LAT + 0.6) + ", " + (LON + 0.6) + " " + (LAT + 0.4) + ", "
                + (LON + 0.4) + " " + (LAT + 0.4) + "))";
        AdminUnits units = new AdminFixture()
                .unit(1, 6, withHole)
                .unit(2, 6, box(0.4, 0.4, 0.6, 0.6))
                .build(OPTIONS);
        AdminBinding binding = area(units, box(0.3, 0.3, 0.7, 0.7));
        assertArrayEquals(new long[]{1, 2}, binding.getUnitIds());
        assertEquals(0.75f, binding.getShares()[0], 1e-4);
        assertEquals(0.25f, binding.getShares()[1], 1e-4);
    }

    @Test
    void largeObject_isSubdividedBeforeBinding() {
        StringBuilder wkt = new StringBuilder("POLYGON ((");
        int vertices = 2000;
        for (int i = 0; i <= vertices; i++) {
            double angle = 2 * Math.PI * (i % vertices) / vertices;
            wkt.append(i == 0 ? "" : ", ").append(LON + 1 + 0.3 * Math.cos(angle)).append(' ')
                    .append(LAT + 0.5 + 0.3 * Math.sin(angle));
        }
        Geometry circle = AdminFixture.read(wkt.append("))").toString());
        AdminBinding binding = adminUnits.getBinder().multipolygon((short) 0, circle, BUILDING);
        assertArrayEquals(new long[]{22, 21, 50}, binding.getUnitIds());
        assertEquals(0.5f, binding.getShares()[0], 1e-3f);
        assertTrue(binding.getShares()[1] < 0.5f && binding.getShares()[2] > 0.01f);
    }

    @Test
    void finish_collectsCellsOfDescendants() {
        AdminUnits units = standard().build(OPTIONS);
        units.getBinder().node((short) 100, LON + 0.3, LAT + 1.3, BUILDING);
        units.getBinder().node((short) 200, LON + 1.5, LAT + 1.7, BUILDING);
        units.getBinder().multipolygon(Short.MAX_VALUE, AdminFixture.read(box(0.95, 0.4, 1.05, 0.5)), BUILDING);
        units.finish();
        Map<Long, AdminUnit> byId = unitsById(units);
        assertArrayEquals(new short[]{100}, byId.get(30L).getCells());
        assertArrayEquals(new short[]{100}, byId.get(20L).getCells());
        assertArrayEquals(new short[]{100, Short.MAX_VALUE}, byId.get(10L).getCells());
        assertArrayEquals(new short[]{100, 200, Short.MAX_VALUE}, byId.get(1L).getCells());
        assertArrayEquals(new short[]{200, Short.MAX_VALUE}, byId.get(11L).getCells());
        assertArrayEquals(new short[0], byId.get(31L).getCells());
        assertEquals(3, units.boundObjects());
        assertTrue(units.reportMetadataJson().contains("\"nodes\":{\"bound\":2,\"multiple\":0,\"outside\":0"));
    }

    @Test
    void ownArea_isUnitMinusDescendants() {
        Map<Long, AdminUnit> byId = unitsById(adminUnits);
        assertTrue(byId.get(1L).getOwnAreaM2() < 1);
        assertTrue(byId.get(10L).getOwnAreaM2() < 1);
        assertEquals(byId.get(30L).getAreaM2(), byId.get(30L).getOwnAreaM2(), 1);
        assertTrue(byId.get(20L).getOwnAreaM2() < byId.get(20L).getAreaM2());
        assertTrue(adminUnits.getDirectory().regionCount() > 0);
        assertEquals(12, adminUnits.getDirectory().unitsWithGeometry());
    }

    @Test
    void cache_reusedWhileFingerprintIsTheSame() throws Exception {
        Path cache = tempDir.resolve("cache");
        AdminUnits built = standard().build(OPTIONS, cache);
        Path file = cache.resolve(AdminCache.FILE_NAME);
        assertTrue(Files.isRegularFile(file));
        long modified = Files.getLastModifiedTime(file).toMillis();

        AdminUnits loaded = standard().build(OPTIONS, cache);
        assertEquals(modified, Files.getLastModifiedTime(file).toMillis());
        assertEquals(built.getDirectory().regionCount(), loaded.getDirectory().regionCount());
        assertEquals(parentId(built, 50), parentId(loaded, 50));
        assertArrayEquals(node(built, 0.55, 1.4), node(loaded, 0.55, 1.4));
        assertArrayEquals(area(built, box(0.65, 0.9, 0.75, 1.35)).getShares(),
                area(loaded, box(0.65, 0.9, 0.75, 1.35)).getShares());
        assertEquals(unitsById(built).get(20L).getOwnAreaM2(), unitsById(loaded).get(20L).getOwnAreaM2());

        AdminOptions otherNames = new AdminOptions(List.of("en"), 0.9, 0.5, 0.01, 10000, 100,
                AdminColumns.UNITS);
        AdminUnits rebuilt = standard().build(otherNames, cache);
        assertNotEquals(built.getDirectory().getFingerprint(), rebuilt.getDirectory().getFingerprint());
        assertEquals("Unit 30", rebuilt.getDirectory().name(unitsById(rebuilt).get(30L)));
    }

    @Test
    void metadata_listsVersionsColumnsAndParameters() {
        String json = adminUnits.metadataJson();
        assertTrue(json.startsWith("{\"version\":1,\"rules_version\":1,\"fingerprint\":\"sha256:"));
        assertTrue(json.contains("\"columns\":{\"units\":\"admin_units\",\"shares\":\"admin_unit_shares\"}"));
        assertTrue(json.endsWith("\"directory\":{\"name_lang\":[\"name\",\"en\"],\"parent_share\":0.9,"
                + "\"subarea_share\":0.5},\"binding\":{\"min_share\":0.01,\"min_area_m2\":10000,\"min_length_m\":100}}"));
        assertTrue(adminUnits.reportMetadataJson().contains("\"report\":{\"units\":13,\"units_with_geometry\":12,"));
    }
}
