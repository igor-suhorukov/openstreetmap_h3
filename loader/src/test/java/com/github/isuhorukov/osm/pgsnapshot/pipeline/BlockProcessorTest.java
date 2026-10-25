package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.util.MinimumEnclosingCircle;
import com.uber.h3core.H3Core;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BlockProcessorTest {

    @Mock
    private CliParameters parameters;

    private H3Core h3Core;
    private BlockProcessor blockProcessor;

    @BeforeEach
    void setUp() throws Exception {
        h3Core = H3Core.newInstance();
        blockProcessor = new BlockProcessor(parameters, h3Core);
    }

    private Node createNode(long id, double lat, double lon) {
        CommonEntityData data = new CommonEntityData(id, 1, new Date(), OsmUser.NONE, 1L);
        return new Node(data, lat, lon);
    }

    private Way createWay(long id, List<Tag> tags, List<WayNode> wayNodes) {
        CommonEntityData data = new CommonEntityData(id, 1, new Date(), OsmUser.NONE, 1L);
        data.getTags().addAll(tags);
        Way way = new Way(data);
        way.getWayNodes().addAll(wayNodes);
        return way;
    }

    private Relation createRelation(long id, List<Tag> tags, List<RelationMember> members) {
        CommonEntityData data = new CommonEntityData(id, 1, new Date(), OsmUser.NONE, 1L);
        data.getTags().addAll(tags);
        Relation relation = new Relation(data);
        relation.getMembers().addAll(members);
        return relation;
    }

    @Test
    void process_collectOnlyStat_nodeCountedWithoutSerializing() {
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        Node node = createNode(1L, 4.1755, 73.5093);
        BlockResult result = blockProcessor.process(List.of(new NodeContainer(node)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getNodeCount());
        assertTrue(result.getArrowNodeOrWays().isEmpty());
        assertTrue(result.getCsvResultPerH33().isEmpty());
    }

    @Test
    void process_saveArrowOnly_nodeAddedToArrowList() {
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(false);

        Node node = createNode(1L, 4.1755, 73.5093);
        BlockResult result = blockProcessor.process(List.of(new NodeContainer(node)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getNodeCount());
        assertEquals(1, result.getArrowNodeOrWays().size());
        assertTrue(result.getCsvResultPerH33().isEmpty());
    }

    @Test
    void process_savePostgresqlTsvOnly_nodeSerializedToCsv() {
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(false);
        when(parameters.isSavePostgresqlTsv()).thenReturn(true);

        Node node = createNode(1L, 4.1755, 73.5093);
        BlockResult result = blockProcessor.process(List.of(new NodeContainer(node)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getNodeCount());
        assertTrue(result.getArrowNodeOrWays().isEmpty());
        assertFalse(result.getCsvResultPerH33().isEmpty());
    }

    @Test
    void process_saveArrowAndTsv_nodeSavedToBoth() {
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(true);

        Node node = createNode(1L, 4.1755, 73.5093);
        BlockResult result = blockProcessor.process(List.of(new NodeContainer(node)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getNodeCount());
        assertEquals(1, result.getArrowNodeOrWays().size());
        assertFalse(result.getCsvResultPerH33().isEmpty());
    }

    @Test
    void process_skipBuildings_wayWithBuildingTagNotSerialized() {
        when(parameters.isSkipBuildings()).thenReturn(true);

        Way way = createWay(1L, List.of(new Tag("building", "yes")),
                List.of(new WayNode(1L, 4.0, 73.0), new WayNode(2L, 4.01, 73.01)));
        BlockResult result = blockProcessor.process(List.of(new WayContainer(way)), 1L, System.currentTimeMillis());

        // wayCount counts containers, not processed ways; stat maps confirm skipping
        assertTrue(result.getBlockStat().getWayStat() == null || result.getBlockStat().getWayStat().isEmpty());
        assertTrue(result.getArrowNodeOrWays().isEmpty());
        assertTrue(result.getCsvResultPerH33().isEmpty());
    }

    @Test
    void process_skipBuildings_wayWithoutBuildingTagIncluded() {
        when(parameters.isSkipBuildings()).thenReturn(true);
        when(parameters.isSkipHighway()).thenReturn(false);
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        Way way = createWay(1L, List.of(new Tag("waterway", "river")),
                List.of(new WayNode(1L, 4.0, 73.0), new WayNode(2L, 4.01, 73.01)));
        BlockResult result = blockProcessor.process(List.of(new WayContainer(way)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getWayCount());
    }

    @Test
    void process_skipHighway_wayWithHighwayTagNotSerialized() {
        when(parameters.isSkipBuildings()).thenReturn(false);
        when(parameters.isSkipHighway()).thenReturn(true);

        Way way = createWay(1L, List.of(new Tag("highway", "primary")),
                List.of(new WayNode(1L, 4.0, 73.0), new WayNode(2L, 4.01, 73.01)));
        BlockResult result = blockProcessor.process(List.of(new WayContainer(way)), 1L, System.currentTimeMillis());

        // stat maps are empty confirming the way was skipped
        assertTrue(result.getBlockStat().getWayStat() == null || result.getBlockStat().getWayStat().isEmpty());
        assertTrue(result.getArrowNodeOrWays().isEmpty());
        assertTrue(result.getCsvResultPerH33().isEmpty());
    }

    @Test
    void process_skipHighway_wayWithoutHighwayTagIncluded() {
        when(parameters.isSkipBuildings()).thenReturn(false);
        when(parameters.isSkipHighway()).thenReturn(true);
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        Way way = createWay(1L, List.of(new Tag("name", "Reef")),
                List.of(new WayNode(1L, 4.0, 73.0), new WayNode(2L, 4.01, 73.01)));
        BlockResult result = blockProcessor.process(List.of(new WayContainer(way)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getWayCount());
    }

    @Test
    void process_way_collectOnlyStat_counted() {
        when(parameters.isSkipBuildings()).thenReturn(false);
        when(parameters.isSkipHighway()).thenReturn(false);
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        Way way = createWay(1L, Collections.emptyList(),
                List.of(new WayNode(1L, 4.0, 73.0), new WayNode(2L, 4.01, 73.01)));
        BlockResult result = blockProcessor.process(List.of(new WayContainer(way)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getWayCount());
        assertTrue(result.getArrowNodeOrWays().isEmpty());
        assertTrue(result.getCsvResultPerH33().isEmpty());
    }

    @Test
    void process_way_savePostgresqlTsv_serializedToCsv() {
        when(parameters.isSkipBuildings()).thenReturn(false);
        when(parameters.isSkipHighway()).thenReturn(false);
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(false);
        when(parameters.isSavePostgresqlTsv()).thenReturn(true);
        when(parameters.isScaleApproximation()).thenReturn(false);

        Way way = createWay(1L, Collections.emptyList(),
                List.of(new WayNode(1L, 4.0, 73.0), new WayNode(2L, 4.01, 73.01)));
        BlockResult result = blockProcessor.process(List.of(new WayContainer(way)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getWayCount());
        assertFalse(result.getCsvResultPerH33().isEmpty());
    }

    @Test
    void process_way_saveArrow_addedToArrowList() {
        when(parameters.isSkipBuildings()).thenReturn(false);
        when(parameters.isSkipHighway()).thenReturn(false);
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(false);
        when(parameters.isScaleApproximation()).thenReturn(false);

        Way way = createWay(1L, Collections.emptyList(),
                List.of(new WayNode(1L, 4.0, 73.0), new WayNode(2L, 4.01, 73.01)));
        BlockResult result = blockProcessor.process(List.of(new WayContainer(way)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getWayCount());
        assertEquals(1, result.getArrowNodeOrWays().size());
    }

    @Test
    void process_wayScale_equalsGeoToolsOrthodromicDistance() throws Exception {
        when(parameters.isSkipBuildings()).thenReturn(false);
        when(parameters.isSkipHighway()).thenReturn(false);
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(false);
        when(parameters.isScaleApproximation()).thenReturn(false);
        List<Way> ways = List.of(
                createWay(1L, Collections.emptyList(), List.of(new WayNode(1L, 13.7563, 100.5018),
                        new WayNode(2L, 13.7570, 100.5030), new WayNode(3L, 13.7575, 100.5021),
                        new WayNode(1L, 13.7563, 100.5018))),
                createWay(2L, Collections.emptyList(), List.of(new WayNode(4L, 4.0, 73.0), new WayNode(5L, 4.01, 73.01))),
                createWay(3L, Collections.emptyList(), List.of(new WayNode(6L, 69.65, 18.95),
                        new WayNode(7L, 69.70, 19.10), new WayNode(8L, 69.68, 19.30))));

        BlockResult result = blockProcessor.process(ways.stream().<EntityContainer>map(WayContainer::new)
                .collect(Collectors.toList()), 1L, System.currentTimeMillis());

        CoordinateReferenceSystem crs = CRS.decode("EPSG:4326");
        for (int i = 0; i < ways.size(); i++) {
            Coordinate[] coordinates = ways.get(i).getWayNodes().stream()
                    .map(wayNode -> new CoordinateXY(wayNode.getLongitude(), wayNode.getLatitude()))
                    .toArray(Coordinate[]::new);
            MinimumEnclosingCircle circle = MinimumEnclosingCircle.of(coordinates);
            float radius = (float) circle.getRadius();
            float expected = (float) JTS.orthodromicDistance(new CoordinateXY(circle.getCentreX(), circle.getCentreY()),
                    new CoordinateXY(circle.getCentreX() + radius, circle.getCentreY()), crs);
            ArrowNodeOrWay way = result.getArrowNodeOrWays().get(i);
            assertEquals(expected, way.getScaleDim(), "way " + way.getId());
        }
    }

    @Test
    void process_processorReusedForNextBlocks_sameResultAsFreshProcessor() {
        when(parameters.isSkipBuildings()).thenReturn(false);
        when(parameters.isSkipHighway()).thenReturn(false);
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(true);
        when(parameters.isScaleApproximation()).thenReturn(false);
        Way road = createWay(1L, List.of(new Tag("highway", "primary")),
                List.of(new WayNode(1L, 13.75, 100.50), new WayNode(2L, 13.80, 100.62)));
        Way building = createWay(2L, List.of(new Tag("building", "yes")), List.of(new WayNode(3L, 18.79, 98.98),
                new WayNode(4L, 18.791, 98.98), new WayNode(5L, 18.791, 98.981), new WayNode(3L, 18.79, 98.98)));

        BlockResult first = blockProcessor.process(List.of(new WayContainer(road)), 1L, System.currentTimeMillis());
        blockProcessor.process(List.of(new WayContainer(building)), 2L, System.currentTimeMillis());
        BlockResult repeated = blockProcessor.process(List.of(new WayContainer(road)), 3L, System.currentTimeMillis());
        BlockResult fresh = new BlockProcessor(parameters, h3Core)
                .process(List.of(new WayContainer(road)), 4L, System.currentTimeMillis());

        assertEquals(csv(first), csv(repeated));
        assertEquals(csv(first), csv(fresh));
        assertEquals(first.getArrowNodeOrWays().get(0).getScaleDim(), repeated.getArrowNodeOrWays().get(0).getScaleDim());
    }

    @Test
    void process_waySpanningMultipleH3Cells_h33IsMaxValue() {
        when(parameters.isSkipBuildings()).thenReturn(false);
        when(parameters.isSkipHighway()).thenReturn(false);
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        // Coordinates far apart — guaranteed different H3 resolution-3 cells
        Way way = createWay(1L, Collections.emptyList(),
                List.of(new WayNode(1L, 0.0, 0.0), new WayNode(2L, 60.0, 60.0)));
        BlockResult result = blockProcessor.process(List.of(new WayContainer(way)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getWayCount());
        assertTrue(result.getBlockStat().getWayStat().containsKey(Short.MAX_VALUE));
    }

    @Test
    void process_waySingleH3Cell_h33NotMaxValue() {
        when(parameters.isSkipBuildings()).thenReturn(false);
        when(parameters.isSkipHighway()).thenReturn(false);
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        // Very close nodes — same H3 resolution-3 cell
        Way way = createWay(1L, Collections.emptyList(),
                List.of(new WayNode(1L, 4.0, 73.0), new WayNode(2L, 4.001, 73.001)));
        BlockResult result = blockProcessor.process(List.of(new WayContainer(way)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getWayCount());
        assertFalse(result.getBlockStat().getWayStat().containsKey(Short.MAX_VALUE));
    }

    @Test
    void process_relation_collectOnlyStat_countedNotSerialized() {
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        Relation relation = createRelation(1L,
                List.of(new Tag("type", "multipolygon")),
                List.of(new RelationMember(1L, EntityType.Way, "outer")));
        BlockResult result = blockProcessor.process(List.of(new RelationContainer(relation)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getRelationCount());
        assertEquals(1, result.getBlockStat().getMultipolygonCount());
        assertEquals(1, result.getBlockStat().getRelationMembersCount());
        assertTrue(result.getCsvResultPerH33().isEmpty());
        assertTrue(result.getArrowRelations().isEmpty());
    }

    @Test
    void process_relation_savePostgresqlTsv_serializedToCsv() {
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(false);
        when(parameters.isSavePostgresqlTsv()).thenReturn(true);

        Relation relation = createRelation(42L,
                List.of(new Tag("type", "boundary")),
                List.of(new RelationMember(10L, EntityType.Way, "outer"),
                        new RelationMember(20L, EntityType.Node, "label")));
        BlockResult result = blockProcessor.process(List.of(new RelationContainer(relation)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getRelationCount());
        assertFalse(result.getCsvResultPerH33().isEmpty());
        assertTrue(result.getArrowRelations().isEmpty());
    }

    @Test
    void process_relation_saveArrow_addedToArrowRelations() {
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(false);

        Relation relation = createRelation(42L,
                List.of(new Tag("type", "route")),
                List.of(new RelationMember(10L, EntityType.Way, "member")));
        BlockResult result = blockProcessor.process(List.of(new RelationContainer(relation)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getRelationCount());
        assertEquals(1, result.getArrowRelations().size());
        assertEquals(1, result.getArrowRelations().get(0).getRelationMembers().size());
    }

    @Test
    void process_relation_saveArrowAndTsv_savedToBoth() {
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(true);

        Relation relation = createRelation(42L,
                List.of(new Tag("natural", "water")),
                List.of(new RelationMember(5L, EntityType.Way, "outer")));
        BlockResult result = blockProcessor.process(List.of(new RelationContainer(relation)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getRelationCount());
        assertEquals(1, result.getArrowRelations().size());
        assertFalse(result.getCsvResultPerH33().isEmpty());
    }

    @Test
    void process_multipolygonCount_noRelations_returnsZero() {
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        Node node = createNode(1L, 4.1755, 73.5093);
        BlockResult result = blockProcessor.process(List.of(new NodeContainer(node)), 1L, System.currentTimeMillis());

        assertEquals(0, result.getBlockStat().getMultipolygonCount());
    }

    @Test
    void process_multipolygonCount_nonMultipolygonRelation_countsZero() {
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        Relation relation = createRelation(1L,
                List.of(new Tag("type", "route")),
                List.of(new RelationMember(1L, EntityType.Way, "member")));
        BlockResult result = blockProcessor.process(List.of(new RelationContainer(relation)), 1L, System.currentTimeMillis());

        assertEquals(1, result.getRelationCount());
        assertEquals(0, result.getBlockStat().getMultipolygonCount());
    }

    @Test
    void process_mixedNodesAndRelations_allCountsCorrect() {
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        Node node1 = createNode(1L, 4.1755, 73.5093);
        Node node2 = createNode(2L, 4.2, 73.6);
        Relation relation = createRelation(1L,
                List.of(new Tag("type", "multipolygon")),
                List.of(new RelationMember(1L, EntityType.Way, "outer"),
                        new RelationMember(2L, EntityType.Way, "inner")));
        BlockResult result = blockProcessor.process(
                List.of(new NodeContainer(node1), new NodeContainer(node2), new RelationContainer(relation)),
                1L, System.currentTimeMillis());

        assertEquals(2, result.getNodeCount());
        assertEquals(0, result.getWayCount());
        assertEquals(1, result.getRelationCount());
        assertEquals(1, result.getBlockStat().getMultipolygonCount());
        assertEquals(2, result.getBlockStat().getRelationMembersCount());
    }

    @Test
    void process_emptyEntityList_allCountsZero() {
        BlockResult result = blockProcessor.process(Collections.emptyList(), 0L, System.currentTimeMillis());

        assertEquals(0, result.getNodeCount());
        assertEquals(0, result.getWayCount());
        assertEquals(0, result.getRelationCount());
    }

    @Test
    void process_updatable_tsvRowsEndWithVersionAndGeomHash() {
        when(parameters.isUpdatable()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(true);
        BlockProcessor processor = new BlockProcessor(parameters, h3Core);
        Node node = new Node(new CommonEntityData(1L, 3, new Date(), OsmUser.NONE, 1L), 4.1755, 73.5093);
        List<WayNode> wayNodes = List.of(new WayNode(1L, 4.0, 73.0), new WayNode(2L, 4.01, 73.01));
        Way way = createWay(2L, Collections.emptyList(), wayNodes);
        Relation relation = createRelation(3L, List.of(new Tag("type", "multipolygon")),
                List.of(new RelationMember(2L, EntityType.Way, "outer")));

        String nodeCsv = csv(processor.process(List.of(new NodeContainer(node)), 1L, System.currentTimeMillis()));
        String wayCsv = csv(processor.process(List.of(new WayContainer(way)), 2L, System.currentTimeMillis()));
        BlockResult relationResult = processor.process(List.of(new RelationContainer(relation)), 3L, System.currentTimeMillis());

        assertTrue(nodeCsv.endsWith("\\N\t3\n"), nodeCsv);
        assertEquals(6, nodeCsv.split("\t", -1).length, nodeCsv);
        assertTrue(wayCsv.endsWith("\\N\t1\t" + WayGeometry.geomHash(wayNodes) + "\n"), wayCsv);
        assertEquals(15, wayCsv.split("\t", -1).length, wayCsv);
        assertTrue(relationResult.getCsvResultPerH33().get((short) 0).toString()
                .endsWith("\t1\n"), relationResult.getCsvResultPerH33().toString());
        assertEquals("3\t2\tW\t\"outer\"\t0\n", relationResult.getCsvResultPerH33().get((short) 1).toString());
    }

    @Test
    void process_storedVersions_onlyChangedAndCreatedObjectsSerialized() {
        when(parameters.isUpdatable()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(true);
        List<WayNode> wayNodes = List.of(new WayNode(1L, 4.0, 73.0), new WayNode(2L, 4.01, 73.01));
        long storedHash = WayGeometry.geomHash(wayNodes);
        List<String> checked = new java.util.ArrayList<>();
        StoredVersions storedVersions = new StoredVersions() {
            @Override
            public boolean unchangedNode(long id, int version) {
                checked.add("n" + id + "v" + version);
                return id == 1;
            }

            @Override
            public boolean unchangedWay(long id, int version, long geomHash) {
                checked.add("w" + id + "v" + version);
                return id == 10 && geomHash == storedHash;
            }

            @Override
            public boolean unchangedRelation(long id, int version) {
                checked.add("r" + id + "v" + version);
                return id == 100;
            }
        };
        BlockProcessor processor = new BlockProcessor(parameters, h3Core, Collections.emptyList(), storedVersions);

        BlockResult nodes = processor.process(List.of(new NodeContainer(createNode(1L, 4.1, 73.1)),
                new NodeContainer(createNode(2L, 4.2, 73.2))), 1L, System.currentTimeMillis());
        BlockResult ways = processor.process(List.of(new WayContainer(createWay(10L, Collections.emptyList(), wayNodes)),
                new WayContainer(createWay(11L, Collections.emptyList(), wayNodes))), 2L, System.currentTimeMillis());
        BlockResult relations = processor.process(List.of(
                new RelationContainer(createRelation(100L, Collections.emptyList(), Collections.emptyList())),
                new RelationContainer(createRelation(101L, Collections.emptyList(),
                        List.of(new RelationMember(10L, EntityType.Way, ""))))), 3L, System.currentTimeMillis());

        assertEquals(List.of("n1v1", "n2v1", "w10v1", "w11v1", "r100v1", "r101v1"), checked);
        assertEquals(2, nodes.getNodeCount());
        assertEquals(1, csv(nodes).split("\n").length);
        assertTrue(csv(nodes).contains("\t2\t"), csv(nodes));
        assertEquals(1, csv(ways).split("\n").length);
        assertTrue(csv(ways).contains("\t11\t"), csv(ways));
        assertEquals(2, ways.getWayCount());
        assertTrue(relations.getCsvResultPerH33().get((short) 0).toString().startsWith("101\t"));
        assertEquals("101\t10\tW\t\\N\t0\n", relations.getCsvResultPerH33().get((short) 1).toString());
        assertEquals(2, relations.getRelationCount());
    }

    @Test
    void process_updatableEntityWithoutVersion_throws() {
        when(parameters.isUpdatable()).thenReturn(true);
        Node node = new Node(new CommonEntityData(7L, -1, new Date(), OsmUser.NONE, 1L), 4.1755, 73.5093);
        List<EntityContainer> entities = List.of(new NodeContainer(node));
        long threadStart = System.currentTimeMillis();

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> blockProcessor.process(entities, 1L, threadStart));

        assertTrue(exception.getMessage().contains("Node 7 has no version"), exception.getMessage());
    }

    private BlockProcessor extensionBlockProcessor(TagExtension... tagExtensions) {
        return new BlockProcessor(parameters, h3Core, List.of(tagExtensions));
    }

    private static String csv(BlockResult result) {
        StringBuilder csv = new StringBuilder();
        result.getCsvResultPerH33().values().forEach(csv::append);
        return csv.toString();
    }

    @Test
    void process_tagExtension_nodeTsvAndArrowContainAdditionalTags() {
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(true);

        Node node = createNode(1L, 13.7563, 100.5018);
        node.getTags().add(new Tag("amenity", "cafe"));
        BlockResult result = extensionBlockProcessor(new TestTagExtension("test", "is.test", "x"))
                .process(List.of(new NodeContainer(node)), 1L, System.currentTimeMillis());

        String csv = csv(result);
        assertTrue(csv.contains("is.test\\\"=>\\\"x-Node"), csv);
        assertEquals(Map.of("amenity", "cafe", "is.test", "x-Node"), result.getArrowNodeOrWays().get(0).getTags());
    }

    @Test
    void process_tagExtensionWithoutTagsForEntity_sourceTagsKept() {
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(false);

        Node node = createNode(1L, 13.7563, 100.5018);
        node.getTags().add(new Tag("name", "Victory Monument"));
        BlockResult result = extensionBlockProcessor(new TestTagExtension("test", "is.test", "x"))
                .process(List.of(new NodeContainer(node)), 1L, System.currentTimeMillis());

        assertEquals(Map.of("name", "Victory Monument"), result.getArrowNodeOrWays().get(0).getTags());
    }

    @Test
    void process_tagExtension_wayTsvContainsAdditionalTags() {
        when(parameters.isSkipBuildings()).thenReturn(false);
        when(parameters.isSkipHighway()).thenReturn(false);
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(false);
        when(parameters.isSavePostgresqlTsv()).thenReturn(true);
        when(parameters.isScaleApproximation()).thenReturn(false);

        Way way = createWay(1L, List.of(new Tag("amenity", "school"), new Tag("building", "yes")),
                List.of(new WayNode(1L, 13.75, 100.50), new WayNode(2L, 13.751, 100.50),
                        new WayNode(3L, 13.751, 100.501), new WayNode(1L, 13.75, 100.50)));
        BlockResult result = extensionBlockProcessor(new TestTagExtension("test", "is.test", "x"))
                .process(List.of(new WayContainer(way)), 1L, System.currentTimeMillis());

        String csv = csv(result);
        assertTrue(csv.contains("is.test\\\"=>\\\"x-Way"), csv);
    }

    @Test
    void process_tagExtension_relationTsvContainsAdditionalTags() {
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(false);
        when(parameters.isSavePostgresqlTsv()).thenReturn(true);

        Relation relation = createRelation(42L,
                List.of(new Tag("type", "multipolygon"), new Tag("amenity", "school")),
                List.of(new RelationMember(10L, EntityType.Way, "outer")));
        BlockResult result = extensionBlockProcessor(new TestTagExtension("test", "is.test", "x"))
                .process(List.of(new RelationContainer(relation)), 1L, System.currentTimeMillis());

        String csv = csv(result);
        assertTrue(csv.contains("is.test\\\"=>\\\"x-Relation"), csv);
    }

    @Test
    void process_twoTagExtensions_tagsOfBothAppended() {
        when(parameters.isCollectOnlyStat()).thenReturn(false);
        when(parameters.isSaveArrow()).thenReturn(true);
        when(parameters.isSavePostgresqlTsv()).thenReturn(false);

        Node node = createNode(1L, 13.7563, 100.5018);
        node.getTags().add(new Tag("amenity", "cafe"));
        BlockResult result = extensionBlockProcessor(new TestTagExtension("a", "is.a", "1"), new TestTagExtension("b", "is.b", "2"))
                .process(List.of(new NodeContainer(node)), 1L, System.currentTimeMillis());

        assertEquals(Map.of("amenity", "cafe", "is.a", "1-Node", "is.b", "2-Node"),
                result.getArrowNodeOrWays().get(0).getTags());
    }

    @Test
    void process_tagExtension_statisticsIncludeAdditionalTags() {
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        Node node = createNode(1L, 13.7563, 100.5018);
        node.getTags().add(new Tag("amenity", "cafe"));
        long sizeWithoutExtension = blockProcessor.process(List.of(new NodeContainer(node)), 1L, System.currentTimeMillis())
                .getBlockStat().getNodeStat().values().iterator().next().getSize();
        long sizeWithExtension = extensionBlockProcessor(new TestTagExtension("test", "is.test", "x"))
                .process(List.of(new NodeContainer(node)), 1L, System.currentTimeMillis())
                .getBlockStat().getNodeStat().values().iterator().next().getSize();

        assertEquals(sizeWithoutExtension + "is.test=x-Node".length(), sizeWithExtension);
    }

    @Test
    void process_blockStatTimesAreSet() {
        when(parameters.isCollectOnlyStat()).thenReturn(true);

        long threadStart = System.currentTimeMillis() - 100;
        Node node = createNode(1L, 4.1755, 73.5093);
        BlockResult result = blockProcessor.process(List.of(new NodeContainer(node)), 5L, threadStart);

        assertEquals(5L, result.getBlockStat().getId());
        assertEquals(threadStart, result.getBlockStat().getThreadStart());
        assertEquals(1, result.getBlockStat().getNodeCount());
    }
}
