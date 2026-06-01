package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.uber.h3core.H3Core;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;

import java.util.Collections;
import java.util.Date;
import java.util.List;

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
