package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.OsmPbfTransformation;
import com.github.isuhorukov.osm.pgsnapshot.Serializer;
import com.github.isuhorukov.osm.pgsnapshot.WaySerializeData;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelationMember;
import com.github.isuhorukov.osm.pgsnapshot.model.TagsUtil;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.Stat;
import com.github.isuhorukov.osm.pgsnapshot.util.CompactH3;
import com.uber.h3core.H3Core;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.MemberTypeValueMapper;

import java.util.*;
import java.util.stream.Collectors;

public class BlockProcessor {

    private final CliParameters parameters;
    private final H3Core h3Core;
    private final GeometryFactory geometryFactory;
    private final MemberTypeValueMapper memberTypeValueMapper;
    private final WKBWriter wkbWriter;
    private final CoordinateReferenceSystem coordinateReferenceSystem;
    private final Serializer serializer;

    public BlockProcessor(CliParameters parameters, H3Core h3Core) {
        this.parameters = parameters;
        this.h3Core = h3Core;
        this.geometryFactory = new GeometryFactory();
        this.memberTypeValueMapper = new MemberTypeValueMapper();
        this.wkbWriter = new WKBWriter();
        this.serializer = new Serializer();
        try {
            this.coordinateReferenceSystem = CRS.decode("EPSG:" + Serializer.SRID);
        } catch (FactoryException e) {
            throw new IllegalStateException(e);
        }
    }

    public BlockResult process(List<EntityContainer> decodedEntities, Long blockNumber, long threadStart) {
        BlockResult result = new BlockResult(blockNumber);
        result.getBlockStat().setThreadStart(threadStart);

        Map<Short, Stat> nodeStat = new HashMap<>();
        long nodeRecords = processNodes(decodedEntities, result, nodeStat);

        Map<Short, Stat> wayStat = new HashMap<>();
        long wayRecords = processWays(decodedEntities, result, wayStat);

        if (!nodeStat.isEmpty()) {
            result.getBlockStat().setNodeStat(nodeStat);
        }
        if (!wayStat.isEmpty()) {
            result.getBlockStat().setWayStat(wayStat);
        }

        long relationCount = processRelations(decodedEntities, result);
        long multipolygonCount = countMultipolygons(decodedEntities, relationCount);
        long relationMemberCount = countRelationMembers(decodedEntities);

        result.setNodeCount(nodeRecords);
        result.setWayCount(wayRecords);
        result.setRelationCount(relationCount);
        result.getBlockStat().setNodeCount(nodeRecords);
        result.getBlockStat().setWayCount(wayRecords);
        result.getBlockStat().setRelationCount(relationCount);
        result.getBlockStat().setRelationMembersCount(relationMemberCount);
        result.getBlockStat().setMultipolygonCount(multipolygonCount);

        return result;
    }

    private long processNodes(List<EntityContainer> decodedEntities, BlockResult result, Map<Short, Stat> nodeStat) {
        return decodedEntities.stream()
                .filter(NodeContainer.class::isInstance)
                .map(ec -> ((NodeContainer) ec).getEntity())
                .map(entity -> {
                    processNode(entity, result, nodeStat);
                    return null;
                }).filter(Objects::isNull).count();
    }

    private void processNode(Node entity, BlockResult result, Map<Short, Stat> nodeStat) {
        long id = entity.getId();
        final double latitude = entity.getLatitude();
        final double longitude = entity.getLongitude();
        short h33 = CompactH3.serialize3(h3Core.latLngToCell(latitude, longitude, 3));
        Stat stat = nodeStat.computeIfAbsent(h33, Stat::new);
        stat.incrementCount();
        stat.updateIdStat(id);
        stat.updateLastModified(entity.getTimestamp().getTime());
        stat.incrementSize(2 + 3 * 8);
        updateSizeStatsByTags(entity, stat);

        if (!parameters.isCollectOnlyStat()) {
            int h38 = CompactH3.serialize8(h3Core.latLngToCell(latitude, longitude, 8));
            if (parameters.isSaveArrow()) {
                result.getArrowNodeOrWays().add(new ArrowNodeOrWay(id, h33, h38, latitude, longitude, getTags(entity)));
            }
            if (parameters.isSavePostgresqlTsv()) {
                StringBuilder resultBuilder = result.getCsvResultPerH33().computeIfAbsent(h33, key -> new StringBuilder());
                serializer.serializeNode(resultBuilder, h33, h38, id, latitude, longitude, getTags(entity));
            }
        }
    }

    private long processWays(List<EntityContainer> decodedEntities, BlockResult result, Map<Short, Stat> wayStat) {
        return decodedEntities.stream()
                .filter(WayContainer.class::isInstance)
                .map(ec -> ((WayContainer) ec).getEntity())
                .map(entity -> {
                    processWay(entity, result, wayStat);
                    return null;
                }).filter(Objects::isNull).count();
    }

    private void processWay(Way entity, BlockResult result, Map<Short, Stat> wayStat) {
        if (parameters.isSkipBuildings() && entity.getTags().stream().anyMatch(tag -> "building".equals(tag.getKey()))) {
            return;
        }
        if (parameters.isSkipHighway() && entity.getTags().stream().anyMatch(tag -> "highway".equals(tag.getKey()))) {
            return;
        }
        long id = entity.getId();
        List<WayNode> wayNodes = entity.getWayNodes();

        short[] h3Idxs = new short[wayNodes.size()];
        for (int i = 0; i < wayNodes.size(); i++) {
            WayNode wn = wayNodes.get(i);
            h3Idxs[i] = CompactH3.serialize3(h3Core.latLngToCell(wn.getLatitude(), wn.getLongitude(), 3));
        }
        boolean isInOneH33 = isInSingleH3Segment(h3Idxs);
        short h33 = isInOneH33 ? h3Idxs[0] : Short.MAX_VALUE;

        Stat stat = wayStat.computeIfAbsent(h33, Stat::new);
        stat.incrementCount();
        stat.updateIdStat(id);
        stat.updateLastModified(entity.getTimestamp().getTime());
        stat.incrementSize(2 + 8);
        stat.incrementSize(wayNodes.size() * 16);
        updateSizeStatsByTags(entity, stat);

        if (!parameters.isCollectOnlyStat()) {
            WayGeometry geom = WayGeometry.from(entity, h3Core, geometryFactory,
                    coordinateReferenceSystem, parameters.isScaleApproximation(), wkbWriter);

            if (parameters.isSaveArrow()) {
                int[] h38Idxs = !geom.getWayIntersectionH38Indexes().isEmpty()
                        ? geom.getWayIntersectionH38Indexes().stream().mapToInt(Integer::intValue).toArray()
                        : null;
                ArrowNodeOrWay arrowNodeOrWay = new ArrowNodeOrWay.Builder(
                        id, h33, geom.getH38(), geom.getLatitude(), geom.getLongitude(), getTags(entity))
                        .bboxMinX(geom.getMinX()).bboxMinY(geom.getMinY())
                        .bboxMaxX(geom.getMaxX()).bboxMaxY(geom.getMaxY())
                        .closed(geom.isClosed())
                        .pointIdxs(geom.getPointIdxs())
                        .scaleDim(geom.getScaleDim())
                        .h38Indexes(h38Idxs)
                        .h33Center(geom.getH33Center())
                        .lineStringWkb(geom.getLineStringWkb())
                        .bboxWkb(geom.getBboxWkb())
                        .build();
                result.getArrowNodeOrWays().add(arrowNodeOrWay);
            }
            if (parameters.isSavePostgresqlTsv()) {
                StringBuilder resultBuilder = result.getCsvResultPerH33().computeIfAbsent(h33, k -> new StringBuilder());
                WaySerializeData wayData = new WaySerializeData.Builder()
                        .h33(h33).id(id)
                        .closed(geom.isClosed()).nonValid(geom.isNonValid()).h38(geom.getH38())
                        .pointsIdx(geom.getPointIdxs())
                        .wayIntersectionH38Indexes(geom.getWayIntersectionH38Indexes())
                        .centre(geom.getCentre()).scaleDim(geom.getScaleDim())
                        .bboxGeometry(geom.getBboxGeometry()).lineString(geom.getLineString())
                        .tags(getTags(entity))
                        .build();
                serializer.serializeWay(resultBuilder, wayData);
            }
        }
    }

    private long processRelations(List<EntityContainer> decodedEntities, BlockResult result) {
        return decodedEntities.stream()
                .filter(RelationContainer.class::isInstance)
                .map(ec -> ((RelationContainer) ec).getEntity())
                .map(entity -> {
                    if (!parameters.isCollectOnlyStat()) {
                        serializeRelation(entity, result);
                    }
                    return null;
                }).count();
    }

    private void serializeRelation(Relation entity, BlockResult result) {
        long relationId = entity.getId();
        if (parameters.isSavePostgresqlTsv()) {
            StringBuilder relationCsv = result.getCsvResultPerH33().computeIfAbsent((short) 0, k -> new StringBuilder());
            serializer.serializeRelation(relationCsv, relationId, getTags(entity));
        }

        ArrowRelation arrowRelation = null;
        if (parameters.isSaveArrow()) {
            arrowRelation = new ArrowRelation(relationId, TagsUtil.tagsToMap(getTags(entity)));
            result.getArrowRelations().add(arrowRelation);
        }

        List<RelationMember> relationMembers = entity.getMembers();
        for (int sequenceId = 0; sequenceId < relationMembers.size(); sequenceId++) {
            RelationMember relationMember = relationMembers.get(sequenceId);
            long memberId = relationMember.getMemberId();
            String memberType = memberTypeValueMapper.getMemberType(relationMember.getMemberType());
            String memberRole = relationMember.getMemberRole();
            if (parameters.isSaveArrow() && arrowRelation != null) {
                arrowRelation.getRelationMembers().add(
                        new ArrowRelationMember(memberId, memberType.charAt(0), memberRole));
            }
            if (parameters.isSavePostgresqlTsv()) {
                StringBuilder relationMembersCsv = result.getCsvResultPerH33().computeIfAbsent((short) 1, k -> new StringBuilder());
                serializer.serializeRelationMembers(relationMembersCsv, relationId,
                        memberId, memberType, memberRole, sequenceId);
            }
        }
    }

    private static long countMultipolygons(List<EntityContainer> decodedEntities, long relationCount) {
        if (relationCount == 0) {
            return 0;
        }
        return decodedEntities.stream()
                .filter(RelationContainer.class::isInstance)
                .map(entity -> {
                    for (Tag tag : entity.getEntity().getTags()) {
                        if ("type".equals(tag.getKey()) && "multipolygon".equals(tag.getValue())) {
                            return 1;
                        }
                    }
                    return 0;
                }).mapToLong(Integer::longValue).sum();
    }

    private static long countRelationMembers(List<EntityContainer> decodedEntities) {
        return decodedEntities.stream()
                .filter(RelationContainer.class::isInstance)
                .map(ec -> ((RelationContainer) ec).getEntity())
                .mapToLong(v -> v.getMembers().size()).sum();
    }

    private static boolean isInSingleH3Segment(short[] h3Idxs) {
        for (int i = 1; i < h3Idxs.length; i++) {
            if (h3Idxs[i] != h3Idxs[0]) {
                return false;
            }
        }
        return true;
    }

    private static void updateSizeStatsByTags(Entity entity, Stat stat) {
        for (Tag tag : getTags(entity)) {
            stat.incrementSize(tag.getKey().length());
            stat.incrementSize(1);
            stat.incrementSize(tag.getValue().length());
        }
    }

    private static Collection<Tag> getTags(Entity entity) {
        Collection<Tag> sourceTags = entity.getTags();
        if (sourceTags != null && !sourceTags.isEmpty() && OsmPbfTransformation.IS_UDT_ENABLED) {
            Set<String> detected = new HashSet<>();
            if (!detected.isEmpty()) {
                ArrayList<Tag> tagList = new ArrayList<>(sourceTags);
                tagList.addAll(detected.stream().map(s -> new Tag(s, null)).collect(Collectors.toList()));
                return tagList;
            }
        }
        return sourceTags;
    }
}
