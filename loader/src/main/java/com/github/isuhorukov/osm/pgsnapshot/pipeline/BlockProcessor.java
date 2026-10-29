package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.Serializer;
import com.github.isuhorukov.osm.pgsnapshot.WaySerializeData;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelationMember;
import com.github.isuhorukov.osm.pgsnapshot.model.TagsUtil;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.Stat;
import com.github.isuhorukov.osm.pgsnapshot.util.CompactH3;
import com.uber.h3core.H3Core;
import org.geotools.referencing.CRS;
import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;
import org.opengis.referencing.FactoryException;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.*;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.MemberTypeValueMapper;

import java.util.*;

public class BlockProcessor {

    private final CliParameters parameters;
    private final H3Core h3Core;
    private final GeometryFactory geometryFactory;
    private final MemberTypeValueMapper memberTypeValueMapper;
    private final WKBWriter wkbWriter;
    private final GeodeticCalculator geodeticCalculator;
    private final Serializer serializer;
    private final List<TagExtension> tagExtensions;
    private final StoredVersions storedVersions;

    public BlockProcessor(CliParameters parameters, H3Core h3Core) {
        this(parameters, h3Core, Collections.emptyList());
    }

    public BlockProcessor(CliParameters parameters, H3Core h3Core, List<TagExtension> tagExtensions) {
        this(parameters, h3Core, tagExtensions, StoredVersions.NONE);
    }

    public BlockProcessor(CliParameters parameters, H3Core h3Core, List<TagExtension> tagExtensions,
                          StoredVersions storedVersions) {
        this.parameters = parameters;
        this.h3Core = h3Core;
        this.tagExtensions = tagExtensions;
        this.storedVersions = storedVersions;
        this.geometryFactory = new GeometryFactory();
        this.memberTypeValueMapper = new MemberTypeValueMapper();
        this.wkbWriter = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
        this.serializer = new Serializer(parameters.isUpdatable());
        try {
            this.geodeticCalculator = new GeodeticCalculator(CRS.decode("EPSG:" + Serializer.SRID));
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
        long nodeCount = 0;
        for (EntityContainer entityContainer : decodedEntities) {
            if (entityContainer instanceof NodeContainer) {
                processNode(((NodeContainer) entityContainer).getEntity(), result, nodeStat);
                nodeCount++;
            }
        }
        return nodeCount;
    }

    private void processNode(Node entity, BlockResult result, Map<Short, Stat> nodeStat) {
        long id = entity.getId();
        int version = version(entity);
        if (storedVersions.unchangedNode(id, version)) {
            return;
        }
        final double latitude = entity.getLatitude();
        final double longitude = entity.getLongitude();
        short h33 = CompactH3.serialize3(h3Core.latLngToCell(latitude, longitude, 3));
        Stat stat = nodeStat.computeIfAbsent(h33, Stat::new);
        stat.incrementCount();
        stat.updateIdStat(id);
        stat.updateLastModified(entity.getTimestamp().getTime());
        stat.incrementSize(2 + 3 * 8);
        Collection<Tag> tags = getTags(entity);
        updateSizeStatsByTags(tags, stat);

        if (!parameters.isCollectOnlyStat()) {
            int h38 = CompactH3.serialize8(h3Core.latLngToCell(latitude, longitude, 8));
            if (parameters.isSaveArrow()) {
                result.getArrowNodeOrWays().add(new ArrowNodeOrWay(id, h33, h38, latitude, longitude, tags));
            }
            if (parameters.isSavePostgresqlTsv()) {
                StringBuilder resultBuilder = result.getCsvResultPerH33().computeIfAbsent(h33, key -> new StringBuilder());
                serializer.serializeNode(resultBuilder, h33, h38, entity, tags);
            }
        }
    }

    private long processWays(List<EntityContainer> decodedEntities, BlockResult result, Map<Short, Stat> wayStat) {
        long wayCount = 0;
        for (EntityContainer entityContainer : decodedEntities) {
            if (entityContainer instanceof WayContainer) {
                processWay(((WayContainer) entityContainer).getEntity(), result, wayStat);
                wayCount++;
            }
        }
        return wayCount;
    }

    private void processWay(Way entity, BlockResult result, Map<Short, Stat> wayStat) {
        if (isSkipped(entity)) {
            return;
        }
        long id = entity.getId();
        List<WayNode> wayNodes = entity.getWayNodes();
        int version = version(entity);
        long geomHash = parameters.isUpdatable() ? WayGeometry.geomHash(wayNodes) : 0;
        if (storedVersions.unchangedWay(id, version, geomHash)) {
            return;
        }

        short[] h3Idxs = new short[wayNodes.size()];
        for (int i = 0; i < wayNodes.size(); i++) {
            WayNode wn = wayNodes.get(i);
            h3Idxs[i] = CompactH3.serialize3(h3Core.latLngToCell(wn.getLatitude(), wn.getLongitude(), 3));
        }
        boolean isInOneH33 = isInSingleH3Segment(h3Idxs);
        short h33 = isInOneH33 ? h3Idxs[0] : Short.MAX_VALUE;
        short[] h33Regions = isInOneH33 ? null : distinctSorted(h3Idxs);

        Stat stat = wayStat.computeIfAbsent(h33, Stat::new);
        stat.incrementCount();
        stat.updateIdStat(id);
        stat.updateLastModified(entity.getTimestamp().getTime());
        stat.incrementSize(2 + 8);
        stat.incrementSize(wayNodes.size() * 16);
        Collection<Tag> tags = getTags(entity);
        updateSizeStatsByTags(tags, stat);

        if (!parameters.isCollectOnlyStat()) {
            WayGeometry geom = WayGeometry.from(entity, h3Core, geometryFactory,
                    geodeticCalculator, parameters.isScaleApproximation(), wkbWriter);

            if (parameters.isSaveArrow()) {
                int[] h38Idxs = !geom.getWayIntersectionH38Indexes().isEmpty()
                        ? geom.getWayIntersectionH38Indexes().stream().mapToInt(Integer::intValue).toArray()
                        : null;
                ArrowNodeOrWay arrowNodeOrWay = new ArrowNodeOrWay.Builder(
                        id, h33, geom.getH38(), geom.getLatitude(), geom.getLongitude(), tags)
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
                        .h33Regions(h33Regions)
                        .centre(geom.getCentre()).scaleDim(geom.getScaleDim())
                        .bboxGeometry(geom.getBboxGeometry()).lineString(geom.getLineString())
                        .tags(tags)
                        .version(version).geomHash(geomHash)
                        .build();
                serializer.serializeWay(resultBuilder, wayData);
            }
        }
    }

    private boolean isSkipped(Way entity) {
        return (parameters.isSkipBuildings() && hasTag(entity, "building"))
                || (parameters.isSkipHighway() && hasTag(entity, "highway"));
    }

    private static boolean hasTag(Way entity, String key) {
        return entity.getTags().stream().anyMatch(tag -> key.equals(tag.getKey()));
    }

    private long processRelations(List<EntityContainer> decodedEntities, BlockResult result) {
        long relationCount = 0;
        for (EntityContainer entityContainer : decodedEntities) {
            if (entityContainer instanceof RelationContainer) {
                Relation relation = ((RelationContainer) entityContainer).getEntity();
                int version = version(relation);
                if (!parameters.isCollectOnlyStat() && !storedVersions.unchangedRelation(relation.getId(), version)) {
                    serializeRelation(relation, version, result);
                }
                relationCount++;
            }
        }
        return relationCount;
    }

    private void serializeRelation(Relation entity, int version, BlockResult result) {
        long relationId = entity.getId();
        Collection<Tag> tags = getTags(entity);
        if (parameters.isSavePostgresqlTsv()) {
            StringBuilder relationCsv = result.getCsvResultPerH33().computeIfAbsent((short) 0, k -> new StringBuilder());
            serializer.serializeRelation(relationCsv, relationId, tags, version);
        }

        ArrowRelation arrowRelation = null;
        if (parameters.isSaveArrow()) {
            arrowRelation = new ArrowRelation(relationId, TagsUtil.tagsToMap(tags));
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

    static short[] distinctSorted(short[] values) {
        short[] sorted = values.clone();
        Arrays.sort(sorted);
        int count = 0;
        for (int i = 0; i < sorted.length; i++) {
            if (i == 0 || sorted[i] != sorted[count - 1]) {
                sorted[count++] = sorted[i];
            }
        }
        return Arrays.copyOf(sorted, count);
    }

    private static boolean isInSingleH3Segment(short[] h3Idxs) {
        for (int i = 1; i < h3Idxs.length; i++) {
            if (h3Idxs[i] != h3Idxs[0]) {
                return false;
            }
        }
        return true;
    }

    private static void updateSizeStatsByTags(Collection<Tag> tags, Stat stat) {
        for (Tag tag : tags) {
            stat.incrementSize(tag.getKey().length());
            stat.incrementSize(1);
            stat.incrementSize(tag.getValue().length());
        }
    }

    private int version(Entity entity) {
        int version = entity.getVersion();
        if (version < 1 && parameters.isUpdatable()) {
            throw new IllegalStateException(entity.getType() + " " + entity.getId() + " has no version: "
                    + "-updatable and -update need a PBF with object metadata");
        }
        return version;
    }

    private Collection<Tag> getTags(Entity entity) {
        Collection<Tag> sourceTags = entity.getTags();
        List<Tag> tags = null;
        for (TagExtension tagExtension : tagExtensions) {
            List<Tag> additionalTags = tagExtension.additionalTags(entity);
            if (!additionalTags.isEmpty()) {
                if (tags == null) {
                    tags = new ArrayList<>(sourceTags);
                }
                tags.addAll(additionalTags);
            }
        }
        return tags != null ? tags : sourceTags;
    }
}
