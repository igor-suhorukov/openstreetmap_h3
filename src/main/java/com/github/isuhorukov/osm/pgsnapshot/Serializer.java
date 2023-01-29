package com.github.isuhorukov.osm.pgsnapshot;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.Stat;
import com.github.isuhorukov.osm.pgsnapshot.util.HStoreFormatSerializer;
import net.postgis.jdbc.geometry.LineString;
import net.postgis.jdbc.geometry.Point;
import net.postgis.jdbc.geometry.Polygon;
import net.postgis.jdbc.geometry.binary.BinaryWriter;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.util.CollectionWrapper;

import java.util.*;
import java.util.stream.Collectors;

public class Serializer {
    public static final int SRID = 4326;

    public static void serializeNode(StringBuilder csvString, BinaryWriter binaryWriter, short h33, int h38,
                                     long id, double latitude, double longitude, Collection<Tag> tags){
        csvString.append(h33).append("\t").append(h38).append('\t').append(id).append("\t")
                .append(binaryWriter.writeHexed(getPoint(latitude, longitude)));
        csvString.append("\t");
        HStoreFormatSerializer.renderTags(tags, csvString);
        csvString.append("\n");
    }
    public static void  serializeWay(StringBuilder csvString, BinaryWriter binaryWriter,
                                     boolean closed, boolean nonValid,
                                     short h33, int h38, long id,
                                     long[] pointsIdx, Set<Integer> wayIntersectionH38Indexes,
                                     Point centre, float scaleDim,
                                     Polygon bboxGeometry, LineString lineString, Collection<Tag> tags){

        if(nonValid){
            Tag nonValidLine = new Tag("is.line_non_valid", "true");
            CollectionWrapper<Tag> wrapper = (CollectionWrapper<Tag>) tags;
            wrapper.add(nonValidLine);
        }
        boolean building = false;
        boolean highway = false;
        for (Tag currentTag : tags) {
            if ("building".equalsIgnoreCase(currentTag.getKey())) {
                building = true;
            } else if ("highway".equalsIgnoreCase(currentTag.getKey())) {
                highway = true;
            }
            if (building && highway) {
                break;
            }
        }
        String pointIdxs = Arrays.stream(pointsIdx).mapToObj(Long::toString).collect(Collectors.joining(","));
/*
        byte[] linestingBinary = binaryWriter.writeBinary(lineString);
        https://postgis.net/docs/ST_GeomFromEWKB.html
*/
        csvString.append(h33).append("\t").append(h38).append("\t")
        .append(id).append("\t")
        .append(closed?'t':'f').append("\t")
        .append(building?'t':'f').append("\t")
        .append(highway?'t':'f').append("\t")
        .append(scaleDim).append("\t")
        .append(binaryWriter.writeHexed(centre)).append("\t")
        .append(binaryWriter.writeHexed(bboxGeometry)).append("\t")
        .append(binaryWriter.writeHexed(lineString)).append("\t")
        .append('{').append(pointIdxs).append('}').append("\t");
        if(wayIntersectionH38Indexes!=null && !wayIntersectionH38Indexes.isEmpty()){
            String h38Indexes = wayIntersectionH38Indexes.stream().
                                                    map(Object::toString).collect(Collectors.joining(","));
            csvString.append('{').append(h38Indexes).append('}').append("\t");
        } else {
            csvString.append(HStoreFormatSerializer.NULL_STRING).append('\t');
        }
        HStoreFormatSerializer.renderTags(tags, csvString);
        csvString.append("\n");
    }

    public static void serializeRelation(StringBuilder csvString, long id, Collection<Tag> tags){
        csvString.append(id).append("\t");
        HStoreFormatSerializer.renderTags(tags, csvString);
        csvString.append("\n");
    }

    public static void serializeRelationMembers(StringBuilder csvString,
                                    long relationId, long memberId,String memberType,String memberRole, int sequenceId){
        csvString.append(relationId).append("\t").append(memberId).append("\t").append(memberType).append("\t");
        HStoreFormatSerializer.escapeString(csvString, memberRole);
        csvString.append("\t").append(sequenceId);
        csvString.append("\n");
    }

    public static Point getPoint(double latitude, double longitude) {
        Point point = new Point(longitude, latitude);
        point.setSrid(SRID);
        return point;
    }

    public static void serializePbfStat(StringBuilder csvString, PbfStatistics pbfStatistics){
        csvString.append(pbfStatistics.getMultipolygonCount()).append('\t');
        csvString.append(pbfStatistics.getDataProcessingTime()).append('\t');
        csvString.append(pbfStatistics.getPbfSplitTime()).append('\t');
        csvString.append(pbfStatistics.getAddLocationsToWaysTime()).append('\t');
        csvString.append(pbfStatistics.getMultipolygonExportTime()).append('\t');
        csvString.append(pbfStatistics.getSplitMultipolygonByPartsTime()).append('\t');
        csvString.append(pbfStatistics.getTotalTime()).append('\n');
    }

    public static void serializeBlockStat(StringBuilder csvString, List<BlockStat> blockStats){
        for(BlockStat blockStat: blockStats){
            csvString.append(blockStat.getId()).append('\t');
            csvString.append(blockStat.getNodeCount()).append('\t');
            csvString.append(blockStat.getWayCount()).append('\t');
            csvString.append(blockStat.getRelationCount()).append('\t');
            csvString.append(blockStat.getRelationMembersCount()).append('\t');
            csvString.append(blockStat.getMultipolygonCount()).append('\t');
            csvString.append(blockStat.getProcessingTime()).append('\t');
            csvString.append(blockStat.getWaitingForSaveTime()).append('\t');
            csvString.append(blockStat.getSaveTime()).append('\t');
            csvString.append(blockStat.getThreadTime()).append('\t');
            csvString.append(blockStat.getThreadStart()).append('\n');
        }
    }

    public static void serializeBlockContent(StringBuilder csvString, BlockStat blockStat){
        if (blockStat.getRelationCount() > 0) {
            return;
        }
        if (blockStat.getNodeCount() > 0 && blockStat.getWayCount() > 0) {
            throw new RuntimeException("Mixed content in block is not supported");
        }
        Map<Short, Stat> statistics = null;
        String objectType = null;
        if (blockStat.getNodeCount() > 0 && blockStat.getNodeStat()!=null) {
            statistics = new TreeMap<>(blockStat.getNodeStat());
            objectType = "N";
        }
        if (blockStat.getWayCount() > 0 && blockStat.getWayStat()!=null) {
            statistics = new TreeMap<>(blockStat.getWayStat());
            objectType = "W";
        }
        for(Map.Entry<Short, Stat> statisticBlock : Objects.requireNonNull(statistics).entrySet()){
            Short blockId = statisticBlock.getKey();
            Stat blockValue = statisticBlock.getValue();
            csvString.append(objectType).append('\t');
            csvString.append(blockId).append('\t');
            csvString.append(blockValue.getH33()).append('\t');
            csvString.append(blockValue.getCount()).append('\t');
            csvString.append(blockValue.getSize()).append('\t');
            csvString.append(blockValue.getMinId()).append('\t');
            csvString.append(blockValue.getMaxId()).append('\t');
            csvString.append(blockValue.getLastModified()).append('\n');
        }
    }
}
