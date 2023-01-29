package com.github.isuhorukov.osm.pgsnapshot;

import com.beust.jcommander.JCommander;
import com.github.isuhorukov.osm.pgsnapshot.model.*;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.MultipolygonTime;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.Stat;
import com.github.isuhorukov.osm.pgsnapshot.model.table.StatType;
import com.github.isuhorukov.osm.pgsnapshot.model.table.TableStat;
import com.github.isuhorukov.osm.pgsnapshot.util.CompactH3;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;
import com.google.common.util.concurrent.MoreExecutors;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import net.postgis.jdbc.geometry.LineString;
import net.postgis.jdbc.geometry.LinearRing;
import net.postgis.jdbc.geometry.Point;
import net.postgis.jdbc.geometry.Polygon;
import net.postgis.jdbc.geometry.binary.BinaryWriter;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.io.IOUtils;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.h2gis.functions.spatial.properties.ST_IsClosed;
import org.locationtech.jts.algorithm.MinimumBoundingCircle;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBWriter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoder;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfBlobDecoderListener;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.MemberTypeValueMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class OsmPbfTransformation {

    public static final String NODES_DIR = "nodes";
    public static final String RELATIONS_DIR = "relations";
    public static final String WAYS_DIR = "ways";
    public static final String MULTIPOLYGON_DIR = "multipolygon";
    public static final String SQL_DIR = "sql";
    public static final String ARROW_DIR = "arrow";
    public static final String IMPORT_RELATED_METADATA_DIR = "import_related_metadata";
    public static final String STATIC_DIR = "static";


    // /home/iam/dev/map/thailand/thailand-latest.osm.pbf
    // /home/iam/dev/map/maldives/maldives-latest.osm.pbf
    // /home/iam/dev/map/planet-220704/planet-220704.osm.pbf
    // /home/iam/dev/map/indonesia/indonesia-latest.osm.pbf
    public static void main(String[] args) throws Exception{

        CliParameters parameters = parseCliArguments(args);
        if (parameters == null){
            return;
        }
        long commandStartTime = System.currentTimeMillis();

        String sourceFilePath = parameters.sourceFilePath;
        File sourcePbfFile = new File(sourceFilePath);
        if(!sourcePbfFile.exists() || sourcePbfFile.length()==0){
            throw new IllegalArgumentException("Input pbf should exists and should be non empty");
        }

        Splitter.Blocks blocks =ExternalProcessing.enrichSourcePbfAndSplitIt(sourcePbfFile);

        File inputDirectory = new File(blocks.getDirectory());
        File[] files = inputDirectory.listFiles();
        Arrays.sort(Objects.requireNonNull(files));
        File resultDirectory = prepareResultDirectories(new File(inputDirectory.getParent(),
                                                            resultDirectoryNameFromSource(inputDirectory)),
                parameters.savePostgresqlTsv, parameters.saveArrow);

        copyOsmiumSettings(resultDirectory);
        if(parameters.savePostgresqlTsv) {
            copyResources(resultDirectory, parameters.columnarStorage);
        }

        long processingStartTime = System.currentTimeMillis();

        final H3Core h3Core = H3Core.newInstance();


        ExecutorService saveExecutorService = getExecutorService(parameters.workers);
        ExecutorService executorService = getExecutorService(parameters.workers);

        Map<Long, BlockStat> blockStat= new ConcurrentHashMap<>();
        AtomicInteger currentBlockToSave= new AtomicInteger(0);
        for(File blockFile: files){

            executorService.submit(() -> {
                long threadStart = System.currentTimeMillis();
                Long blockNumber = Long.parseLong(blockFile.getName());
                if(blockNumber%1000==0){
                    System.out.println(blockNumber);
                }
                RawBlob rawBlob;
                try {
                    FileInputStream blobInputStream = new FileInputStream(blockFile);
                    rawBlob = new RawBlob("OSMData", IOUtils.toByteArray(blobInputStream));
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }

                PbfBlobDecoder blobDecoder = new PbfBlobDecoder(rawBlob, new PbfBlobDecoderListener() {
                    @Override
                    public void complete(List<EntityContainer> decodedEntities) {
                        long blockStartTime = System.currentTimeMillis();
                        GeometryFactory geometryFactory = new GeometryFactory();
                        MemberTypeValueMapper memberTypeValueMapper = new MemberTypeValueMapper();
                        Map<Short, StringBuilder> csvResultPerH33 =new HashMap<>();
                        BinaryWriter binaryWriter = new BinaryWriter();
                        ArrayList<ArrowNodeOrWay> arrowNodeOrWays = new ArrayList<>();
                        ArrayList<ArrowRelation> arrowRelations = new ArrayList<>();
                        WKBWriter wkbWriter = new WKBWriter();
                        final CoordinateReferenceSystem coordinateReferenceSystem;
                        try {
                            coordinateReferenceSystem = CRS.decode("EPSG:" + Serializer.SRID);
                        } catch (FactoryException e) {
                            throw new RuntimeException(e);
                        }

                        Map<Short, Stat> nodeStat =new HashMap<>();
                        long nodeRecords = decodedEntities.stream().
                                filter(entityContainer -> entityContainer instanceof NodeContainer).
                            map(entityContainer -> ((NodeContainer) entityContainer).getEntity()).map(entity -> {
                                prepareNodeData(csvResultPerH33, binaryWriter, arrowNodeOrWays,
                                        nodeStat, entity, h3Core, parameters.collectOnlyStat, parameters.saveArrow, parameters.savePostgresqlTsv);
                                return null;
                            }).filter(Objects::isNull).count();

                        Map<Short, Stat> wayStat =new HashMap<>();
                        long wayRecords = decodedEntities.stream().
                                filter(entityContainer -> entityContainer instanceof WayContainer).
                            map(entityContainer -> ((WayContainer) entityContainer).getEntity()).map(entity -> {
                            prepareWayData(geometryFactory, csvResultPerH33, binaryWriter,  wkbWriter,
                                    arrowNodeOrWays, wayStat, entity, h3Core,
                                    parameters.scaleApproximation, parameters.collectOnlyStat, parameters.skipBuildings,
                                    coordinateReferenceSystem, parameters.saveArrow, parameters.savePostgresqlTsv);
                                return null;
                            }).filter(Objects::isNull).count();
                        BlockStat blockStatistic = new BlockStat(blockNumber);
                        blockStatistic.setThreadStart(threadStart);
                        if(!nodeStat.isEmpty()) {
                            blockStatistic.setNodeStat(nodeStat);
                        }
                        if(!wayStat.isEmpty()) {
                            blockStatistic.setWayStat(wayStat);
                        }
                        long relationCount = decodedEntities.stream().
                            filter(entityContainer -> entityContainer instanceof RelationContainer).
                            map(entityContainer -> ((RelationContainer) entityContainer).getEntity()).
                            map(entity -> {
                                if(!parameters.collectOnlyStat) {
                                    long relationId = entity.getId();
                                    if(parameters.savePostgresqlTsv){
                                        StringBuilder relationCsv = csvResultPerH33.computeIfAbsent((short)0, h33Key -> new StringBuilder());
                                        Serializer.serializeRelation(relationCsv, relationId, entity.getTags());
                                    }

                                    ArrowRelation arrowRelation = null;
                                    if(parameters.saveArrow){
                                        arrowRelation = new ArrowRelation(relationId, TagsUtil.tagsToMap(entity.getTags()));
                                        arrowRelations.add(arrowRelation);
                                    }

                                    List<RelationMember> relationMembers = entity.getMembers();
                                    for(int sequenceId=0; sequenceId<relationMembers.size();sequenceId++){
                                        RelationMember relationMember = relationMembers.get(sequenceId);
                                        long memberId = relationMember.getMemberId();
                                        String memberType = memberTypeValueMapper.getMemberType(relationMember.getMemberType());
                                        String memberRole = relationMember.getMemberRole();
                                        if(parameters.saveArrow){
                                            arrowRelation.getRelationMembers().add(
                                                    new ArrowRelationMember(memberId, memberType.charAt(0), memberRole));
                                        }
                                        if(parameters.savePostgresqlTsv){
                                            StringBuilder relationMembersCsv = csvResultPerH33.computeIfAbsent((short)1, h33Key -> new StringBuilder());
                                            Serializer.serializeRelationMembers(relationMembersCsv, relationId,
                                                    memberId, memberType, memberRole, sequenceId);
                                        }
                                    }
                                }
                                return null;
                            }).count();
                        long multipolygonCount = relationCount==0 ? 0 : decodedEntities.stream().
                            filter(entityContainer -> entityContainer instanceof RelationContainer).
                            map(entity -> {
                                for(Tag tag: entity.getEntity().getTags()){
                                    if("type".equals(tag.getKey()) && "multipolygon".equals(tag.getValue())){
                                        return 1;
                                    }
                                }
                                return 0;
                            }).mapToLong(Integer::longValue).sum();
                        long relationMemberCount = decodedEntities.stream().
                                filter(entityContainer -> entityContainer instanceof RelationContainer).
                                map(entityContainer -> ((RelationContainer) entityContainer).getEntity()).
                                mapToLong(value -> value.getMembers().size()).sum();
                        blockStatistic.setNodeCount(nodeRecords);
                        blockStatistic.setWayCount(wayRecords);
                        blockStatistic.setRelationCount(relationCount);
                        blockStatistic.setRelationMembersCount(relationMemberCount);
                        blockStatistic.setMultipolygonCount(multipolygonCount);
                        blockStat.put(blockNumber, blockStatistic);
                        blockStatistic.setProcessingTime(System.currentTimeMillis()-blockStartTime);

                        if(!parameters.collectOnlyStat) {
                            if(parameters.saveArrow){
                                long startSaveTime = System.currentTimeMillis();
                                if(!arrowNodeOrWays.isEmpty()){
                                    saveArrowNodesOrWays(arrowNodeOrWays, blockNumber, new File(resultDirectory,ARROW_DIR));
                                }
                                if(!arrowRelations.isEmpty()){
                                    saveArrowRelations(arrowRelations, blockNumber, new File(resultDirectory, ARROW_DIR));
                                }
                                blockStatistic.setSaveTime(System.currentTimeMillis()-startSaveTime);
                            }
                            if(parameters.savePostgresqlTsv){
                                saveDataOnlyInOneThread(csvResultPerH33, nodeRecords, wayRecords,
                                    blockStatistic, relationCount,
                                    currentBlockToSave, blockNumber, resultDirectory, saveExecutorService);
                            }
                        }
                    }

                    @Override
                    public void error() {
                        System.out.println("ERROR in block "+blockNumber);
                    }
                });
                blobDecoder.run();
                long threadTime = System.currentTimeMillis() - threadStart;
                blockStat.get(blockNumber).setThreadTime(threadTime);
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(2,TimeUnit.DAYS);
        saveExecutorService.shutdown();//stop executor only when all tasks in processing executor is finished

        List<BlockStat> blockStatistics = new ArrayList<BlockStat>(blockStat.values());
        long multipolygonCount = blockStatistics.stream().map(BlockStat::getMultipolygonCount).mapToLong(Long::longValue).sum();
        long dataProcessingTime = System.currentTimeMillis() - processingStartTime;
        System.out.println(files.length+" "+" time "+dataProcessingTime);
        System.out.println("diff between total and processing " + blockStatistics.stream().map(blockStat1 -> blockStat1.getThreadTime()-blockStat1.getProcessingTime()).mapToLong(Long::longValue).sum());
        System.out.println("total thread time "+ blockStatistics.stream().map(BlockStat::getThreadTime).mapToLong(Long::longValue).sum());
        System.out.println("total processing time "+ blockStatistics.stream().map(BlockStat::getProcessingTime).mapToLong(Long::longValue).sum());
        System.out.println("total save time "+ blockStatistics.stream().map(BlockStat::getSaveTime).mapToLong(Long::longValue).sum());
        System.out.println("total waiting for save time "+ blockStatistics.stream().map(BlockStat::getWaitingForSaveTime).mapToLong(Long::longValue).sum());
        System.out.println("thread max time "+ blockStatistics.stream().map(BlockStat::getThreadTime).mapToLong(Long::longValue).max().orElse(0));
        System.out.println("processing max time "+ blockStatistics.stream().map(BlockStat::getProcessingTime).mapToLong(Long::longValue).max().orElse(0));
        System.out.println("nodes "+ blockStatistics.stream().map(BlockStat::getNodeCount).mapToLong(Long::longValue).sum());
        System.out.println("ways "+ blockStatistics.stream().map(BlockStat::getWayCount).mapToLong(Long::longValue).sum());
        System.out.println("relations "+ blockStatistics.stream().map(BlockStat::getRelationCount).mapToLong(Long::longValue).sum());
        System.out.println("relation members "+ blockStatistics.stream().map(BlockStat::getRelationMembersCount).mapToLong(Long::longValue).sum());
        System.out.println("multipolygon count "+ multipolygonCount);


        if(!parameters.collectOnlyStat) {
            savePartitioningScripts(resultDirectory, parameters.scriptCount,
                    parameters.thresholdPercentFromMaxPartition, blockStatistics, parameters.columnarStorage);
        }

        MultipolygonTime multipolygonTime = new MultipolygonTime(); //multipolygonCount calculation is only one reason why this generator at the end of process
        if(!parameters.collectOnlyStat) {
            multipolygonTime = ExternalProcessing.prepareMultipolygonDataAndScripts(sourcePbfFile,
                    resultDirectory, parameters.scriptCount, multipolygonCount);
        }
        PbfStatistics statistics = new PbfStatistics(blockStatistics);
        statistics.setMultipolygonCount(multipolygonCount);
        statistics.setDataProcessingTime(dataProcessingTime);
        statistics.setAddLocationsToWaysTime(blocks.getAddLocationsToWaysTime());
        statistics.setPbfSplitTime(blocks.getPbfSplitTime());
        statistics.setMultipolygonExportTime(multipolygonTime.getMultipolygonExportTime());
        statistics.setSplitMultipolygonByPartsTime(multipolygonTime.getSplitMultipolygonByPartsTime());
        statistics.setTotalTime(System.currentTimeMillis()-commandStartTime);

        saveStatistics(resultDirectory, statistics);

    }

    private static CliParameters parseCliArguments(String[] args) {
        CliParameters parameters = new CliParameters();
        JCommander jc = JCommander.newBuilder().addObject(parameters).build();
        try {
            jc.parse(args);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            jc.usage();
            return null;
        }
        if(parameters.help){
            jc.usage();
            return null;
        }
        return parameters;
    }

    private static void saveArrowRelations(ArrayList<ArrowRelation> arrowRelations,  Long blockNumber, File resultDirectory) {
        if(arrowRelations==null || arrowRelations.isEmpty()){
            return;
        }
        Schema schema = getRelationSchema();
        try (BufferAllocator allocator = new RootAllocator()) {

            try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)) {
                BigIntVector idVector = (BigIntVector) vectorSchemaRoot.getVector("id");
                MapVector mapVector = (MapVector) vectorSchemaRoot.getVector("tags");
                ListVector memberIdVector = (ListVector) vectorSchemaRoot.getVector("memberId");
                ListVector memberTypeVector = (ListVector) vectorSchemaRoot.getVector("memberType");
                ListVector memberRoleVector = (ListVector) vectorSchemaRoot.getVector("memberRole");

                UnionListWriter memberIdVectorWriter = memberIdVector.getWriter();
                UnionListWriter memberTypeVectorWriter = memberTypeVector.getWriter();
                UnionListWriter memberRoleVectorWriter = memberRoleVector.getWriter();

                memberIdVectorWriter.allocate();
                memberTypeVectorWriter.allocate();
                memberRoleVectorWriter.allocate();

                idVector.allocateNew(arrowRelations.size());
                mapVector.allocateNew();
                UnionMapWriter mapWriter = mapVector.getWriter();

                for(int idx = 0; idx< arrowRelations.size(); idx++) {
                    ArrowRelation arrowRelation = arrowRelations.get(idx);
                    List<ArrowRelationMember> relationMembers = arrowRelation.getRelationMembers();

                    idVector.set(idx, arrowRelation.getId());
                    writeTagsToArrow(allocator, mapWriter, idx, arrowRelation.getTags());

                    memberIdVectorWriter.setPosition(idx);
                    memberTypeVectorWriter.setPosition(idx);
                    memberRoleVectorWriter.setPosition(idx);
                    if(!relationMembers.isEmpty()){
                        memberIdVectorWriter.startList();
                        memberTypeVectorWriter.startList();
                        memberRoleVectorWriter.startList();
                        for(int pidx = 0; pidx< relationMembers.size(); pidx++){
                            ArrowRelationMember arrowRelationMember = relationMembers.get(pidx);
                            memberIdVectorWriter.bigInt().writeBigInt(arrowRelationMember.getMemberId());
                            memberTypeVectorWriter.tinyInt().writeTinyInt((byte)arrowRelationMember.getMemberType());
                            byte[] roleBytes = arrowRelationMember.getMemberRole().getBytes(StandardCharsets.UTF_8);
                            try (ArrowBuf roleBytesBuf = allocator.buffer(roleBytes.length)){
                                roleBytesBuf.writeBytes(roleBytes);
                                memberRoleVectorWriter.writeVarChar(0,roleBytes.length, roleBytesBuf);
                            }
                        }
                        memberIdVectorWriter.endList();
                        memberTypeVectorWriter.endList();
                        memberRoleVectorWriter.endList();
                        memberIdVector.setLastSet(idx);
                        memberTypeVector.setLastSet(idx);
                        memberRoleVector.setLastSet(idx);

                    } else {
                        memberIdVectorWriter.setAddVectorAsNullable(true);
                        memberTypeVectorWriter.setAddVectorAsNullable(true);
                        memberRoleVectorWriter.setAddVectorAsNullable(true);
                    }
                }
                memberIdVector.setValueCount(arrowRelations.size());
                memberTypeVector.setValueCount(arrowRelations.size());
                memberRoleVector.setValueCount(arrowRelations.size());
                mapWriter.setValueCount(arrowRelations.size());
                vectorSchemaRoot.setRowCount(arrowRelations.size());
                try (
                        FileOutputStream fileOutputStream = new FileOutputStream(new File(resultDirectory, String.format("%s/%08d.arrow", RELATIONS_DIR, blockNumber)));
                        ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null,  fileOutputStream.getChannel())
                ) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private static void saveArrowNodesOrWays(ArrayList<ArrowNodeOrWay> arrowNodeOrWays, Long blockNumber, File resultDirectory) {
        if(arrowNodeOrWays ==null || arrowNodeOrWays.isEmpty()){
            return;
        }
        boolean isWays = arrowNodeOrWays.stream().anyMatch(arrowNodeOrWay -> arrowNodeOrWay.getPointIdxs()!=null);
        //test purpose only: arrowRows = new ArrayList<>(arrowRows.subList(0,10));
        Schema schema = getNodesOrWaysSchema(isWays);

        try (BufferAllocator allocator = new RootAllocator()) {

            try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)){
                BigIntVector idVector = (BigIntVector) vectorSchemaRoot.getVector("id");
                SmallIntVector h33Vector = (SmallIntVector) vectorSchemaRoot.getVector("h33");
                IntVector h38Vector = (IntVector)vectorSchemaRoot.getVector("h38");
                Float8Vector latitudeVector = (Float8Vector) vectorSchemaRoot.getVector("latitude");
                Float8Vector longitudeVector = (Float8Vector) vectorSchemaRoot.getVector("longitude");
                MapVector mapVector = (MapVector) vectorSchemaRoot.getVector("tags");
                idVector.allocateNew(arrowNodeOrWays.size());
                h33Vector.allocateNew(arrowNodeOrWays.size());
                h38Vector.allocateNew(arrowNodeOrWays.size());
                latitudeVector.allocateNew(arrowNodeOrWays.size());
                longitudeVector.allocateNew(arrowNodeOrWays.size());
                UnionMapWriter mapWriter = mapVector.getWriter();
                mapVector.allocateNew();

                ListVector pointIdxsVector = null;
                SmallIntVector h33CenterVector = null;
                BitVector isClosed = null;
                BitVector building = null;
                BitVector highway = null;
                Float4Vector scale = null;
                VarBinaryVector lineStringWkb = null;
                ListVector h38IndexesVector = null;
                Float8Vector bboxMinX = null;
                Float8Vector bboxMaxX = null;
                Float8Vector bboxMinY = null;
                Float8Vector bboxMaxY = null;
                UnionListWriter pointIdxsVectorWriter = null;
                UnionListWriter h38IndexesVectorWriter = null;

                if(isWays){
                    pointIdxsVector = (ListVector) vectorSchemaRoot.getVector("pointIdxs");
                    h33CenterVector = (SmallIntVector) vectorSchemaRoot.getVector("h33Center");
                    isClosed = (BitVector) vectorSchemaRoot.getVector("closed");
                    building = (BitVector) vectorSchemaRoot.getVector("building");
                    highway = (BitVector) vectorSchemaRoot.getVector("highway");
                    scale = (Float4Vector) vectorSchemaRoot.getVector("scale");
                    lineStringWkb = (VarBinaryVector) vectorSchemaRoot.getVector("lineStringWkb");
                    h38IndexesVector = (ListVector) vectorSchemaRoot.getVector("h38Indexes");
                    bboxMinX = (Float8Vector) vectorSchemaRoot.getVector("bboxMinX");
                    bboxMaxX = (Float8Vector) vectorSchemaRoot.getVector("bboxMaxX");
                    bboxMinY = (Float8Vector) vectorSchemaRoot.getVector("bboxMinY");
                    bboxMaxY = (Float8Vector) vectorSchemaRoot.getVector("bboxMaxY");
                    pointIdxsVectorWriter = pointIdxsVector.getWriter();
                    pointIdxsVectorWriter.allocate();
                    h38IndexesVectorWriter = h38IndexesVector.getWriter();
                    h38IndexesVectorWriter.allocate();
                    h33CenterVector.allocateNew(arrowNodeOrWays.size());
                    isClosed.allocateNew(arrowNodeOrWays.size());
                    building.allocateNew(arrowNodeOrWays.size());
                    highway.allocateNew(arrowNodeOrWays.size());
                    scale.allocateNew(arrowNodeOrWays.size());
                    lineStringWkb.allocateNew(1024*1024*10, arrowNodeOrWays.size());
                    bboxMinX.allocateNew(arrowNodeOrWays.size());
                    bboxMaxX.allocateNew(arrowNodeOrWays.size());
                    bboxMinY.allocateNew(arrowNodeOrWays.size());
                    bboxMaxY.allocateNew(arrowNodeOrWays.size());
                }

                for(int idx = 0; idx< arrowNodeOrWays.size(); idx++){
                    ArrowNodeOrWay arrowNodeOrWay = arrowNodeOrWays.get(idx);
                    long arrowNodeOrWayId = arrowNodeOrWay.getId();
                    idVector.set(idx, arrowNodeOrWayId);
                    h33Vector.set(idx, arrowNodeOrWay.getH33());
                    h38Vector.set(idx, arrowNodeOrWay.getH38());
                    latitudeVector.set(idx, arrowNodeOrWay.getLatitude());
                    longitudeVector.set(idx, arrowNodeOrWay.getLongitude());
                    Map<String, String> tags = arrowNodeOrWay.getTags();
                    writeTagsToArrow(allocator, mapWriter, idx, tags);
                    if(isWays){
                        pointIdxsVectorWriter.setPosition(idx);
                        pointIdxsVectorWriter.startList();
                        for(int pidx = 0; pidx< arrowNodeOrWay.getPointIdxs().length; pidx++){
                            pointIdxsVectorWriter.bigInt().writeBigInt(arrowNodeOrWay.getPointIdxs()[pidx]);
                        }
                        pointIdxsVectorWriter.setValueCount(arrowNodeOrWay.getPointIdxs().length);
                        pointIdxsVectorWriter.endList();
                        pointIdxsVector.setLastSet(idx);
                        if(arrowNodeOrWay.getH38Indexes()!=null && arrowNodeOrWay.getH38Indexes().length>0){
                            h38IndexesVectorWriter.setPosition(idx);
                            h38IndexesVectorWriter.startList();
                            for(int pidx = 0; pidx< arrowNodeOrWay.getH38Indexes().length; pidx++){
                                h38IndexesVectorWriter.integer().writeInt(arrowNodeOrWay.getH38Indexes()[pidx]);
                            }
                            h38IndexesVectorWriter.setValueCount(arrowNodeOrWay.getH38Indexes().length);
                            h38IndexesVectorWriter.endList();
                            h38IndexesVector.setLastSet(idx);
                        } else {
                            h38IndexesVectorWriter.setPosition(idx);
                            h38IndexesVectorWriter.setAddVectorAsNullable(true);
                        }
                        h33CenterVector.set(idx, arrowNodeOrWay.getH33Center());
                        isClosed.set(idx, arrowNodeOrWay.isClosed()?1:0);
                        building.set(idx, arrowNodeOrWay.isBuilding()?1:0);
                        highway.set(idx, arrowNodeOrWay.isHighway()?1:0);
                        scale.set(idx, arrowNodeOrWay.getScaleDim());
                        lineStringWkb.set(idx, arrowNodeOrWay.getLineStringWkb());
                        bboxMinX.set(idx, arrowNodeOrWay.getBboxMinX());
                        bboxMaxX.set(idx, arrowNodeOrWay.getBboxMaxX());
                        bboxMinY.set(idx, arrowNodeOrWay.getBboxMinY());
                        bboxMaxY.set(idx, arrowNodeOrWay.getBboxMaxY());
                    }
                    //mapWriter.setPosition(idx);
                }
                mapWriter.setValueCount(arrowNodeOrWays.size());
                if(isWays){
                    pointIdxsVector.setValueCount(arrowNodeOrWays.size());
                    h38IndexesVector.setValueCount(arrowNodeOrWays.size());
                }
                vectorSchemaRoot.setRowCount(arrowNodeOrWays.size());
                try (
                        FileOutputStream fileOutputStream = new FileOutputStream(new File(resultDirectory, String.format("%s/%08d.arrow",isWays ? WAYS_DIR:NODES_DIR,blockNumber)));
                        ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel())
                ) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                    //https://arrow.apache.org/cookbook/java/io.html#writing
/*                    System.out.println("Record batches written: " + writer.getRecordBlocks().size() +
                            ". Number of rows written: " + vectorSchemaRoot.getRowCount());*/
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Throwable e){
            System.out.println("block "+blockNumber+" "+e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }
    }

    static Schema getNodesOrWaysSchema(boolean isWays) {
        FieldType mapType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
        FieldType keyType1 = new FieldType(false, new ArrowType.Utf8(), null, null);
        Map<String, String> idMetadata = null;/*new HashMap<>();
        idMetadata.put("min_values", Long.toString(arrowNodeOrWays.stream().mapToLong(ArrowRow::getId).min().orElse(Long.MIN_VALUE)));
        idMetadata.put("max_values", Long.toString(arrowNodeOrWays.stream().mapToLong(ArrowRow::getId).max().orElse(Long.MAX_VALUE)));*/
        Map<String, String> h33Metadata = null;/*new HashMap<>();
        h33Metadata.put("min_values", Integer.toString(arrowNodeOrWays.stream().mapToInt(ArrowRow::getH33).min().orElse(Short.MIN_VALUE)));
        h33Metadata.put("max_values", Integer.toString(arrowNodeOrWays.stream().mapToInt(ArrowRow::getH33).max().orElse(Short.MAX_VALUE)));*/
        Map<String, String> h38Metadata = null;/*new HashMap<>();
        h38Metadata.put("min_values", Integer.toString(arrowNodeOrWays.stream().mapToInt(ArrowRow::getH38).min().orElse(Integer.MIN_VALUE)));
        h38Metadata.put("max_values", Integer.toString(arrowNodeOrWays.stream().mapToInt(ArrowRow::getH38).max().orElse(Integer.MAX_VALUE)));*/
        FieldType idFieldType = new FieldType(false, new ArrowType.Int(64, true), null, idMetadata);
        FieldType h33FieldType = new FieldType(false, new ArrowType.Int(16, true),null, h33Metadata);
        FieldType h38FieldType = new FieldType(false, new ArrowType.Int(32, true),null, h38Metadata);
        List<Field> schemaFields = new ArrayList<>(Arrays.asList(
                new Field("id", idFieldType, null),
                new Field("h33", h33FieldType, null),
                new Field("h38", h38FieldType, null),
                new Field("latitude", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("longitude", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                , new Field("tags", FieldType.nullable(new ArrowType.Map(true)),
                        Collections.singletonList(new Field(MapVector.KEY_NAME, mapType,
                                Arrays.asList(new Field(MapVector.KEY_NAME, keyType1, null),
                                        new Field(MapVector.VALUE_NAME, keyType1, null)))))
        ));
        if(isWays){
            Map<String, String> h33CenterMetadata = null;/*new HashMap<>();
            h33CenterMetadata.put("min_values", Integer.toString(arrowNodeOrWays.stream().mapToInt(ArrowRow::getH33Center).min().orElse(Short.MIN_VALUE)));
            h33CenterMetadata.put("max_values", Integer.toString(arrowNodeOrWays.stream().mapToInt(ArrowRow::getH33Center).max().orElse(Short.MAX_VALUE)));*/
            FieldType h33CenterFieldType = new FieldType(false, new ArrowType.Int(16, true),null, h33CenterMetadata);
            schemaFields.addAll(Arrays.asList(
                    new Field("pointIdxs", FieldType.notNullable(new ArrowType.List()), Collections.singletonList(new Field("point", FieldType.notNullable(new ArrowType.Int(64, true)), null)))
                    , new Field("h33Center", h33CenterFieldType, null)
                    , new Field("closed", FieldType.notNullable(new ArrowType.Bool()), null)
                    , new Field("building", FieldType.notNullable(new ArrowType.Bool()), null)
                    , new Field("highway", FieldType.notNullable(new ArrowType.Bool()), null)
                    , new Field("scale", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null)
                    , new Field("lineStringWkb", FieldType.notNullable(new ArrowType.Binary()), null)
                    , new Field("h38Indexes", FieldType.nullable(new ArrowType.List()), Collections.singletonList(new Field("h38Idx", FieldType.notNullable(new ArrowType.Int(32, true)), null)))
                    , new Field("bboxMinX", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                    , new Field("bboxMaxX", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                    , new Field("bboxMinY", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                    , new Field("bboxMaxY", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                ));
        }
        return new Schema(schemaFields);
    }

    public static Schema getRelationSchema() {
        FieldType mapType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
        FieldType keyType1 = new FieldType(false, new ArrowType.Utf8(), null, null);
        Map<String, String> idMetadata = null;/*new HashMap<>();
idMetadata.put("min_values", Long.toString(arrowRelations.stream().mapToLong(ArrowRow::getId).min().orElse(Long.MIN_VALUE)));
idMetadata.put("max_values", Long.toString(arrowRelations.stream().mapToLong(ArrowRow::getId).max().orElse(Long.MAX_VALUE)));*/
        FieldType idFieldType = new FieldType(false, new ArrowType.Int(64, true), null, idMetadata);
        List<Field> schemaFields = new ArrayList<>(Arrays.asList(
                new Field("id", idFieldType, null),
                new Field("tags", FieldType.nullable(new ArrowType.Map(true)),
                        Collections.singletonList(new Field(MapVector.KEY_NAME, mapType,
                                Arrays.asList(new Field(MapVector.KEY_NAME, keyType1, null),
                                        new Field(MapVector.VALUE_NAME, keyType1, null)))))
                ,new Field("memberId", FieldType.notNullable(new ArrowType.List()), Collections.singletonList(
                        new Field("memberId", FieldType.notNullable(new ArrowType.Int(64, true)), null)
                ))
                ,new Field("memberType", FieldType.notNullable(new ArrowType.List()), Collections.singletonList(
                        new Field("memberType", FieldType.notNullable(new ArrowType.Int(8, true)), null)
                ))
                ,new Field("memberRole", FieldType.notNullable(new ArrowType.List()), Collections.singletonList(
                        new Field("memberRole", FieldType.notNullable(new ArrowType.Utf8()), null)
                ))
        ));
        Schema schema = new Schema(schemaFields);
        return schema;
    }

    private static void writeTagsToArrow(BufferAllocator allocator, UnionMapWriter mapWriter, int idx, Map<String, String> tags) {
        mapWriter.setPosition(idx);
        if(tags !=null && !tags.isEmpty()){
            mapWriter.startMap();
            tags.forEach((key, value) -> {
                if( value.length()==0){
                    value="-";
                }
                if(key.length()==0){
                    key="-";
                }
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                try (
                        ArrowBuf keyBuf = allocator.buffer(keyBytes.length);
                        ArrowBuf valueBuf = allocator.buffer(valueBytes.length);
                     ){
                    mapWriter.startEntry();
                    keyBuf.writeBytes(keyBytes);
                    valueBuf.writeBytes(valueBytes);
                    mapWriter.key().varChar().writeVarChar(0,keyBytes.length, keyBuf);
                    mapWriter.value().varChar().writeVarChar(0,valueBytes.length, valueBuf);
                    mapWriter.endEntry();
                }
            });
            mapWriter.endMap();
        } else {
            mapWriter.writeNull();
        }
    }

    private static void savePartitioningScripts(File resultDirectory, int scriptCount,
                                                double thresholdPercentFromMaxPartition,
                                                List<BlockStat> blockStatistics, boolean storeColumnar) {
        List<TableStat> tableStat = blockStatistics.stream().map(statItem -> {
            if (statItem.getRelationCount() > 0) {
                return null;
            }
            if (statItem.getNodeCount() > 0 && statItem.getWayCount() > 0) {
                throw new RuntimeException("Invalid block - mixed content nodes and ways");
            }
            if (statItem.getNodeCount() > 0) {
                if(statItem.getNodeStat()==null){
                    return null;
                }
                return statItem.getNodeStat().values().stream().map(stat -> new TableStat(StatType.N, statItem.getId(), stat)).collect(toList());
            }
            if (statItem.getWayCount() > 0) {
                if(statItem.getWayStat()==null){
                    return null;
                }
                return statItem.getWayStat().values().stream().map(stat -> new TableStat(StatType.W, statItem.getId(), stat)).collect(toList());
            }
            throw new RuntimeException();
        }).filter(Objects::nonNull).flatMap(Collection::stream).collect(toList());


        Map<Short, Long> waysSizeStat = tableStat.stream().filter(tableStat1 -> tableStat1.getType() == StatType.W).collect(Collectors.groupingBy(TableStat::getH33, TreeMap::new, Collectors.summingLong(TableStat::getSize)));
        Map<Short, Long> nodesSizeStat = tableStat.stream().filter(tableStat1 -> tableStat1.getType() == StatType.N).collect(Collectors.groupingBy(TableStat::getH33, TreeMap::new, Collectors.summingLong(TableStat::getSize)));


        List<Partition> partitionsWay = PartitionSplitter.distributeH33ByPartitionsForWays(waysSizeStat.entrySet().stream().filter(entry -> !entry.getKey().equals(Short.MAX_VALUE)).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue)), thresholdPercentFromMaxPartition);
        List<Partition> partitionsNode=
                PartitionSplitter.distributeH33ByPartitionsForNodes(partitionsWay, nodesSizeStat.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue)));
        System.out.println(partitionsWay.size()+" "+(partitionsWay.stream().map(Partition::getSerializedSize).min(Comparator.comparingLong(Long::longValue)).get()/1000000)+" "+(partitionsWay.stream().map(Partition::getSerializedSize).collect(Collectors.averagingLong(Long::longValue))/1000000));
        PartitionSplitter.createNodesScript(resultDirectory, scriptCount, partitionsNode, storeColumnar);
        PartitionSplitter.createWaysScript(resultDirectory, scriptCount, partitionsWay, storeColumnar);
        PartitionSplitter.createMultipolygonScript(resultDirectory, partitionsWay, storeColumnar);
    }

    private static void saveStatistics(File resultDirectory, PbfStatistics statistics) {
        File currentBlockTypeDir = new File(resultDirectory, OsmPbfTransformation.IMPORT_RELATED_METADATA_DIR);
        savePbfStatistics(statistics, currentBlockTypeDir);
        savePbfBlockStatistic(statistics, currentBlockTypeDir);
        savePbfBlockDataByHashStatistics(statistics, currentBlockTypeDir);
        /*
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(new File(resultDirectory, "statistics.obj")))){
            objectOutputStream.writeObject(statistics);
        }*/
    }

    private static String resultDirectoryNameFromSource(File inputDirectory) {
        return inputDirectory.getName().replace("_blocks", "");
    }

    private static void copyOsmiumSettings(File resultDir) throws IOException {
        File staticDir = new File(resultDir, STATIC_DIR);
        copyResource("/osmium_export.json",new File(staticDir,"osmium_export.json"));
    }

    private static void copyResources(File resultDir, boolean columnarStorage) throws IOException {
        File staticDir = new File(resultDir, STATIC_DIR);
        copyResource("/h3_poly.tsv.gz",new File(staticDir,"h3_poly.tsv.gz"));
        copyResourceWithSubstitute("/database_init.sql",
                new File(staticDir,"database_init.sql"),
                columnarStorage? "CREATE EXTENSION citus;":"");
        copyResourceWithSubstitute("/multipolygon.sql",new File(staticDir,"multipolygon.sql"),
                columnarStorage? "USING COLUMNAR":"");
        copyResourceWithSubstitute("/database_after_init.sql",
                new File(staticDir,"database_after_init.sql"),
                columnarStorage? "SELECT alter_table_set_access_method('ways_32767', 'columnar');":"");
    }

    private static ExecutorService getExecutorService(int workers) {
        ExecutorService executorService;
        if (workers > 0) {
            executorService = Executors.newFixedThreadPool(workers);
        } else {
            executorService = MoreExecutors.newDirectExecutorService();
        }
        return executorService;
    }

    private static File prepareResultDirectories(File resultDir, boolean savePostgresqlTsv, boolean saveArrow) throws IOException {
        resultDir.mkdir();
        if(savePostgresqlTsv){
            File nodesDir = new File(resultDir, NODES_DIR);
            if(nodesDir.exists() && nodesDir.list().length>0){
                throw new IllegalArgumentException("Nodes directory contains files "+ nodesDir.getAbsolutePath());
            }
            File waysDir = new File(resultDir, WAYS_DIR);
            if(waysDir.exists() && waysDir.list().length>0){
                throw new IllegalArgumentException("Ways directory contains files "+waysDir.getAbsolutePath());
            }
            File relationsDir = new File(resultDir, RELATIONS_DIR);
            if(relationsDir.exists() && relationsDir.list().length>0){
                throw new IllegalArgumentException("Relations directory contains files "+relationsDir.getAbsolutePath());
            }
            File multipolygonDir = new File(resultDir, MULTIPOLYGON_DIR);
            if(multipolygonDir.exists() && multipolygonDir.list().length>0){
                throw new IllegalArgumentException("Multipolygon directory contains files "+multipolygonDir.getAbsolutePath());
            }
            nodesDir.mkdir();
            waysDir.mkdir();
            relationsDir.mkdir();
            new File(resultDir, SQL_DIR).mkdirs();
            multipolygonDir.mkdirs();
        }
        new File(resultDir, IMPORT_RELATED_METADATA_DIR).mkdirs();
        if(saveArrow){
            File arrowDir = new File(resultDir, ARROW_DIR);
            arrowDir.mkdir();
            new File(arrowDir,NODES_DIR).mkdir();
            new File(arrowDir,WAYS_DIR).mkdir();
            new File(arrowDir,RELATIONS_DIR).mkdir();
        }
        File staticDir = new File(resultDir, STATIC_DIR);
        staticDir.mkdir();
        return resultDir;
    }

    public static void copyResource(String classpath,File destination) throws IOException{
        try (OutputStream outputStream = new FileOutputStream(destination);
             InputStream resourceStream = OsmPbfTransformation.class.getResourceAsStream(classpath)){
            if(resourceStream==null){
                throw new IllegalArgumentException("Resource not found: "+classpath);
            }
            IOUtils.copy(resourceStream, outputStream);
        }
    }

    public static void copyResourceWithSubstitute(String classpath,File destination, String substituteValue) throws IOException{
        try (OutputStream outputStream = new FileOutputStream(destination);
             InputStream resourceStream = OsmPbfTransformation.class.getResourceAsStream(classpath)){
            if(resourceStream==null){
                throw new IllegalArgumentException("Resource not found: "+classpath);
            }
            String resourceContent = IOUtils.toString(resourceStream, StandardCharsets.UTF_8).replace("${substitute}", substituteValue);
            IOUtils.write(resourceContent, outputStream, StandardCharsets.UTF_8);
        }
    }

    private static void prepareNodeData(Map<Short, StringBuilder> csvResultPerH33, BinaryWriter binaryWriter,
                                        ArrayList<ArrowNodeOrWay> arrowNodeOrWays,
                                        Map<Short, Stat> nodeStat,
                                        org.openstreetmap.osmosis.core.domain.v0_6.Node entity, H3Core h3Core,
                                        boolean collectOnlyStat, boolean saveArrow, boolean savePostgresqlTsv) {
        long id = entity.getId();
        final double latitude = entity.getLatitude();
        final double longitude = entity.getLongitude();
        short h33 = CompactH3.serialize3(h3Core.latLngToCell(latitude, longitude, 3));
        Stat stat = nodeStat.computeIfAbsent(h33, Stat::new);
        stat.incrementCount();
        stat.updateIdStat(id);
        stat.updateLastModified(entity.getTimestamp().getTime());
        stat.incrementSize(2 + 3 * 8);
        updateSizeStatistictsByTags(entity, stat);

        if(!collectOnlyStat) {
            int h38 = CompactH3.serialize8(h3Core.latLngToCell(latitude, longitude, 8));
            if(saveArrow) {
                arrowNodeOrWays.add(new ArrowNodeOrWay(id, h33, h38, latitude, longitude, entity.getTags()));
            }
            if(savePostgresqlTsv){
                StringBuilder resultBuilder = csvResultPerH33.computeIfAbsent(h33, key -> new StringBuilder());
                Serializer.serializeNode(resultBuilder, binaryWriter,
                        h33, h38, id, latitude, longitude, entity.getTags());
            }
        }
    }

    private static void prepareWayData(GeometryFactory geometryFactory,
                                       Map<Short, StringBuilder> csvResultPerH33,
                                       BinaryWriter binaryWriter, WKBWriter wkbWriter,
                                       List<ArrowNodeOrWay> arrowNodeOrWays,
                                       Map<Short, Stat> wayStat,
                                       org.openstreetmap.osmosis.core.domain.v0_6.Way entity, H3Core h3Core,
                                       boolean scaleApproximation, boolean collectOnlyStat, boolean skipBuildings,
                                       CoordinateReferenceSystem coordinateReferenceSystem, boolean saveArrow, boolean savePostgresqlTsv) {
        if(skipBuildings) {
            if(entity.getTags().stream().anyMatch(tag -> "building".equals(tag.getKey()))){
                return;
            }
        }
        long id = entity.getId();
        List<WayNode> wayNodes = entity.getWayNodes();
        Point[] points = new Point[wayNodes.size()];
        Coordinate[] coordinates = new Coordinate[wayNodes.size()];
        long[] pointsIdx = new long[wayNodes.size()];
        short[] h3Idxs = new short[wayNodes.size()];
        for (int pointIdx = 0; pointIdx < wayNodes.size(); pointIdx++) {
            WayNode wayNode = wayNodes.get(pointIdx);
            double latitude = wayNode.getLatitude();
            double longitude = wayNode.getLongitude();
            points[pointIdx] = new Point(longitude, latitude);
            coordinates[pointIdx] = new CoordinateXY(longitude, latitude);
            pointsIdx[pointIdx] = wayNode.getNodeId();
            final long h3Cell3 = h3Core.latLngToCell(latitude, longitude, 3);
            h3Idxs[pointIdx] = CompactH3.serialize3(h3Cell3);
        }
        boolean isInOneH33 = isInSingleH3Segment(h3Idxs);
        short h33= isInOneH33? h3Idxs[0] : Short.MAX_VALUE;
        Stat stat = wayStat.computeIfAbsent(h33, Stat::new);
        stat.incrementCount();
        stat.updateIdStat(id);
        stat.updateLastModified(entity.getTimestamp().getTime());
        stat.incrementSize(2 + 8);
        stat.incrementSize(wayNodes.size() * 16);
        updateSizeStatistictsByTags(entity, stat);

        if(!collectOnlyStat) {
            constructAndSaveWay(h3Core, geometryFactory, csvResultPerH33, binaryWriter, wkbWriter, arrowNodeOrWays,
                    entity, scaleApproximation,
                    id, points, coordinates, pointsIdx, h33, wayNodes, coordinateReferenceSystem, saveArrow, savePostgresqlTsv);
        }
    }

    private static boolean isInSingleH3Segment(short[] h3Idxs) {
        boolean oneH3 = true;
        for (int i = 1; i < h3Idxs.length; i++) {
            if(h3Idxs[i]!= h3Idxs[0]){
                oneH3=false;
                break;
            }
        }
        return oneH3;
    }

    private static void constructAndSaveWay(H3Core h3Core,
                                            GeometryFactory geometryFactory,
                                            Map<Short, StringBuilder> csvResultPerH33,
                                            BinaryWriter binaryWriter, WKBWriter wkbWriter,
                                            List<ArrowNodeOrWay> arrowNodeOrWays,
                                            org.openstreetmap.osmosis.core.domain.v0_6.Way entity,
                                            boolean scaleApproximation, long id, Point[] points, Coordinate[] coordinates,
                                            long[] pointsIdx, short h33,
                                            List<WayNode> wayNodes, CoordinateReferenceSystem coordinateReferenceSystem, boolean saveArrow, boolean savePostgresqlTsv) {
        LineString lineString = new LineString(points);
        lineString.setSrid(Serializer.SRID);

        Geometry currentWayGeometry;
        if (coordinates.length > 1){
            currentWayGeometry = geometryFactory.createLineString(coordinates);
        } else {
            currentWayGeometry = geometryFactory.createPoint(coordinates[0]);
        }

        boolean closed = ST_IsClosed.isClosed(currentWayGeometry);
        boolean nonValid = !(currentWayGeometry.isValid()&&coordinates.length>1);

        Envelope envelopeInternal = currentWayGeometry.getEnvelopeInternal();

        final double minX = envelopeInternal.getMinX();
        final double minY = envelopeInternal.getMinY();
        final double maxX = envelopeInternal.getMaxX() + Double.MIN_VALUE;
        final double maxY = envelopeInternal.getMaxY() + Double.MIN_VALUE;
        Polygon bboxGeometry = new Polygon(new LinearRing[] {
                new LinearRing(new Point[]{
                        new Point(minX, minY),
                        new Point(minX, maxY),
                        new Point(maxX, maxY),
                        new Point(maxX, minY),
                        new Point(minX, minY)})
        });
        bboxGeometry.srid = Serializer.SRID;

        float scaleDim;
        Point centre;
        double latitude;
        double longitude;
        if(!scaleApproximation){
            MinimumBoundingCircle boundingCircle = new MinimumBoundingCircle(currentWayGeometry);
            scaleDim = (float) boundingCircle.getRadius();
            Coordinate coordinate = boundingCircle.getCentre();

            if(coordinate!=null){
                latitude = coordinate.y;
                longitude = boundingCircle.getCentre().x;
            } else {
                org.locationtech.jts.geom.Point centroid = currentWayGeometry.getCentroid();
                longitude = centroid.getX();
                latitude = centroid.getY();
                scaleDim = (float) envelopeInternal.maxExtent();
            }
            centre = Serializer.getPoint(latitude, longitude);

        } else {
            org.locationtech.jts.geom.Point centroid = currentWayGeometry.getCentroid();
            longitude = centroid.getX();
            latitude = centroid.getY();
            centre = Serializer.getPoint(latitude, longitude);
            scaleDim = (float) envelopeInternal.maxExtent();
        }
        long centerSrcIndex = h3Core.latLngToCell(latitude, longitude, 8);
        int h38 = CompactH3.serialize8(centerSrcIndex);

        double distanceMeter=Double.MIN_VALUE;
        try {
            distanceMeter = JTS.orthodromicDistance(new CoordinateXY(longitude, latitude),
                    new CoordinateXY(longitude+scaleDim, latitude), coordinateReferenceSystem);
        } catch (TransformException e) {
            //ignore it
        }
        if(distanceMeter!=Double.MIN_VALUE){
            scaleDim = (float) distanceMeter; //save distance in meters
        }


        boolean isOneH38=true;

        long[] h38SourceIdxs = new long[wayNodes.size()];
        for (int pointIndex = 0, wayNodesSize = wayNodes.size(); pointIndex < wayNodesSize; pointIndex++) {
            WayNode wayNode = wayNodes.get(pointIndex);
            long currentSrcIndex = h3Core.latLngToCell(wayNode.getLatitude(), wayNode.getLongitude(), 8);
            h38SourceIdxs[pointIndex]=currentSrcIndex;
            if(centerSrcIndex!=currentSrcIndex){
                isOneH38 = false;
            }
        }

        Set<Integer> wayIntersectionH38Indexes = null;
        if(!isOneH38 && distanceMeter < 3000.0){
            wayIntersectionH38Indexes = Arrays.stream(h38SourceIdxs).mapToObj(CompactH3::serialize8).collect(Collectors.toCollection(TreeSet::new));
            for (int i = 0; i < h38SourceIdxs.length - 1; i++) {
                wayIntersectionH38Indexes.addAll(h3Core.gridPathCells(h38SourceIdxs[i],h38SourceIdxs[i+1]).stream().map(CompactH3::serialize8).collect(Collectors.toSet()));
            }
            if(closed){
                wayIntersectionH38Indexes.addAll(h3Core.gridPathCells(h38SourceIdxs[0],h38SourceIdxs[h38SourceIdxs.length-1]).stream().map(CompactH3::serialize8).collect(Collectors.toSet()));
                wayIntersectionH38Indexes.addAll(h3Core.polygonToCells(wayNodes.stream().map(wayNode -> new LatLng(wayNode.getLatitude(), wayNode.getLongitude())).collect(toList()), null, 8).stream().map(CompactH3::serialize8).collect(Collectors.toSet()));
            }
            if(wayIntersectionH38Indexes.size() == 1){
                if(wayIntersectionH38Indexes.iterator().next().equals(h38)){
                    wayIntersectionH38Indexes = null;// save only center h38 as it duplicate it
                }
            }
        }
        if(saveArrow){
            ArrowNodeOrWay arrowNodeOrWay = new ArrowNodeOrWay(id, h33, h38, latitude, longitude, entity.getTags());
            arrowNodeOrWays.add(arrowNodeOrWay);
            arrowNodeOrWay.setBboxMinX(minX);
            arrowNodeOrWay.setBboxMinY(minY);
            arrowNodeOrWay.setBboxMaxX(maxX);
            arrowNodeOrWay.setBboxMaxY(maxY);
            arrowNodeOrWay.setClosed(closed);
            arrowNodeOrWay.setPointIdxs(pointsIdx);
            arrowNodeOrWay.setScaleDim(scaleDim);
            arrowNodeOrWay.setH38Indexes(wayIntersectionH38Indexes!=null&&!wayIntersectionH38Indexes.isEmpty()?wayIntersectionH38Indexes.stream().mapToInt(Integer::intValue).toArray():null);
            arrowNodeOrWay.setH33Center(CompactH3.serialize3(h3Core.latLngToCell(latitude, longitude, 3)));
            arrowNodeOrWay.setLineStringWkb(wkbWriter.write(currentWayGeometry));
        }
        if(savePostgresqlTsv){
            StringBuilder resultBuilder = csvResultPerH33.computeIfAbsent(h33, h33Key -> new StringBuilder());
            Serializer.serializeWay(resultBuilder, binaryWriter, closed, nonValid,
                    h33, h38, id, pointsIdx, wayIntersectionH38Indexes,  centre,
                    scaleDim, bboxGeometry, lineString, entity.getTags());
        }
    }

    private static void updateSizeStatistictsByTags(Entity entity, Stat stat) {
        for(Tag tag : entity.getTags()){
            stat.incrementSize(tag.getKey().length());
            stat.incrementSize(1);
            stat.incrementSize(tag.getValue().length());
        }
    }

    private static File blockResultDirectory(long nodeRecords, long wayRecords, long relationCount, File resultDir) {
        String typeDir=null;
        if(nodeRecords >0){
            typeDir = NODES_DIR;
        } else if(wayRecords >0){
            typeDir = WAYS_DIR;
        } else if(relationCount >0){
            typeDir = RELATIONS_DIR;
        } else {
            throw new IllegalArgumentException("unknown block type");
        }
        return new File(resultDir, typeDir);
    }

    private static void saveDataOnlyInOneThread(Map<Short, StringBuilder> csvResultPerH33, long nodeRecords, long wayRecords, BlockStat blockStatistic, long relationCount, AtomicInteger currentBlockToSave, Long blockNumber, File resultDir, ExecutorService saveExecutorService) {
        long waitSaveTime = System.currentTimeMillis();
        while (true){ //waiting to save/append data by block number order
            if(currentBlockToSave.get()== blockNumber){
                blockStatistic.setWaitingForSaveTime(System.currentTimeMillis() - waitSaveTime);
                long startSaveTime = System.currentTimeMillis();
                try {
                    File currentBlockTypeDir =
                            blockResultDirectory(nodeRecords, wayRecords, relationCount, resultDir);
                    saveBlockData(csvResultPerH33, currentBlockTypeDir, saveExecutorService);
                } finally {
                    blockStatistic.setSaveTime(blockStatistic.getSaveTime()+(System.currentTimeMillis() - startSaveTime));
                    currentBlockToSave.incrementAndGet();
                }
                return;
            }
            Thread.yield();
        }
    }

    private static void saveBlockData(Map<Short, StringBuilder> csvResultPerH33, File currentBlockTypeDir, ExecutorService saveExecutorService) {
        List<Future<?>> saveFutures = new ArrayList<>();
        for(Map.Entry<Short,StringBuilder> result: csvResultPerH33.entrySet()){
            saveFutures.add(saveExecutorService.submit(() -> {
                try (OutputStream dataExport = //new NullOutputStream()
                             new FileOutputStream(new File(currentBlockTypeDir, String.format("%05d.tsv",result.getKey())), true)
                ) {
                    IOUtils.write(result.getValue(), dataExport, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        long savedCount = saveFutures.stream().map(future -> {
            try {
                return future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }).filter(Objects::nonNull).count();
    }

    private static void savePbfBlockDataByHashStatistics(PbfStatistics pbfStatistics, File currentBlockTypeDir) {
        StringBuilder csvString = new StringBuilder();
        for(BlockStat blockStatistics : pbfStatistics.getBlockStatistics()){
            Serializer.serializeBlockContent(csvString, blockStatistics);
            if(csvString.length()>0) {
                saveStatistics(currentBlockTypeDir, csvString, "osm_file_block_content.tsv");
            }
            csvString.setLength(0);
        }
    }

    private static void savePbfBlockStatistic(PbfStatistics pbfStatistics, File currentBlockTypeDir) {
        StringBuilder csvString = new StringBuilder();
        Serializer.serializeBlockStat(csvString, pbfStatistics.getBlockStatistics());
        saveStatistics(currentBlockTypeDir, csvString, "osm_file_block.tsv");
    }

    private static void savePbfStatistics(PbfStatistics pbfStatistics, File currentBlockTypeDir) {
        StringBuilder csvString = new StringBuilder();
        Serializer.serializePbfStat(csvString, pbfStatistics);
        saveStatistics(currentBlockTypeDir, csvString, "osm_file_statistics.tsv");
    }

    private static void saveStatistics(File currentBlockTypeDir, StringBuilder csvString, String statName) {
        try (OutputStream dataExport =
                     new FileOutputStream(new File(currentBlockTypeDir, statName), true)) {
            IOUtils.write(csvString, dataExport, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
