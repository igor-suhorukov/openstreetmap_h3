package com.github.isuhorukov.osm.pgsnapshot.util;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class PartitionSplitter {

    private static final Logger log = LoggerFactory.getLogger(PartitionSplitter.class);

    public static final int LOWER_H3_3_BOUND = -32768;
    public static final int HIGH_H3_3_BOUND = 32695;
    public static final String USING_COLUMNAR = "USING COLUMNAR";
    public static final String NODES = "nodes";
    public static final String WAYS = "ways";
    public static final String NODE_COLUMNS = "h3_3,h3_8,id,geom,tags";
    public static final String WAY_COLUMNS = "h3_3,h3_8,id,closed,building,highway,scale,centre,bbox,linestring,points,h3_8_regions,tags";
    public static final String RELATION_COLUMNS = "id,tags";
    public static final String VERSION_COLUMN = ",version";
    public static final String WAY_VERSION_COLUMNS = ",version,geom_hash";
    public static final String MULTIPOLYGON_TABLES_SQL = "multipolygon_tables.sql";

    private PartitionSplitter() {}

    public static String nodeColumns(boolean updatable) {
        return updatable ? NODE_COLUMNS + VERSION_COLUMN : NODE_COLUMNS;
    }

    public static String wayColumns(boolean updatable) {
        return updatable ? WAY_COLUMNS + WAY_VERSION_COLUMNS : WAY_COLUMNS;
    }

    public static String relationColumns(boolean updatable) {
        return updatable ? RELATION_COLUMNS + VERSION_COLUMN : RELATION_COLUMNS;
    }

    public static int createNodesScript(File resultDirectory, int scriptCount, List<Partition> partitions, boolean storeColumnar) {
        return createNodesScript(resultDirectory, scriptCount, partitions, storeColumnar, false);
    }

    public static int createNodesScript(File resultDirectory, int scriptCount, List<Partition> partitions,
                                        boolean storeColumnar, boolean updatable) {
        return createImportScript(resultDirectory, scriptCount, partitions, storeColumnar, NODES, nodeColumns(updatable));
    }

    public static int createWaysScript(File resultDirectory, int scriptCount, List<Partition> partitions, boolean storeColumnar) {
        return createWaysScript(resultDirectory, scriptCount, partitions, storeColumnar, false);
    }

    public static int createWaysScript(File resultDirectory, int scriptCount, List<Partition> partitions,
                                       boolean storeColumnar, boolean updatable) {
        return createImportScript(resultDirectory, scriptCount, partitions, storeColumnar, WAYS, wayColumns(updatable));
    }

    private static int createImportScript(File resultDirectory, int scriptCount, List<Partition> partitions,
                                          boolean storeColumnar, String entityName, String copyColumns) {
        final List<List<Partition>> partition = balanceBySize(partitions, scriptCount);
        for (int partIdx = 0; partIdx < partition.size(); partIdx++) {
            List<Partition> part = partition.get(partIdx);
            String scriptName = String.format("sql/%s_import_%03d.sql", entityName, partIdx);
            try (FileOutputStream scriptOs = new FileOutputStream(new File(resultDirectory, scriptName))) {
                appendScriptHead(scriptName, scriptOs);
                scriptOs.write("BEGIN;\n".getBytes(StandardCharsets.UTF_8));
                for (Partition currentPart : part) {
                    writeLine(scriptOs, createPartitionTableSql(entityName, currentPart.getId(), storeColumnar));
                    List<Short> h33RegionsInside = currentPart.getH33RegionsInside();
                    Collections.sort(h33RegionsInside);
                    for (Short h33Region : h33RegionsInside) {
                        writeLine(scriptOs, copyPartitionSql(entityName, currentPart.getId(), copyColumns,
                                String.format("'/input/%s/%05d.tsv'", entityName, h33Region), storeColumnar));
                    }
                    writeLine(scriptOs, analyzePartitionSql(entityName, currentPart.getId()));
                }
                for (Partition currentPart : part) {
                    writeLine(scriptOs, attachPartitionSql(entityName, currentPart));
                }
                scriptOs.write("COMMIT;".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return partition.size();
    }

    private static void writeLine(FileOutputStream scriptOs, String sql) throws IOException {
        scriptOs.write(String.format("%s%n", sql).getBytes(StandardCharsets.UTF_8));
    }

    public static String partitionTableName(String entityName, int partitionId) {
        return String.format("%s_%03d", entityName, partitionId);
    }

    public static String createPartitionTableSql(String entityName, int partitionId, boolean storeColumnar) {
        return String.format("CREATE TABLE \"%s\" (like %s) %s;", partitionTableName(entityName, partitionId),
                entityName, getColumnarString(storeColumnar));
    }

    public static String copyPartitionSql(String entityName, int partitionId, String copyColumns, String source,
                                          boolean storeColumnar) {
        return String.format("COPY \"%s\"(%s) FROM %s DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV%s;",
                partitionTableName(entityName, partitionId), copyColumns, source, storeColumnar ? "" : " FREEZE");
    }

    public static String analyzePartitionSql(String entityName, int partitionId) {
        return String.format("ANALYZE \"%s\";", partitionTableName(entityName, partitionId));
    }

    public static String attachPartitionSql(String entityName, Partition partition) {
        return String.format("ALTER TABLE  %s ATTACH PARTITION  \"%s\" FOR VALUES FROM (%s) TO (%s);",
                entityName, partitionTableName(entityName, partition.getId()),
                partition.getMinRange(), partition.getMaxRange());
    }

    public static List<List<Partition>> balanceBySize(List<Partition> partitions, int scriptCount) {
        int binCount = Math.min(Math.max(scriptCount, 1), partitions.size());
        List<List<Partition>> bins = new ArrayList<>(binCount);
        long[] binSizes = new long[binCount];
        for (int i = 0; i < binCount; i++) {
            bins.add(new ArrayList<>());
        }
        List<Partition> largestFirst = new ArrayList<>(partitions);
        largestFirst.sort(Comparator.comparingLong(Partition::getSerializedSize).reversed());
        for (Partition partition : largestFirst) {
            int smallestBin = 0;
            for (int i = 1; i < binCount; i++) {
                if (binSizes[i] < binSizes[smallestBin]) {
                    smallestBin = i;
                }
            }
            bins.get(smallestBin).add(partition);
            binSizes[smallestBin] += partition.getSerializedSize();
        }
        for (List<Partition> bin : bins) {
            bin.sort(Comparator.comparingInt(Partition::getId));
        }
        return bins;
    }

    private static String getColumnarString(boolean storeColumnar) {
        return storeColumnar ? USING_COLUMNAR : "";
    }

    public static void appendScriptHead(String scriptName, FileOutputStream waysScriptOs) throws IOException {
        waysScriptOs.write(("SET application_name = 'psql "+ scriptName +"';\n").getBytes(StandardCharsets.UTF_8));
        waysScriptOs.write("\\timing on\n".getBytes(StandardCharsets.UTF_8));
    }

    public static void createMultipolygonScript(File resultDirectory, List<Partition> partitions, boolean storeColumnar) {
        try (FileOutputStream waysScriptOs = new FileOutputStream(new File(resultDirectory, "static/" + MULTIPOLYGON_TABLES_SQL))) {
            waysScriptOs.write(multipolygonTablesSql(partitions, storeColumnar).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String multipolygonTablesSql(List<Partition> partitions, boolean storeColumnar) {
        StringBuilder sql = new StringBuilder();
        for (Partition currentPart : partitions) {
            sql.append(String.format("CREATE TABLE  \"multipolygon_%03d\" PARTITION OF multipolygon FOR VALUES FROM (%s) TO (%s) %s;%n",
                    currentPart.getId(), currentPart.getMinRange(), currentPart.getMaxRange(),
                    getColumnarString(storeColumnar)));
        }
        return sql.toString();
    }

    public static List<Partition> distributeH33ByPartitionsForWays(Map<Short, Long> waysSizeStat, double thresholdPercentFromMaxPartition){
        if(waysSizeStat==null || waysSizeStat.isEmpty()){
            throw new IllegalArgumentException("'Ways' statistics is absent");
        }
        Map<Short, Partition> h3RangeInH2 = null;
        Map<Short, Short> h33to2 = null;
        try {
            h3RangeInH2 = getH3RangeInH2();
            h33to2 = parseH33to2();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Map<Short, Short> finalH33to = h33to2;
        Map<Short, Long> sumByH2 = new TreeMap<>(waysSizeStat.entrySet().stream().map(entry -> new AbstractMap.SimpleImmutableEntry<>(finalH33to.get(entry.getKey()), entry.getValue())).collect(Collectors.groupingBy(Map.Entry::getKey,
                Collectors.summingLong(Map.Entry::getValue))));
        long maxPartitionSize = (long) (sumByH2.values().stream().max(Comparator.comparingLong(Long::longValue)).orElse(0L) * thresholdPercentFromMaxPartition);
        long currentSum=0;
        short lowerBound=LOWER_H3_3_BOUND;
        int partitionNumber=0;
//8103	8096	8102
        List<Partition> partitions = new ArrayList<>();
        for(Map.Entry<Short,Long> entry: sumByH2.entrySet()){
            Short h2 = entry.getKey();
            Partition nextRange = h3RangeInH2.get(h2);
            if(currentSum>0 && currentSum+entry.getValue()>=maxPartitionSize){
                short maxRangeOfInterval = nextRange.getMinRange();
                partitions.add(createPartition(waysSizeStat, currentSum, lowerBound, partitionNumber, maxRangeOfInterval));
                log.info("{}\t[{},{})\t{}\t{}", partitionNumber++, lowerBound, maxRangeOfInterval, currentSum, (maxRangeOfInterval - lowerBound));
                lowerBound=nextRange.getMinRange();
                currentSum=entry.getValue();
                continue;
            }
            currentSum+=entry.getValue();
        }
        if(currentSum!=0){
            short maxRangeOfInterval = HIGH_H3_3_BOUND;
            partitions.add(createPartition(waysSizeStat, currentSum, lowerBound, partitionNumber, maxRangeOfInterval));
            log.info("{}\t[{},{})\t{}\t{}", partitionNumber, lowerBound, maxRangeOfInterval, currentSum, (maxRangeOfInterval - lowerBound));
        }
        return partitions;
    }

    private static Partition createPartition(Map<Short, Long> waysSizeStat, long currentSum, short lowerBound, int partitionNumber, short maxRangeOfInterval) {
        Partition partition = new Partition(partitionNumber, lowerBound, maxRangeOfInterval, currentSum);
        partition.setH33RegionsInside(waysSizeStat.keySet().stream().filter(region -> region>= lowerBound && region< maxRangeOfInterval).collect(Collectors.toList()));
        return partition;
    }

    private static Map<Short, Partition> getH3RangeInH2() throws IOException {
        try (InputStreamReader inputStreamReader = new InputStreamReader(
                Objects.requireNonNull(PartitionSplitter.class.getResourceAsStream("/h3_2_ranges.tsv")))){
            return new BufferedReader(inputStreamReader).lines().map(line -> {
                String[] parts = line.split("\t");
                return new AbstractMap.SimpleImmutableEntry<>(Short.parseShort(parts[0]), new Partition(Short.parseShort(parts[1]), Short.parseShort(parts[2])));
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    private static Map<Short, Short> parseH33to2() throws IOException {
        try (InputStreamReader inputStreamReader = new InputStreamReader(
                Objects.requireNonNull(PartitionSplitter.class.getResourceAsStream("/h3_3_to_2.tsv")))) {
            return new TreeMap<>(new BufferedReader(inputStreamReader).lines().map(line -> {
                String[] parts = line.split("\t");
                return new AbstractMap.SimpleImmutableEntry<>(Short.parseShort(parts[0]), Short.parseShort(parts[1]));
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
    }

    public static List<Partition>  distributeH33ByPartitionsForNodes(List<Partition> partitionsWay, Map<Short, Long> nodesSizeStat) {
        return distributeH33ByPartitionsForNodes(partitionsWay, nodesSizeStat, false);
    }

    public static List<Partition> distributeH33ByPartitionsForNodes(List<Partition> partitionsWay,
                                                                    Map<Short, Long> nodesSizeStat,
                                                                    boolean keepEmptyPartitions) {
        List<Partition> partitionsNode = new ArrayList<>();
        for(Partition partition: partitionsWay){
            List<Map.Entry<Short, Long>> relatedNodes = nodesSizeStat.entrySet().stream().filter(entry -> entry.getKey() >= partition.getMinRange() && entry.getKey() < partition.getMaxRange()).collect(Collectors.toList());
            if(relatedNodes.isEmpty() && !keepEmptyPartitions){
                continue;
            }
            Partition newPartition = new Partition(partition.getId(), partition.getMinRange(), partition.getMaxRange(), relatedNodes.stream().mapToLong(Map.Entry::getValue).sum());
            newPartition.setH33RegionsInside(relatedNodes.stream().map(Map.Entry::getKey).sorted().collect(Collectors.toList()));
            partitionsNode.add(newPartition);
        }
        return partitionsNode;
    }
}
