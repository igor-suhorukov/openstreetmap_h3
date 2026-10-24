package com.github.isuhorukov.osm.pgsnapshot.util;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.google.common.collect.Lists;
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

    private PartitionSplitter() {}

    public static int createNodesScript(File resultDirectory, int scriptCount, List<Partition> partitions, boolean storeColumnar) {
        return createImportScript(resultDirectory, scriptCount, partitions, storeColumnar,
                "nodes", "h3_3,h3_8,id,geom,tags");
    }

    public static int createWaysScript(File resultDirectory, int scriptCount, List<Partition> partitions, boolean storeColumnar) {
        return createImportScript(resultDirectory, scriptCount, partitions, storeColumnar,
                "ways", "h3_3,h3_8,id,closed,building,highway,scale,centre,bbox,linestring,points,h3_8_regions,tags");
    }

    private static int createImportScript(File resultDirectory, int scriptCount, List<Partition> partitions,
                                          boolean storeColumnar, String entityName, String copyColumns) {
        final List<List<Partition>> partition = Lists.partition(partitions, partitions.size() / scriptCount + 1);
        for (int partIdx = 0; partIdx < partition.size(); partIdx++) {
            List<Partition> part = partition.get(partIdx);
            String scriptName = String.format("sql/%s_import_%03d.sql", entityName, partIdx);
            try (FileOutputStream scriptOs = new FileOutputStream(new File(resultDirectory, scriptName))) {
                appendScriptHead(scriptName, scriptOs);
                scriptOs.write("BEGIN;\n".getBytes(StandardCharsets.UTF_8));
                for (Partition currentPart : part) {
                    scriptOs.write(String.format("CREATE TABLE \"%s_%03d\" (like %s) %s;%n",
                            entityName, currentPart.getId(), entityName,
                            getColumnarString(storeColumnar)).getBytes(StandardCharsets.UTF_8));
                    List<Short> h33RegionsInside = currentPart.getH33RegionsInside();
                    Collections.sort(h33RegionsInside);
                    for (Short h33Region : h33RegionsInside) {
                        scriptOs.write(String.format("COPY \"%s_%03d\"(%s) FROM '/input/%s/%05d.tsv' DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV;%n",
                                entityName, currentPart.getId(), copyColumns, entityName, h33Region).getBytes(StandardCharsets.UTF_8));
                    }
                }
                for (Partition currentPart : part) {
                    scriptOs.write(String.format("ALTER TABLE  %s ATTACH PARTITION  \"%s_%03d\" FOR VALUES FROM (%s) TO (%s);%n",
                            entityName, entityName, currentPart.getId(),
                            currentPart.getMinRange(), currentPart.getMaxRange()).getBytes(StandardCharsets.UTF_8));
                }
                scriptOs.write("COMMIT;".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return partition.size();
    }

    private static String getColumnarString(boolean storeColumnar) {
        return storeColumnar ? USING_COLUMNAR : "";
    }

    public static void appendScriptHead(String scriptName, FileOutputStream waysScriptOs) throws IOException {
        waysScriptOs.write(("SET application_name = 'psql "+ scriptName +"';\n").getBytes(StandardCharsets.UTF_8));
        waysScriptOs.write("\\timing on\n".getBytes(StandardCharsets.UTF_8));
    }

    public static void createMultipolygonScript(File resultDirectory, List<Partition> partitions, boolean storeColumnar) {
        try (FileOutputStream waysScriptOs = new FileOutputStream(new File(resultDirectory, "static/multipolygon_tables.sql"))) {
            for (Partition currentPart : partitions) {
                waysScriptOs.write(String.format("CREATE TABLE  \"multipolygon_%03d\" PARTITION OF multipolygon FOR VALUES FROM (%s) TO (%s) %s;%n",
                        currentPart.getId(),currentPart.getMinRange(),currentPart.getMaxRange(),
                        getColumnarString(storeColumnar)).getBytes(StandardCharsets.UTF_8)
                );
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
        List<Partition> partitionsNode = new ArrayList<>();
        for(Partition partition: partitionsWay){
            List<Map.Entry<Short, Long>> relatedNodes = nodesSizeStat.entrySet().stream().filter(entry -> entry.getKey() >= partition.getMinRange() && entry.getKey() < partition.getMaxRange()).collect(Collectors.toList());
            if(relatedNodes.isEmpty()){
                continue;
            }
            Partition newPartition = new Partition(partition.getId(), partition.getMinRange(), partition.getMaxRange(), relatedNodes.stream().mapToLong(Map.Entry::getValue).sum());
            newPartition.setH33RegionsInside(relatedNodes.stream().map(Map.Entry::getKey).sorted().collect(Collectors.toList()));
            partitionsNode.add(newPartition);
        }
        return partitionsNode;
    }
}
