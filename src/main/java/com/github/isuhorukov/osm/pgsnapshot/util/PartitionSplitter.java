package com.github.isuhorukov.osm.pgsnapshot.util;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.google.common.collect.Lists;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PartitionSplitter {

    public static final int LOWER_H3_3_BOUND = -32768;
    public static final int HIGH_H3_3_BOUND = 32695;
    public static final String USING_COLUMNAR = "USING COLUMNAR";

    public static int createNodesScript(File resultDirectory, int scriptCount, List<Partition> partitions, boolean storeColumnar) {
/*        final ArrayList<Short> waysPart = new ArrayList<>(partition_keys);
        waysPart.sort(Short::compareTo);*/
        final List<List<Partition>> partition = Lists.partition(partitions, partitions.size()/ scriptCount +1);
        for (int waysPartIdx = 0; waysPartIdx < partition.size(); waysPartIdx++) {
            List<Partition> part = partition.get(waysPartIdx);
            String scriptName = String.format("sql/nodes_import_%03d.sql", waysPartIdx);
            try (FileOutputStream waysScriptOs = new FileOutputStream(new File(resultDirectory, scriptName))){
                appendScriptHead(scriptName, waysScriptOs);
                waysScriptOs.write("BEGIN;\n".getBytes(StandardCharsets.UTF_8));
                for (Partition currentPart : part) {
                    waysScriptOs.write(String.format("CREATE TABLE \"nodes_%03d\" (like nodes) %s;\n", currentPart.getId(),
                            getColumnarString(storeColumnar)).getBytes(StandardCharsets.UTF_8));
                    List<Short> h33RegionsInside = currentPart.getH33RegionsInside();
                    Collections.sort(h33RegionsInside);
                    for(Short h33Region: h33RegionsInside){
                        waysScriptOs.write(String.format("COPY \"nodes_%03d\"(h3_3,h3_8,id,geom,tags) FROM '/input/nodes/%05d.tsv' DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV;\n",currentPart.getId(), h33Region).getBytes(StandardCharsets.UTF_8));
                    }
                }
                for (Partition currentPart : part) {
                    waysScriptOs.write(String.format("ALTER TABLE  nodes ATTACH PARTITION  \"nodes_%03d\" FOR VALUES FROM (%s) TO (%s);\n", currentPart.getId(),currentPart.getMinRange(),currentPart.getMaxRange()).getBytes(StandardCharsets.UTF_8));
                }
                waysScriptOs.write("COMMIT;".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
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

    public static int createWaysScript(File resultDirectory, int scriptCount, List<Partition> partitions, boolean storeColumnar) {
        final List<List<Partition>> partition = Lists.partition(partitions, partitions.size()/ scriptCount +1);
        for (int waysPartIdx = 0; waysPartIdx < partition.size(); waysPartIdx++) {
            List<Partition> part = partition.get(waysPartIdx);
            String scriptName = String.format("sql/ways_import_%03d.sql", waysPartIdx);
            try (FileOutputStream waysScriptOs = new FileOutputStream(new File(resultDirectory, scriptName))){
                appendScriptHead(scriptName, waysScriptOs);
                waysScriptOs.write("BEGIN;\n".getBytes(StandardCharsets.UTF_8));
                for (Partition currentPart : part) {
                    waysScriptOs.write(String.format("CREATE TABLE \"ways_%03d\" (like ways) %s;\n", currentPart.getId(),
                            getColumnarString(storeColumnar)).getBytes(StandardCharsets.UTF_8));
                    List<Short> h33RegionsInside = currentPart.getH33RegionsInside();
                    Collections.sort(h33RegionsInside);
                    for(Short h33Region: h33RegionsInside){
                        waysScriptOs.write(String.format("COPY \"ways_%03d\"(h3_3,h3_8,id,closed,building,highway,scale,centre,bbox,linestring,points,h3_8_regions,tags) FROM '/input/ways/%05d.tsv' DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV;\n",currentPart.getId(), h33Region).getBytes(StandardCharsets.UTF_8));
                    }
                }
                for (Partition currentPart : part) {
                    waysScriptOs.write(String.format("ALTER TABLE  ways ATTACH PARTITION  \"ways_%03d\" FOR VALUES FROM (%s) TO (%s);\n", currentPart.getId(),currentPart.getMinRange(),currentPart.getMaxRange()).getBytes(StandardCharsets.UTF_8));
                }
                waysScriptOs.write("COMMIT;".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return partition.size();
    }

    public static void createMultipolygonScript(File resultDirectory, List<Partition> partitions, boolean storeColumnar) {
        try (FileOutputStream waysScriptOs = new FileOutputStream(new File(resultDirectory, "static/multipolygon_tables.sql"))) {
            for (Partition currentPart : partitions) {
                waysScriptOs.write(String.format("CREATE TABLE  \"multipolygon_%03d\" PARTITION OF multipolygon FOR VALUES FROM (%s) TO (%s) %s;\n",
                        currentPart.getId(),currentPart.getMinRange(),currentPart.getMaxRange(),
                        getColumnarString(storeColumnar)).getBytes(StandardCharsets.UTF_8)
                );
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Partition> distributeH33ByPartitionsForWays(Map<Short, Long> waysSizeStat, double thresholdPercentFromMaxPartition){
        Map<Short, Partition> h3RangeInH2 = null;
        Map<Short, Short> h33to2 = null;
        try {
            h3RangeInH2 = getH3RangeInH2();
            h33to2 = parseH33to2();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Map<Short, Short> finalH33to = h33to2;
        Map<Short, Long> sumByH2 = new TreeMap<>(waysSizeStat.entrySet().stream().map(entry -> new AbstractMap.SimpleImmutableEntry<>(finalH33to.get(entry.getKey()), entry.getValue())).collect(Collectors.groupingBy(Map.Entry::getKey,
                Collectors.summingLong(Map.Entry::getValue))));
        long maxPartitionSize = (long) (sumByH2.values().stream().max(Comparator.comparingLong(Long::longValue)).get()* thresholdPercentFromMaxPartition);
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
                System.out.println(partitionNumber++ +"\t["+lowerBound+","+ maxRangeOfInterval +")\t"+currentSum+"\t"+(maxRangeOfInterval-lowerBound));
                lowerBound=nextRange.getMinRange();
                currentSum=entry.getValue();
                continue;
            }
            currentSum+=entry.getValue();
        }
        if(currentSum!=0){
            short maxRangeOfInterval = HIGH_H3_3_BOUND;
            partitions.add(createPartition(waysSizeStat, currentSum, lowerBound, partitionNumber, maxRangeOfInterval));
            System.out.println(partitionNumber+"\t["+lowerBound+","+maxRangeOfInterval+")\t"+currentSum+"\t"+(maxRangeOfInterval-lowerBound));
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
            Stream<String> stream = new BufferedReader(inputStreamReader).lines();
            Map<Short, Partition> h3RangeInH2 = stream.map(line -> {
                String[] parts = line.split("\t");
                return new AbstractMap.SimpleImmutableEntry<>(Short.parseShort(parts[0]), new Partition(Short.parseShort(parts[1]), Short.parseShort(parts[2])));
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return h3RangeInH2;
        }

    }

    private static Map<Short, Short> parseH33to2() throws IOException {
        try (InputStreamReader inputStreamReader = new InputStreamReader(
                Objects.requireNonNull(PartitionSplitter.class.getResourceAsStream("/h3_3_to_2.tsv")))) {
            Stream<String> stream = new BufferedReader(inputStreamReader).lines();
            Map<Short, Short> h33to2 = new TreeMap<>(stream.map(line -> {
                String[] parts = line.split("\t");
                return new AbstractMap.SimpleImmutableEntry<>(Short.parseShort(parts[0]), Short.parseShort(parts[1]));
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            return h33to2;
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
