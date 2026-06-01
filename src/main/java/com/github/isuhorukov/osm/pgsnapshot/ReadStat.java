package com.github.isuhorukov.osm.pgsnapshot;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.BlockStat;
import com.github.isuhorukov.osm.pgsnapshot.model.statistics.PbfStatistics;
import com.github.isuhorukov.osm.pgsnapshot.model.table.StatType;
import com.github.isuhorukov.osm.pgsnapshot.model.table.TableStat;
import com.github.isuhorukov.osm.pgsnapshot.util.CompactH3;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class ReadStat {
    private static final Logger log = LoggerFactory.getLogger(ReadStat.class);

    public static void main(String[] args) throws Exception{
        H3Core h3Core = H3Core.newInstance();
        long h3 = CompactH3.toFull3(870);//
        //h3Core.cellToLatLng(h3);
        LatLng base = h3Core.cellToLatLng(h3);
        //latitude 0 90
        for (double latitude=0; latitude<90; latitude+=1){
            /*for(int id=0;id<16;id++)*/{ int id=8;
                long measure = h3Core.latLngToCell(latitude, 0, id);
                //long measure = h3Core.latLngToCell(base.lat, base.lng, id);
                LatLng latLng = h3Core.cellToLatLng(measure);
                List<LatLng> latLngs = h3Core.cellToBoundary(measure);
                double rad = 0;
                for(LatLng lng: latLngs){
                    double distance = Math.sqrt(Math.pow(latLng.lat - lng.lat, 2) + Math.pow(latLng.lng - lng.lng, 2));
                    rad = Math.max(distance,rad);
                }
                log.info("{}\t{}", latitude, rad);
            }
        }

        try (ObjectInputStream inputStream = new ObjectInputStream(
                new FileInputStream("/home/iam/dev/map/thailand/thailand-latest_loc_ways/statistics.obj"))) {
            PbfStatistics pbfStatistics = (PbfStatistics) inputStream.readObject();
            Collection<BlockStat> blockStats = pbfStatistics.getBlockStatistics();

            List<TableStat> tableStat = blockStats.stream().map(blockStat -> {
                if (blockStat.getRelationCount() > 0) {
                    return null;
                }
                if (blockStat.getNodeCount() > 0 && blockStat.getWayCount() > 0) {
                    throw new IllegalStateException("Invalid block - mixed content nodes and ways");
                }
                if (blockStat.getNodeCount() > 0) {
                    return blockStat.getNodeStat().values().stream().map(stat -> new TableStat(StatType.N, blockStat.getId(), stat)).collect(toList());
                }
                if (blockStat.getWayCount() > 0) {
                    return blockStat.getWayStat().values().stream().map(stat -> new TableStat(StatType.W, blockStat.getId(), stat)).collect(toList());
                }
                throw new IllegalStateException("Unexpected block without nodes, ways or relations");
            }).filter(Objects::nonNull).flatMap(Collection::stream).collect(toList());

            Map<Short, List<TableStat>> ways = tableStat.stream().filter(tableStat1 -> tableStat1.getType() == StatType.W).sorted(Comparator.comparingInt(TableStat::getH33).thenComparing(TableStat::getBlockId)).collect(Collectors.groupingBy(TableStat::getH33, TreeMap::new, toList()));
            Map<Short, List<TableStat>> nodes = tableStat.stream().filter(tableStat1 -> tableStat1.getType() == StatType.N).sorted(Comparator.comparingInt(TableStat::getH33).thenComparing(TableStat::getBlockId)).collect(Collectors.groupingBy(TableStat::getH33, TreeMap::new, toList()));
            log.info("ways h33 buckets: {}, nodes h33 buckets: {}", ways.size(), nodes.size());
        }
    }

}
