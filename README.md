# openstreetmap_h3
OSM planet dump high performance data loader. Transform OpenStreetMap World/Region PBF dump into partitioned by [H3 regions](https://h3geo.org) PostGIS pgsnapshot (lossless) OSM schema representation and/or into ArrowIPC/Parquet dumps [with schema](https://github.com/igor-suhorukov/openstreetmap_h3/tree/master/src/main/resources/schema). "columnar_storage" option activate Citus columnar storage in PostgreSQL to speedup aggregation queries.

More details in [blog posts](https://habr.com/en/post/717408)...

![image](https://user-images.githubusercontent.com/10332206/217207435-0ee000c0-d66c-4c99-9fe2-71dfaf5e5396.png)

![image](https://user-images.githubusercontent.com/10332206/219882376-6cfa3de7-697e-4e22-8d83-6dd0495d79db.png)

## Build and run in Docker
``` bash
git clone https://github.com/igor-suhorukov/openstreetmap_h3.git
cd openstreetmap_h3 && docker build -t openstreetmap_h3 .
cd postgis_docker-master && docker build -t postgres16_postgis .

wget https://download.geofabrik.de/europe/cyprus-latest.osm.pbf
docker run -it --rm -w $(pwd) -v $(pwd):/$(pwd) openstreetmap_h3:latest -source_pbf $(pwd)/cyprus-latest.osm.pbf -result_in_tsv true
docker run --name postgis16-cyprus --memory=12g --memory-swap=12g --memory-swappiness 0 --shm-size=1g -v $(pwd)/database:/var/lib/postgresql/data -v $(pwd)/cyprus-latest_loc_ways:/input -e POSTGRES_PASSWORD=osmworld -d -p 5432:5432 postgres16_postgis:latest -c checkpoint_timeout='15 min' -c checkpoint_completion_target=0.9 -c shared_buffers='4096 MB' -c wal_buffers=-1 -c bgwriter_delay=200ms -c bgwriter_lru_maxpages=100 -c bgwriter_lru_multiplier=2.0 -c bgwriter_flush_after=0 -c max_wal_size='32768 MB' -c min_wal_size='16384 MB'
```

## Build and run in JVM:
``` bash
~/dev/projects/oss_contrib/openstreetmap_h3$ mvn install
[INFO] Scanning for projects...
[INFO] 
[INFO] -------< com.github.igor-suhorukov:osm-to-pgsnapshot-schema-ng >--------
[INFO] Building osm-to-pgsnapshot-schema-ng 1.0-SNAPSHOT
[INFO] --------------------------------[ jar ]---------------------------------
[INFO] 
[INFO] --- maven-resources-plugin:2.6:resources (default-resources) @ osm-to-pgsnapshot-schema-ng ---
[WARNING] Using platform encoding (UTF-8 actually) to copy filtered resources, i.e. build is platform dependent!
[INFO] Copying 12 resources
[INFO] 
[INFO] --- maven-compiler-plugin:3.1:compile (default-compile) @ osm-to-pgsnapshot-schema-ng ---
[INFO] Changes detected - recompiling the module!
[WARNING] File encoding has not been set, using platform encoding UTF-8, i.e. build is platform dependent!
[INFO] Compiling 24 source files to /home/acc/dev/projects/oss_contrib/openstreetmap_h3/target/classes
[INFO] 
[INFO] --- maven-resources-plugin:2.6:testResources (default-testResources) @ osm-to-pgsnapshot-schema-ng ---
[WARNING] Using platform encoding (UTF-8 actually) to copy filtered resources, i.e. build is platform dependent!
[INFO] skip non existing resourceDirectory /home/acc/dev/projects/oss_contrib/openstreetmap_h3/src/test/resources
[INFO] 
[INFO] --- maven-compiler-plugin:3.1:testCompile (default-testCompile) @ osm-to-pgsnapshot-schema-ng ---
[INFO] No sources to compile
[INFO] 
[INFO] --- maven-surefire-plugin:2.12.4:test (default-test) @ osm-to-pgsnapshot-schema-ng ---
[INFO] No tests to run.
[INFO] 
[INFO] --- maven-jar-plugin:2.4:jar (default-jar) @ osm-to-pgsnapshot-schema-ng ---
[INFO] Building jar: /home/acc/dev/projects/oss_contrib/openstreetmap_h3/target/osm-to-pgsnapshot-schema-ng-1.0-SNAPSHOT.jar
[INFO] 
[INFO] --- spring-boot-maven-plugin:2.7.3:repackage (default) @ osm-to-pgsnapshot-schema-ng ---
[INFO] Layout: ZIP
[INFO] Replacing main artifact with repackaged archive
[INFO] 
[INFO] --- maven-install-plugin:2.4:install (default-install) @ osm-to-pgsnapshot-schema-ng ---
[INFO] Installing /home/acc/dev/projects/oss_contrib/openstreetmap_h3/target/osm-to-pgsnapshot-schema-ng-1.0-SNAPSHOT.jar to /home/acc/.m2/repository/com/github/igor-suhorukov/osm-to-pgsnapshot-schema-ng/1.0-SNAPSHOT/osm-to-pgsnapshot-schema-ng-1.0-SNAPSHOT.jar
[INFO] Installing /home/acc/dev/projects/oss_contrib/openstreetmap_h3/pom.xml to /home/acc/.m2/repository/com/github/igor-suhorukov/osm-to-pgsnapshot-schema-ng/1.0-SNAPSHOT/osm-to-pgsnapshot-schema-ng-1.0-SNAPSHOT.pom
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  29.592 s
[INFO] Finished at: 2023-01-29T11:47:33+03:00
[INFO] ------------------------------------------------------------------------

```

## Usage details:
```
~/dev/projects/oss_contrib/openstreetmap_h3$ java -jar target/osm-to-pgsnapshot-schema-ng-1.0-SNAPSHOT.jar
The following option is required: [-source_pbf]
Usage: <main class> [options]
  Options:
  * -source_pbf
      Source path for OpenStreetMap data in PBF format
    -collect_only_statistics
      Collect only statistics from data - partition distribution
      Default: false
    -columnar_storage
      Use columnar storage in PostgreSql tables for nodes/ways/multipolygon
      Default: false
    -data_partition_ratio
      Filling ratio from maximum size of partition. This parameter change 
      PostgreSQL partitions count
      Default: 0.48
    -help
      Information about command line parameters
    -pg_script_count
      Script count for PostgreSQL parallel COPY
      Default: 4
    -preserve_all_nodes
      Preserve all nodes information in case of 'true' or only nodes with tags 
      or referenced from relations in other case
      Default: false
    -arrow_format
      In case of not null parameter save Arrow data serialization: ARROW_IPC 
      or PARQUET
      Possible Values: [PARQUET, ARROW_IPC]
    -result_in_tsv
      Save result data in TabSeparatedValue format for PostgreSQL COPY
      Default: true
    -scale_approx_calc
      Approximate scale calculation. Value 'false' - distance in meter
      Default: false
    -skip_buildings
      Skip any ways with 'building' tag
      Default: false
    -skip_highway
      Skip any ways with 'highway' tag
      Default: false
    -worker_threads
      Worker threads count for data processing
      Default: 4
```


## Run:
```

~/dev/projects/oss_contrib/openstreetmap_h3$ java -jar target/osm-to-pgsnapshot-schema-ng-1.0-SNAPSHOT.jar -source_pbf /home/acc/dev/map/thailand/thailand-latest.osm.pbf

[ 0:00] Started osmium add-locations-to-ways
[ 0:00]   osmium version 1.14.0
[ 0:00]   libosmium version 2.18.0
[ 0:00] Command line options and default settings:
[ 0:00]   input options:
[ 0:00]     file names: 
[ 0:00]       thailand-latest.osm.pbf
[ 0:00]     file format: 
[ 0:00]   output options:
[ 0:00]     file name: thailand-latest_loc_ways.pbf
[ 0:00]     file format: pbf,pbf_compression=none
[ 0:00]     generator: osmium/1.14.0
[ 0:00]     overwrite: no
[ 0:00]     fsync: no
[ 0:00]   other options:
[ 0:00]     index type (for positive ids): sparse_mem_array
[ 0:00]     index type (for negative ids): flex_mem
[ 0:00]     keep untagged nodes: no
[ 0:00]     keep nodes that are relation members: yes
[ 0:00] 
[ 0:00] Getting all nodes referenced from relations...
[ 0:01] Found 7508 nodes referenced from relations.
[ 0:01] Copying input file 'thailand-latest.osm.pbf'...
[ 0:13] About 540 MBytes used for node location index (in main memory or on disk).
[ 0:13] Peak memory used: 2152 MBytes
[ 0:13] Done.

0
566  time 89572
diff between total and processing 45908
total thread time 358078
total processing time 312170
total save time 9689
total waiting for save time 23255
thread max time 2482
processing max time 2065
nodes 382390
ways 4117237
relations 18317
relation members 191294
multipolygon count 6995
0	[-32768,25648)	67392403	58416
1	[25648,25728)	46338970	80
2	[25728,25744)	87393905	16
3	[25744,25760)	41088683	16
4	[25760,25768)	204997591	8
5	[25768,25880)	78535020	112
6	[25880,25992)	41090100	112
7	[25992,26000)	84484353	8
8	[26000,26008)	17026978	8
9	[26008,26016)	135197249	8
10	[26016,32695)	223367	6679
11 0 73.06987445454546

[ 0:00] Started osmium export
[ 0:00]   osmium version 1.14.0
[ 0:00]   libosmium version 2.18.0
[ 0:00] Command line options and default settings:
[ 0:00]   input options:
[ 0:00]     file name: thailand-latest.osm.pbf
[ 0:00]     file format: 
[ 0:00]   output options:
[ 0:00]     file name: thailand-latest_loc_ways/multipolygon/source.tsv
[ 0:00]     file format: pg
[ 0:00]     overwrite: no
[ 0:00]     fsync: yes
[ 0:00]   attributes:
[ 0:00]     type:      @type
[ 0:00]     id:        @id
[ 0:00]     version:   (omitted)
[ 0:00]     changeset: (omitted)
[ 0:00]     timestamp: (omitted)
[ 0:00]     uid:       (omitted)
[ 0:00]     user:      (omitted)
[ 0:00]     way_nodes: (omitted)
[ 0:00]   output format options:
[ 0:00]     tags_type = hstore
[ 0:00]   linear tags: none
[ 0:00]   area tags:   one of the following:
[ 0:00]     @type=relation
[ 0:00]   other options:
[ 0:00]     index type: sparse_mem_array
[ 0:00]     add unique IDs: no
[ 0:00]     keep untagged features: no
[ 0:00] 
[ 0:00] Create table with something like this:
[ 0:00] CREATE EXTENSION IF NOT EXISTS hstore;
[ 0:00] CREATE TABLE osmdata (
[ 0:00]     geom      GEOMETRY, -- or GEOGRAPHY
[ 0:00]     osm_type  TEXT,
[ 0:00]     osm_id    BIGINT,
[ 0:00]     tags      hstore
[ 0:00] );
[ 0:00] Then load data with something like this:
[ 0:00] \copy osmdata FROM 'thailand-latest_loc_ways/multipolygon/source.tsv'
[ 0:00] 
[ 0:00] First pass (of two) through input file (reading relations)...
[ 0:01] First pass done.
[ 0:01] Second pass (of two) through input file...
Geometry error: Could not build area geometry
Geometry error: Could not build area geometry
Geometry error: Could not build area geometry
Geometry error: Could not build area geometry
Geometry error: Could not build area geometry
Geometry error: Could not build area geometry
Geometry error: Could not build area geometry
Geometry error: Could not build area geometry
Geometry error: Could not build area geometry
[ 0:20] About 540 MBytes used for node location index (in main memory or on disk).
[ 0:20] Second pass done.
[ 0:20] Wrote 156178 features.
[ 0:20] Encountered 9 errors.
[ 0:20] Peak memory used: 2134 MBytes
[ 0:20] Done.

cat /home/acc/dev/map/thailand/thailand-latest_loc_ways/multipolygon/source.tsv | grep $'\trelation\t' | split -l 1749 - /home/acc/dev/map/thailand/thailand-latest_loc_ways/multipolygon/multipolygon_

```

![image](https://user-images.githubusercontent.com/10332206/215318316-300e4ff0-167b-4250-9771-a39917f1e550.png)

```
~/dev/map/thailand/thailand-latest_loc_ways$ tree
.
├── import_related_metadata
│   ├── osm_file_block_content.tsv
│   ├── osm_file_block.tsv
│   └── osm_file_statistics.tsv
├── multipolygon
│   ├── multipolygon_aa
│   ├── multipolygon_ab
│   ├── multipolygon_ac
│   ├── multipolygon_ad
│   └── multipolygon_ae
├── nodes
│   ├── 16713.tsv
│   ├── 16717.tsv
│   ├── 16744.tsv
│   ├── 16745.tsv
│   ├── 16747.tsv
│   ├── 16748.tsv
│   ├── 16749.tsv
│   ├── 25600.tsv
│   ├── 25601.tsv
│   ├── 25606.tsv
│   ├── 25620.tsv
│   ├── 25622.tsv
│   ├── 25632.tsv
│   ├── 25634.tsv
│   ├── 25636.tsv
│   ├── 25638.tsv
│   ├── 25648.tsv
│   ├── 25649.tsv
│   ├── 25650.tsv
│   ├── 25651.tsv
│   ├── 25652.tsv
│   ├── 25653.tsv
│   ├── 25654.tsv
│   ├── 25728.tsv
│   ├── 25729.tsv
│   ├── 25730.tsv
│   ├── 25731.tsv
│   ├── 25732.tsv
│   ├── 25733.tsv
│   ├── 25734.tsv
│   ├── 25744.tsv
│   ├── 25745.tsv
│   ├── 25748.tsv
│   ├── 25749.tsv
│   ├── 25756.tsv
│   ├── 25760.tsv
│   ├── 25761.tsv
│   ├── 25762.tsv
│   ├── 25763.tsv
│   ├── 25764.tsv
│   ├── 25765.tsv
│   ├── 25766.tsv
│   ├── 25774.tsv
│   ├── 25776.tsv
│   ├── 25777.tsv
│   ├── 25778.tsv
│   ├── 25779.tsv
│   ├── 25780.tsv
│   ├── 25781.tsv
│   ├── 25782.tsv
│   ├── 25873.tsv
│   ├── 25880.tsv
│   ├── 25881.tsv
│   ├── 25882.tsv
│   ├── 25883.tsv
│   ├── 25884.tsv
│   ├── 25885.tsv
│   ├── 25886.tsv
│   ├── 25985.tsv
│   ├── 25989.tsv
│   ├── 25992.tsv
│   ├── 25993.tsv
│   ├── 25994.tsv
│   ├── 25995.tsv
│   ├── 25996.tsv
│   ├── 25998.tsv
│   ├── 26000.tsv
│   ├── 26001.tsv
│   ├── 26003.tsv
│   ├── 26008.tsv
│   ├── 26009.tsv
│   ├── 26010.tsv
│   ├── 26011.tsv
│   ├── 26012.tsv
│   ├── 26013.tsv
│   ├── 26014.tsv
│   ├── 26019.tsv
│   ├── 26026.tsv
│   ├── 26029.tsv
│   └── 26030.tsv
├── relations
│   ├── 00000.tsv
│   └── 00001.tsv
├── sql
│   ├── nodes_import_000.sql
│   ├── nodes_import_001.sql
│   ├── nodes_import_002.sql
│   ├── nodes_import_003.sql
│   ├── ways_import_000.sql
│   ├── ways_import_001.sql
│   ├── ways_import_002.sql
│   ├── ways_import_003.sql
│   ├── y_multipoly_aa.sql
│   ├── y_multipoly_ab.sql
│   ├── y_multipoly_ac.sql
│   ├── y_multipoly_ad.sql
│   └── y_multipoly_ae.sql
├── static
│   ├── database_after_init.sql
│   ├── database_init.sql
│   ├── h3_poly.tsv.gz
│   ├── multipolygon.sql
│   ├── multipolygon_tables.sql
│   └── osmium_export.json
└── ways
    ├── 16717.tsv
    ├── 16744.tsv
    ├── 16745.tsv
    ├── 16747.tsv
    ├── 16748.tsv
    ├── 16749.tsv
    ├── 25620.tsv
    ├── 25622.tsv
    ├── 25634.tsv
    ├── 25638.tsv
    ├── 25648.tsv
    ├── 25649.tsv
    ├── 25650.tsv
    ├── 25651.tsv
    ├── 25652.tsv
    ├── 25653.tsv
    ├── 25654.tsv
    ├── 25728.tsv
    ├── 25729.tsv
    ├── 25730.tsv
    ├── 25731.tsv
    ├── 25732.tsv
    ├── 25733.tsv
    ├── 25734.tsv
    ├── 25744.tsv
    ├── 25745.tsv
    ├── 25748.tsv
    ├── 25749.tsv
    ├── 25756.tsv
    ├── 25760.tsv
    ├── 25761.tsv
    ├── 25762.tsv
    ├── 25763.tsv
    ├── 25764.tsv
    ├── 25765.tsv
    ├── 25766.tsv
    ├── 25774.tsv
    ├── 25776.tsv
    ├── 25777.tsv
    ├── 25778.tsv
    ├── 25779.tsv
    ├── 25780.tsv
    ├── 25781.tsv
    ├── 25782.tsv
    ├── 25873.tsv
    ├── 25880.tsv
    ├── 25881.tsv
    ├── 25882.tsv
    ├── 25883.tsv
    ├── 25884.tsv
    ├── 25886.tsv
    ├── 25984.tsv
    ├── 25985.tsv
    ├── 25989.tsv
    ├── 25992.tsv
    ├── 25993.tsv
    ├── 25994.tsv
    ├── 25995.tsv
    ├── 25996.tsv
    ├── 25998.tsv
    ├── 26000.tsv
    ├── 26001.tsv
    ├── 26003.tsv
    ├── 26008.tsv
    ├── 26009.tsv
    ├── 26010.tsv
    ├── 26011.tsv
    ├── 26012.tsv
    ├── 26013.tsv
    ├── 26014.tsv
    ├── 26017.tsv
    ├── 26019.tsv
    ├── 26026.tsv
    ├── 26030.tsv
    └── 32767.tsv

7 directories, 184 files
```

## Run PostGIS and import data:
```
docker run --name postgis14-thailand --memory=12g --memory-swap=12g --memory-swappiness 0 --shm-size=1g -v /home/acc/dev/map/database/thailand:/var/lib/postgresql/data -v /home/acc/dev/map/thailand/thailand-latest_loc_ways:/input -e POSTGRES_PASSWORD=osmworld -e LD_LIBRARY_PATH=/usr/lib/jvm/java-11-openjdk-amd64/lib/server/ -d -p 5432:5432 -p 5005:5005 5d411c3be57f -c checkpoint_timeout='15 min' -c checkpoint_completion_target=0.9 -c shared_buffers='4096 MB' -c wal_buffers=-1 -c bgwriter_delay=200ms -c bgwriter_lru_maxpages=100 -c bgwriter_lru_multiplier=2.0 -c bgwriter_flush_after=0 -c max_wal_size='32768 MB' -c min_wal_size='16384 MB'

docker logs postgis14-thailand | tail -n 40
initdb: warning: enabling "trust" authentication for local connections
You can change this by editing pg_hba.conf or using the option -A, or
--auth-local and --auth-host, the next time you run initdb.
2023-01-29 10:22:09.841 UTC [1] LOG:  starting PostgreSQL 14.1 (Debian 14.1-1.pgdg110+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
2023-01-29 10:22:09.841 UTC [1] LOG:  listening on IPv4 address "0.0.0.0", port 5432
2023-01-29 10:22:09.841 UTC [1] LOG:  listening on IPv6 address "::", port 5432
2023-01-29 10:22:09.847 UTC [1] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
2023-01-29 10:22:09.855 UTC [163] LOG:  database system was shut down at 2023-01-29 10:22:08 UTC
2023-01-29 10:22:09.863 UTC [1] LOG:  database system is ready to accept connections
Time: 16.011 ms
ANALYZE
Time: 49.590 ms
CREATE TABLE
Time: 1.509 ms
COPY 1
Time: 1.110 ms
ANALYZE
Time: 1.167 ms
CREATE TABLE
Time: 1.284 ms
COPY 566
Time: 2.092 ms
ANALYZE
Time: 1.880 ms
CREATE TABLE
Time: 1.210 ms
COPY 17482
Time: 19.023 ms
ANALYZE
Time: 27.044 ms
SELECT 80
Time: 8.333 ms
ANALYZE
Time: 1.533 ms
SELECT 75
Time: 9.650 ms
ANALYZE
Time: 1.213 ms

2023-01-29 10:22:07.219 UTC [49] LOG:  received fast shutdown request
waiting for server to shut down....2023-01-29 10:22:07.222 UTC [49] LOG:  aborting any active transactions
2023-01-29 10:22:07.223 UTC [49] LOG:  background worker "logical replication launcher" (PID 56) exited with exit code 1
2023-01-29 10:22:07.392 UTC [51] LOG:  shutting down
..2023-01-29 10:22:09.631 UTC [49] LOG:  database system is shut down
 done
server stopped

PostgreSQL init process complete; ready for start up.

docker exec -it postgis14-thailand psql -U postgres -d osmworld
psql (14.1 (Debian 14.1-1.pgdg110+1))
Type "help" for help.

osmworld=# \d
                       List of relations
 Schema |          Name          |       Type        |  Owner   
--------+------------------------+-------------------+----------
 public | geography_columns      | view              | postgres
 public | geometry_columns       | view              | postgres
 public | h3_3_bounds_complex    | table             | postgres
 public | multipolygon           | partitioned table | postgres
 public | multipolygon_000       | table             | postgres
 public | multipolygon_001       | table             | postgres
 public | multipolygon_002       | table             | postgres
 public | multipolygon_003       | table             | postgres
 public | multipolygon_004       | table             | postgres
 public | multipolygon_005       | table             | postgres
 public | multipolygon_006       | table             | postgres
 public | multipolygon_007       | table             | postgres
 public | multipolygon_008       | table             | postgres
 public | multipolygon_009       | table             | postgres
 public | multipolygon_010       | table             | postgres
 public | multipolygon_32767     | table             | postgres
 public | nodes                  | partitioned table | postgres
 public | nodes_000              | table             | postgres
 public | nodes_001              | table             | postgres
 public | nodes_002              | table             | postgres
 public | nodes_003              | table             | postgres
 public | nodes_004              | table             | postgres
 public | nodes_005              | table             | postgres
 public | nodes_006              | table             | postgres
 public | nodes_007              | table             | postgres
 public | nodes_008              | table             | postgres
 public | nodes_009              | table             | postgres
 public | nodes_010              | table             | postgres
 public | osm_file_block         | table             | postgres
 public | osm_file_block_content | table             | postgres
 public | osm_file_statistics    | table             | postgres
 public | osm_stat_nodes_3_3     | table             | postgres
 public | osm_stat_ways_3_3      | table             | postgres
 public | relation_members       | table             | postgres
 public | relations              | table             | postgres
 public | spatial_ref_sys        | table             | postgres
 public | ways                   | partitioned table | postgres
 public | ways_000               | table             | postgres
 public | ways_001               | table             | postgres
 public | ways_002               | table             | postgres
 public | ways_003               | table             | postgres
 public | ways_004               | table             | postgres
 public | ways_005               | table             | postgres
 public | ways_006               | table             | postgres
 public | ways_007               | table             | postgres
 public | ways_008               | table             | postgres
 public | ways_009               | table             | postgres
 public | ways_010               | table             | postgres
 public | ways_32767             | table             | postgres
(49 rows)

osmworld=# explain select h3_3, count(*) from ways group by 1;
                                        QUERY PLAN                                         
-------------------------------------------------------------------------------------------
 Gather  (cost=34456.99..97465.64 rows=76 width=10)
   Workers Planned: 4
   ->  Parallel Append  (cost=33456.99..96458.04 rows=19 width=10)
         ->  HashAggregate  (cost=96457.88..96457.95 rows=7 width=10)
               Group Key: ways_4.h3_3
               ->  Seq Scan on ways_004 ways_4  (cost=0.00..90269.92 rows=1237592 width=2)
         ->  HashAggregate  (cost=60359.46..60359.53 rows=7 width=10)
               Group Key: ways_9.h3_3
               ->  Seq Scan on ways_009 ways_9  (cost=0.00..56666.64 rows=738564 width=2)
         ->  HashAggregate  (cost=39018.48..39018.54 rows=6 width=10)
               Group Key: ways_7.h3_3
               ->  Seq Scan on ways_007 ways_7  (cost=0.00..36567.32 rows=490232 width=2)
         ->  HashAggregate  (cost=33456.99..33457.06 rows=7 width=10)
               Group Key: ways_2.h3_3
               ->  Seq Scan on ways_002 ways_2  (cost=0.00..31580.66 rows=375266 width=2)
         ->  HashAggregate  (cost=30029.44..30029.53 rows=9 width=10)
               Group Key: ways_5.h3_3
               ->  Seq Scan on ways_005 ways_5  (cost=0.00..28346.96 rows=336496 width=2)
         ->  HashAggregate  (cost=26775.62..26775.72 rows=10 width=10)
               Group Key: ways.h3_3
               ->  Seq Scan on ways_000 ways  (cost=0.00..25251.75 rows=304775 width=2)
         ->  HashAggregate  (cost=19023.96..19024.03 rows=7 width=10)
               Group Key: ways_1.h3_3
               ->  Seq Scan on ways_001 ways_1  (cost=0.00..17901.97 rows=224397 width=2)
         ->  HashAggregate  (cost=15682.35..15682.43 rows=8 width=10)
               Group Key: ways_6.h3_3
               ->  Seq Scan on ways_006 ways_6  (cost=0.00..14803.23 rows=175823 width=2)
         ->  HashAggregate  (cost=13881.85..13881.90 rows=5 width=10)
               Group Key: ways_3.h3_3
               ->  Seq Scan on ways_003 ways_3  (cost=0.00..13173.90 rows=141590 width=2)
         ->  HashAggregate  (cost=6970.89..6970.92 rows=3 width=10)
               Group Key: ways_8.h3_3
               ->  Seq Scan on ways_008 ways_8  (cost=0.00..6567.26 rows=80726 width=2)
         ->  HashAggregate  (cost=2385.09..2385.10 rows=1 width=10)
               Group Key: ways_11.h3_3
               ->  Seq Scan on ways_32767 ways_11  (cost=0.00..2327.06 rows=11606 width=2)
         ->  HashAggregate  (cost=37.55..37.59 rows=4 width=10)
               Group Key: ways_10.h3_3
               ->  Seq Scan on ways_010 ways_10  (cost=0.00..36.70 rows=170 width=2)
(39 rows)

osmworld=# select h3_3, count(*) from ways group by 1 order by 2 desc limit 20;
 h3_3  | count  
-------+--------
 25764 | 890643
 26010 | 329973
 25994 | 203645
 25730 | 188810
 25995 | 139373
 26011 | 136930
 25780 | 118165
 25765 | 105996
 26009 | 100196
 25762 |  91177
 26014 |  75555
 26003 |  71479
 26008 |  70353
 25883 |  60808
 25634 |  59705
 25777 |  58151
 16749 |  57887
 25782 |  57509
 25638 |  57296
 25880 |  56838
(20 rows)
```

## Apache Parquet export

Parquet export activated with "-arrow_format PARQUET" command line parameter.
OpenStreetMap H3 also allows you to convert data to Apache Parquet and process it in Hadoop and Apache Spark/Sedona. In this project, it allows you to do the same as osm-parquetizer but with additional data and collected geo coordinates for ways, as well as enriched h3 geo indexes. The files are successfully uploaded to the DuckDB Geo extension, I checked this and I will tell you the details in the publication.

```
./spark-shell --packages org.apache.sedona:sedona-python-adapter-3.0_2.12:1.3.1-incubating,org.apache.sedona:sedona-viz-3.0_2.12:1.3.1-incubating,org.datasyslab:geotools-wrapper:1.3.0-27.2 --conf spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions

23/03/26 21:36:19 WARN Utils: Your hostname, geohost resolves to a loopback address: 127.0.1.1; using 192.168.0.209 instead (on interface wlp0s20f3)
23/03/26 21:36:19 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/home/geo/dev/tools/spark-3.2.2-bin-hadoop3.2/jars/spark-unsafe_2.12-3.2.2.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
:: loading settings :: url = jar:file:/home/geo/dev/tools/spark-3.2.2-bin-hadoop3.2/jars/ivy-2.5.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
Ivy Default Cache set to: /home/geo/.ivy2/cache
The jars for the packages stored in: /home/geo/.ivy2/jars
org.apache.sedona#sedona-python-adapter-3.0_2.12 added as a dependency
org.apache.sedona#sedona-viz-3.0_2.12 added as a dependency
org.datasyslab#geotools-wrapper added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-47c49091-20ed-49ad-88f1-026d43baeed8;1.0
	confs: [default]
	found org.apache.sedona#sedona-python-adapter-3.0_2.12;1.3.1-incubating in central
	found org.locationtech.jts#jts-core;1.18.2 in local-m2-cache
	found org.wololo#jts2geojson;0.16.1 in central
	found org.apache.sedona#sedona-core-3.0_2.12;1.3.1-incubating in central
	found org.apache.sedona#sedona-common;1.3.1-incubating in central
	found org.apache.sedona#sedona-sql-3.0_2.12;1.3.1-incubating in central
	found org.scala-lang.modules#scala-collection-compat_2.12;2.5.0 in central
	found org.apache.sedona#sedona-viz-3.0_2.12;1.3.1-incubating in central
	found org.beryx#awt-color-factory;1.0.0 in central
	found org.datasyslab#geotools-wrapper;1.3.0-27.2 in central
:: resolution report :: resolve 275ms :: artifacts dl 11ms
	:: modules in use:
	org.apache.sedona#sedona-common;1.3.1-incubating from central in [default]
	org.apache.sedona#sedona-core-3.0_2.12;1.3.1-incubating from central in [default]
	org.apache.sedona#sedona-python-adapter-3.0_2.12;1.3.1-incubating from central in [default]
	org.apache.sedona#sedona-sql-3.0_2.12;1.3.1-incubating from central in [default]
	org.apache.sedona#sedona-viz-3.0_2.12;1.3.1-incubating from central in [default]
	org.beryx#awt-color-factory;1.0.0 from central in [default]
	org.datasyslab#geotools-wrapper;1.3.0-27.2 from central in [default]
	org.locationtech.jts#jts-core;1.18.2 from local-m2-cache in [default]
	org.scala-lang.modules#scala-collection-compat_2.12;2.5.0 from central in [default]
	org.wololo#jts2geojson;0.16.1 from central in [default]
	---------------------------------------------------------------------
	|                  |            modules            ||   artifacts   |
	|       conf       | number| search|dwnlded|evicted|| number|dwnlded|
	---------------------------------------------------------------------
	|      default     |   10  |   0   |   0   |   0   ||   10  |   0   |
	---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-47c49091-20ed-49ad-88f1-026d43baeed8
	confs: [default]
	0 artifacts copied, 10 already retrieved (0kB/7ms)
23/03/26 21:36:19 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark context Web UI available at http://192.168.0.209:4040
Spark context available as 'sc' (master = local[*], app id = local-1679855783524).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.2.2
      /_/
         
Using Scala version 2.12.15 (OpenJDK 64-Bit Server VM, Java 11.0.18)
Type in expressions to have them evaluated.
Type :help for more information.

scala> 

scala> val ways = spark.read.parquet("/home/geo/dev/map/phuket3/phuket_loc_ways/arrow/ways/*.parquet")
ways: org.apache.spark.sql.DataFrame = [id: bigint, h33: smallint ... 17 more fields]

scala> ways.createOrReplaceTempView("ways")

scala> spark.time(sql("select id,h33,tags,ST_SetSRID(ST_GeomFromWKB(lineStringWkb),4326) wkb_str from ways where closed and (tags['building']='school' or (tags['building'] is not null and tags['amenity']='school')) limit 50")).show()
Time taken: 126 ms
+---------+-----+--------------------+--------------------+                     
|       id|  h33|                tags|             wkb_str|
+---------+-----+--------------------+--------------------+
| 31985155|25764|{building -> scho...|LINESTRING (100.5...|
| 31985189|25764|{building -> scho...|LINESTRING (100.5...|
| 31985207|25764|{building -> scho...|LINESTRING (100.5...|
| 31988377|25764|{building -> scho...|LINESTRING (100.5...|
| 31988502|25764|{amenity -> libra...|LINESTRING (100.5...|
| 31988658|25764|{building -> scho...|LINESTRING (100.5...|
| 31988661|25764|{building -> scho...|LINESTRING (100.5...|
| 31988692|25764|{building -> scho...|LINESTRING (100.5...|
| 31988865|25764|{building -> scho...|LINESTRING (100.5...|
| 31989612|25764|{building -> scho...|LINESTRING (100.5...|
|101939892|25880|{building -> school}|LINESTRING (100.5...|
|492451660|25880|{building -> school}|LINESTRING (100.5...|
|492451661|25880|{building -> school}|LINESTRING (100.5...|
|492451663|25880|{building -> school}|LINESTRING (100.5...|
|492451668|25880|{building -> school}|LINESTRING (100.5...|
|492451672|25880|{building -> school}|LINESTRING (100.5...|
|492483595|25880|{building -> school}|LINESTRING (100.5...|
|272560749|25764|{building -> scho...|LINESTRING (100.5...|
|272560751|25764|{building -> scho...|LINESTRING (100.5...|
|272560753|25764|{building -> scho...|LINESTRING (100.5...|
+---------+-----+--------------------+--------------------+
only showing top 20 rows
```

Node:
```
scala> spark.read.parquet("/home/geo/arrow/nodes/*.parquet").printSchema
root                                                                            
|-- id: long (nullable = true)
|-- h33: short (nullable = true)
|-- h38: integer (nullable = true)
|-- latitude: double (nullable = true)
|-- longitude: double (nullable = true)
|-- tags: map (nullable = true)
|    |-- key: string
|    |-- value: string (valueContainsNull = true)
```
Way:
```
scala> spark.read.parquet("/home/geo/arrow/ways/*.parquet").printSchema
root                                                                            
|-- id: long (nullable = true)
|-- h33: short (nullable = true)
|-- h38: integer (nullable = true)
|-- latitude: double (nullable = true)
|-- longitude: double (nullable = true)
|-- tags: map (nullable = true)
|    |-- key: string
|    |-- value: string (valueContainsNull = true)
|-- pointIdxs: array (nullable = true)
|    |-- element: long (containsNull = true)
|-- h33Center: short (nullable = true)
|-- closed: boolean (nullable = true)
|-- building: boolean (nullable = true)
|-- highway: boolean (nullable = true)
|-- scale: float (nullable = true)
|-- lineStringWkb: binary (nullable = true)
|-- bboxWkb: binary (nullable = true)
|-- h38Indexes: array (nullable = true)
|    |-- element: integer (containsNull = true)
|-- bboxMinX: double (nullable = true)
|-- bboxMaxX: double (nullable = true)
|-- bboxMinY: double (nullable = true)
|-- bboxMaxY: double (nullable = true)
```
Relation:
```
scala> spark.read.parquet("/home/geo/arrow/relations/*.parquet").printSchema
root
|-- id: long (nullable = true)
|-- tags: map (nullable = true)
|    |-- key: string
|    |-- value: string (valueContainsNull = true)
|-- memberId: array (nullable = true)
|    |-- element: long (containsNull = true)
|-- memberType: array (nullable = true)
|    |-- element: byte (containsNull = true)
|-- memberRole: array (nullable = true)
|    |-- element: string (containsNull = true)
```
Multipolygon:
```
scala> spark.read.parquet("/home/geo/arrow/multipolygon.parquet").printSchema
root
|-- id: long (nullable = true)
|-- wkb_hex: string (nullable = true)
|-- tags_json: string (nullable = true)
```

### Use case diagram
![Usecase](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/igor-suhorukov/openstreetmap_h3/master/Usecase.puml)

### Project flowchart diagram

![OsmPbfTransformation](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/igor-suhorukov/openstreetmap_h3/master/OsmPbfTransformation.uml)
