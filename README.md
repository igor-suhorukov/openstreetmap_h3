# openstreetmap_h3
Transform OpenStreetMap World/Region PBF dump into partitioned by [H3 regions](https://h3geo.org) PostGIS pgsnapshot (loseless) OSM schema representation and/or into ArrowIPC/Parquet dumps

More details is coming on [PGConf.Russia 2023](https://pgconf.ru/2023) and my blog posts...


## Build:
```
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
    -result_in_arrow
      Save data transforming and enrichment in Apache Arrow format
      Default: false
    -result_in_tsv
      Save result data in TabSeparatedValue format for PostgreSQL COPY
      Default: true
    -scale_approx_calc
      Approximate scale calculation. Value 'false' - distance in meter
      Default: false
    -skip_buildings
      Skip any ways with 'building' tag
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
