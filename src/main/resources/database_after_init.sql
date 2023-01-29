\timing on
\c osmworld

BEGIN;
CREATE TABLE "preimport_ways_32767"  (like ways);
COPY "preimport_ways_32767"(h3_3,h3_8, id,closed,building,highway,scale,centre,bbox,linestring,points,h3_8_regions,tags) FROM '/input/ways/32767.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV;

create table preimport_ways_large_h3_3 as select id, array_agg (h3_3) h3_3_multi_regions from (select distinct wl.id, h3b.id h3_3 from "preimport_ways_32767"  wl inner join h3_3_bounds_complex h3b on ST_Intersects(wl.linestring, h3b.bounds)) a group by id;
create table ways_32767 as select  closed,building,highway,h3_3,h3_8, scale,tags,bbox,centre,p.id,linestring,points,h3_8_regions, CASE WHEN cardinality(h3.h3_3_multi_regions)>1 THEN h3.h3_3_multi_regions ELSE ARRAY(select distinct unnest((tags->'is.h33idxs')::smallint[]) order by 1)::smallint[]  END h3_3_multi_regions  from preimport_ways_32767 p inner join preimport_ways_large_h3_3 h3 on h3.id=p.id order by p.id;

alter table ways_32767 alter COLUMN id set NOT NULL;
alter table ways_32767 alter COLUMN h3_8 set NOT NULL;
alter table ways_32767 alter COLUMN scale set NOT NULL;
alter table ways_32767 alter COLUMN closed set NOT NULL;
alter table ways_32767 alter COLUMN building set NOT NULL;
alter table ways_32767 alter COLUMN highway set NOT NULL;
alter table ways_32767 alter COLUMN bbox set NOT NULL;
alter table ways_32767 alter COLUMN centre set NOT NULL;
alter table ways_32767 alter COLUMN linestring set NOT NULL;
alter table ways_32767 alter COLUMN points set NOT NULL;

ALTER TABLE ways ADD COLUMN h3_3_multi_regions smallint[];

ALTER TABLE  ways ATTACH PARTITION  "ways_32767" DEFAULT;
drop table preimport_ways_large_h3_3;
drop table preimport_ways_32767;
${substitute}

COPY relations(id,tags) FROM '/input/relations/00000.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV;
COPY relation_members(relation_id,member_id,member_type,member_role,sequence_id) FROM '/input/relations/00001.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV;
ALTER TABLE ONLY relation_members ADD CONSTRAINT pk_relation_members PRIMARY KEY (relation_id, sequence_id);

COMMIT;


\i /input/static/multipolygon.sql

--CREATE STATISTICS ways_stat ON id,h3_3,h3_8 FROM ways;

ANALYZE nodes;
ANALYZE ways;
ANALYZE multipolygon;
ANALYZE relations;
ANALYZE relation_members;
--create table preimport_multipolygon_relations_h3_3 as select m.id, CASE WHEN cardinality(array_agg(distinct h.id))=1 THEN min(h.id) ELSE null END h3_3,CASE WHEN cardinality(array_agg(distinct h.id))>1 THEN array_agg(distinct h.id) ELSE null END h3_3_multi_regions from preimport_multipolygon m left join h3_3_bounds_complex h on st_intersects(h.bounds,m.polygon) group by 1 order by 1;
--explain insert into multipolygon select p.id, h3_3, GREATEST(ST_XMax(ST_Envelope(polygon)) - ST_XMin(ST_Envelope(polygon)),ST_YMax(ST_Envelope(polygon)) - ST_YMin(ST_Envelope(polygon)))::real as scale, ST_Envelope(polygon) as bbox, polygon, tags, h3_3_multi_regions from preimport_multipolygon p left join preimport_multipolygon_relations_h3_3 h on p.id=h.id order by 2,1;
-- create table circle_multipolygon as select id,h3_3,scale,  CASE WHEN ST_MemSize(polygon)<10000 THEN (ST_MinimumBoundingRadius(polygon)).radius ELSE (ST_MinimumBoundingRadius(ST_Simplify(polygon, GREATEST(ST_XMax(ST_Envelope(polygon)) - ST_XMin(ST_Envelope(polygon)),ST_YMax(ST_Envelope(polygon)) - ST_YMin(ST_Envelope(polygon)))::real / 1000, true))).radius END radius from multipolygon;



--select id,(replace(replace(boundingradius,'(','{'),')','}')::text[])[1]::geometry(Geometry,4326) point, (replace(replace(boundingradius,'(','{'),')','}')::text[])[2]::real scale from preimport_circle_multipolygon limit 5;
--1481242,292 ms (24:41,242)
--previous version      create table multipolygon as select p.id, CASE WHEN cardinality(h3_3)=1 THEN h3_3[1] ELSE null END h3_3, GREATEST(ST_XMax(ST_Envelope(polygon)) - ST_XMin(ST_Envelope(polygon)),ST_YMax(ST_Envelope(polygon)) - ST_YMin(ST_Envelope(polygon)))::real as scale, ST_Envelope(polygon) as bbox, polygon, tags, CASE WHEN cardinality(h3_3)>1 THEN h3_3 ELSE null END h3_3_multi_regions from preimport_multipolygon p left join preimport_multipolygon_relations_h3_3 h on p.id=h.id order by 2,1;



--CREATE INDEX idx_nodes_id_h33 ON nodes USING btree (id,h3_3);
--CREATE INDEX idx_nodes_geom ON nodes USING gist (geom);
--CREATE INDEX idx_ways_id_h33 ON ways USING btree (id,h3_3);
--CREATE INDEX idx_ways_scale ON ways USING btree (scale);
--CREATE INDEX idx_ways_h3_8 ON ways USING btree (h3_8);
--CREATE INDEX idx_ways_linestring ON ways USING gist (linestring);

--create index idx_nodes_tags on nodes using gin(tags); Time: 4329236,691 ms (01:12:09,237)`


create table osm_file_statistics(
    multipolygonCount bigint NOT NULL,
    dataProcessingTime bigint NOT NULL,
    pbfSplitTime bigint NOT NULL,
    addLocationsToWaysTime bigint NOT NULL,
    multipolygonExportTime bigint NOT NULL,
    splitMultipolygonByPartsTime bigint NOT NULL,
    totalTime bigint NOT NULL
);
copy osm_file_statistics( multipolygoncount, dataprocessingtime,pbfsplittime,addlocationstowaystime, multipolygonexporttime, splitmultipolygonbypartstime, totaltime) from '/input/import_related_metadata/osm_file_statistics.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV;
ANALYZE osm_file_statistics;

create table osm_file_block(
    id bigint NOT NULL,
    nodeCount bigint NOT NULL,
    wayCount bigint NOT NULL,
    relationCount bigint NOT NULL,
    relationMembersCount bigint NOT NULL,
    multipolygonCount bigint NOT NULL,
    processingTime bigint NOT NULL,
    waitingForSaveTime bigint NOT NULL,
    saveTime bigint NOT NULL,
    threadTime bigint NOT NULL,
    threadStart bigint NOT NULL
);
copy osm_file_block(id, nodeCount, wayCount, relationCount, relationMembersCount, multipolygonCount, processingTime, waitingForSaveTime, saveTime, threadTime, threadStart) from '/input/import_related_metadata/osm_file_block.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV;
ANALYZE osm_file_block;

create table osm_file_block_content(
    object_type character(1) NOT NULL,
    blockId bigint NOT NULL,
    h33 smallint NOT NULL,
    count bigint NOT NULL,
    size bigint NOT NULL,
    minId bigint NOT NULL,
    maxId bigint NOT NULL,
    lastModified  bigint NOT NULL
);
copy osm_file_block_content(object_type,blockId,h33,"count","size",minId,maxId,lastModified) from '/input/import_related_metadata/osm_file_block_content.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV;
ANALYZE osm_file_block_content;

create table osm_stat_nodes_3_3 as select h33, count(*) count,sum(size) size from osm_file_block_content where object_type='N' group by 1 order by 1;
ANALYZE osm_stat_nodes_3_3;
create table osm_stat_ways_3_3 as select h33, count(*) count,sum(size) size from osm_file_block_content where object_type='W' group by 1 order by 1;
ANALYZE osm_stat_ways_3_3;


--ALTER TABLE relation_members DISABLE TRIGGER ALL;