SET application_name = 'psql sql/ways_32767_import.sql';
\timing on

BEGIN;
CREATE TABLE "preimport_ways_32767"  (like ways);
COPY "preimport_ways_32767"(h3_3,h3_8, id,closed,building,highway,scale,centre,bbox,linestring,points,h3_8_regions,tags${way_version_columns}) FROM '/input/ways/32767.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV FREEZE;

create table preimport_ways_large_h3_3 as select id, array_agg(h3_3 ORDER BY h3_3) h3_3_multi_regions from (select distinct wl.id, h3b.id h3_3 from "preimport_ways_32767"  wl inner join h3_3_bounds_complex h3b on ST_Intersects(wl.linestring, h3b.bounds)) a group by id;
create table ways_32767 as select  closed,building,highway,h3_3,h3_8, scale,tags,bbox,centre,p.id,linestring,points,h3_8_regions, CASE WHEN cardinality(h3.h3_3_multi_regions)>1 THEN h3.h3_3_multi_regions ELSE ARRAY(select distinct unnest((tags->'is.h33idxs')::smallint[]) order by 1)::smallint[]  END h3_3_multi_regions${way_version_columns}  from preimport_ways_32767 p inner join preimport_ways_large_h3_3 h3 on h3.id=p.id order by p.id;

alter table ways_32767 alter COLUMN id set NOT NULL;
alter table ways_32767 alter COLUMN h3_8 set NOT NULL;
alter table ways_32767 alter COLUMN scale set NOT NULL;
alter table ways_32767 alter COLUMN closed set NOT NULL;
alter table ways_32767 alter COLUMN building set NOT NULL;
alter table ways_32767 alter COLUMN highway set NOT NULL;
alter table ways_32767 alter COLUMN bbox set NOT NULL;
alter table ways_32767 alter COLUMN centre set NOT NULL;
alter table ways_32767 alter COLUMN linestring set NOT NULL;
alter table ways_32767 alter COLUMN points set NOT NULL;${way_version_not_null}

drop table preimport_ways_large_h3_3;
drop table preimport_ways_32767;
${substitute}
COMMIT;
ANALYZE ways_32767;
