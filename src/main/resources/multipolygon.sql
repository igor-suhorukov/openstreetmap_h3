\c osmworld
\timing on

create table multipolygon_32767 PARTITION OF multipolygon (check (h3_3=32767)) DEFAULT ${substitute};
CREATE EXTENSION IF NOT EXISTS h3;

delete from preimport_multipolygon where osm_type <>'relation';
create table preimport_multipolygon_relations_h3_3 as select m.id, array_agg(distinct h.id) as h3_3 from preimport_multipolygon m left join h3_3_bounds_complex h on st_intersects(h.bounds,m.polygon) group by 1 order by 1;
create table preimport_circle_multipolygon as select id, CASE WHEN ST_MemSize(polygon)<10000 THEN ST_MinimumBoundingRadius(polygon)::text ELSE ST_MinimumBoundingRadius(ST_Simplify(polygon, GREATEST(ST_XMax(ST_Envelope(polygon)) - ST_XMin(ST_Envelope(polygon)),ST_YMax(ST_Envelope(polygon)) - ST_YMin(ST_Envelope(polygon)))::real / 1000, true))::text END boundingRadius from preimport_multipolygon order by 1;
CREATE INDEX idx_preimport_multipolygon_relations_id ON preimport_multipolygon_relations_h3_3 USING btree (id);
CREATE INDEX idx_preimport_circle_multipolygon_id ON preimport_circle_multipolygon USING btree (id);
insert into multipolygon select
CASE WHEN cardinality(h3_3)=1 THEN h3_3[1] ELSE 32767 END h3_3,
to_short_h3_8(CAST(h3_lat_lng_to_cell((replace(replace(boundingradius,'(','{'),')','}')::text[])[1]::geometry(Geometry,4326)::point,8) as bigint)) h3_8,
ST_DistanceSpheroid((replace(replace(boundingradius,'(','{'),')','}')::text[])[1]::geometry(Geometry,4326), ST_Point(ST_X((replace(replace(boundingradius,'(','{'),')','}')::text[])[1]::geometry(Geometry,4326)) + (replace(replace(boundingradius,'(','{'),')','}')::text[])[2]::real, ST_Y((replace(replace(boundingradius,'(','{'),')','}')::text[])[1]::geometry(Geometry,4326)), 4326), 'SPHEROID["WGS 84",6378137,298.257223563]') scale,
tags,
ST_Envelope(polygon) as bbox,
p.id,
(replace(replace(boundingradius,'(','{'),')','}')::text[])[1]::geometry(Point,4326) center,
polygon,
CASE WHEN cardinality(h3_3)>1 THEN h3_3 ELSE null END h3_3_multi_regions

from preimport_multipolygon p left join preimport_multipolygon_relations_h3_3 h on p.id=h.id inner join preimport_circle_multipolygon c on c.id=h.id order by 1;

CREATE INDEX idx_multipolygon_id_h3_3 ON multipolygon using btree(id,h3_3);
CREATE INDEX idx_multipolygon_scale ON multipolygon using btree(scale);
CREATE INDEX idx_multipolygon_polygon ON multipolygon USING gist (polygon);
drop table preimport_multipolygon_relations_h3_3;
drop table preimport_circle_multipolygon;
drop table preimport_multipolygon;