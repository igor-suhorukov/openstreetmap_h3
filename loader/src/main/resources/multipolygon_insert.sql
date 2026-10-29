delete from preimport_multipolygon where osm_type <>'relation';
create table preimport_circle_multipolygon as select id, CASE WHEN ST_MemSize(polygon)<10000 THEN ST_MinimumBoundingRadius(polygon)::text ELSE ST_MinimumBoundingRadius(ST_Simplify(polygon, GREATEST(ST_XMax(ST_Envelope(polygon)) - ST_XMin(ST_Envelope(polygon)),ST_YMax(ST_Envelope(polygon)) - ST_YMin(ST_Envelope(polygon)))::real / 1000, true))::text END boundingRadius from preimport_multipolygon order by 1;
CREATE INDEX idx_preimport_circle_multipolygon_id ON preimport_circle_multipolygon USING btree (id);
insert into multipolygon select
h3_3,
to_short_h3_8(CAST(h3_latlng_to_cell((replace(replace(boundingradius,'(','{'),')','}')::text[])[1]::geometry(Geometry,4326)::point,8) as bigint)) h3_8,
ST_DistanceSpheroid((replace(replace(boundingradius,'(','{'),')','}')::text[])[1]::geometry(Geometry,4326), ST_Point(ST_X((replace(replace(boundingradius,'(','{'),')','}')::text[])[1]::geometry(Geometry,4326)) + (replace(replace(boundingradius,'(','{'),')','}')::text[])[2]::real, ST_Y((replace(replace(boundingradius,'(','{'),')','}')::text[])[1]::geometry(Geometry,4326)), 4326), 'SPHEROID["WGS 84",6378137,298.257223563]') scale,
tags,
ST_Envelope(polygon) as bbox,
p.id,
(replace(replace(boundingradius,'(','{'),')','}')::text[])[1]::geometry(Point,4326) centre,
polygon,
h3_3_multi_regions

from preimport_multipolygon p inner join preimport_circle_multipolygon c on c.id=p.id order by 1;
drop table preimport_circle_multipolygon;
