create table h3_8_by_partitions as select h3_3,h3_8 from  (select distinct h3_3,h3_8 from ways union select h3_3,h3_8 from nodes union select h3_3,h3_8 from multipolygon) a;

create table h3_8_ring1 as select h3_3,h3_8,array_agg(ring1) ring1 from (select h3_3,h3_8, to_short_h3_8(h3_grid_disk((to_full_short_h3_8(h3_8)::h3index),1)::bigint) ring1 from h3_8_by_partitions) b group by h3_3,h3_8 order by 1,2;
CREATE INDEX idx_h3_8_ring1 ON h3_8_ring1 USING btree (h3_8);

create table h3_8_ring2 as select h3_3,h3_8,array_agg(ring1) ring2 from (select h3_3,h3_8, to_short_h3_8(h3_grid_disk((to_full_short_h3_8(h3_8)::h3index),2)::bigint) ring1 from h3_8_by_partitions) b group by h3_3,h3_8 order by 1,2;
CREATE INDEX idx_h3_8_ring2 ON h3_8_ring2 USING btree (h3_8);

create table h3_8_ring3 as select h3_3,h3_8,array_agg(ring1) ring3 from (select h3_3,h3_8, to_short_h3_8(h3_grid_disk((to_full_short_h3_8(h3_8)::h3index),3)::bigint) ring1 from h3_8_by_partitions) b group by h3_3,h3_8 order by 1,2;
CREATE INDEX idx_h3_8_ring3 ON h3_8_ring3 USING btree (h3_8);

create table h3_8_ring4 as select h3_3,h3_8,array_agg(ring1) ring4 from (select h3_3,h3_8, to_short_h3_8(h3_grid_disk((to_full_short_h3_8(h3_8)::h3index),4)::bigint) ring1 from h3_8_by_partitions) b group by h3_3,h3_8 order by 1,2;
CREATE INDEX idx_h3_8_ring4 ON h3_8_ring4 USING btree (h3_8);

create table h3_8_ring5 as select h3_3,h3_8,array_agg(ring1) ring5 from (select h3_3,h3_8, to_short_h3_8(h3_grid_disk((to_full_short_h3_8(h3_8)::h3index),5)::bigint) ring1 from h3_8_by_partitions) b group by h3_3,h3_8 order by 1,2;
CREATE INDEX idx_h3_8_ring5 ON h3_8_ring5 USING btree (h3_8);

drop table h3_8_by_partitions;