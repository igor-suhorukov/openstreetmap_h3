SET application_name = 'psql sql_finalize/ways_default_partition.sql';
\timing on

ALTER TABLE  ways ATTACH PARTITION  "ways_32767" DEFAULT;

SELECT current_setting('server_version_num')::int >= 180000 AS analyze_only \gset
\if :analyze_only
ANALYZE ONLY ways;
\else
ANALYZE ways;
\endif
