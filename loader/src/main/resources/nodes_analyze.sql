SET application_name = 'psql sql_finalize/nodes_analyze.sql';
\timing on${nodes_id_index}

SELECT current_setting('server_version_num')::int >= 180000 AS analyze_only \gset
\if :analyze_only
ANALYZE ONLY nodes;
\else
ANALYZE nodes;
\endif
