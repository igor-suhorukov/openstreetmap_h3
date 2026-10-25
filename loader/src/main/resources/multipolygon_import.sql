SET application_name = 'psql sql_finalize/multipolygon_import.sql';
\i /input/static/multipolygon.sql
ANALYZE multipolygon;
