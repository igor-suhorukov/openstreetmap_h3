SET application_name = 'psql sql/om_poi_type.sql';
\timing on
BEGIN;
CREATE TABLE om_poi_type (
    om_type text PRIMARY KEY,
    om_categories text[],
    name_en text,
    name_ru text,
    name_th text
);
COPY om_poi_type(om_type, om_categories, name_en, name_ru, name_th) FROM '/input/static/om_poi_type.tsv' DELIMITER E'\t' ESCAPE '\' NULL '\N' CSV;
COMMENT ON TABLE om_poi_type IS 'Organic Maps types treated as POI. Objects of these types have tags is.poi (types) and is.poi_category (categories) as PostgreSQL arrays';
COMMENT ON COLUMN om_poi_type.om_type IS 'Organic Maps type from data/mapcss-mapping.csv, e.g. highway-bus_stop';
COMMENT ON COLUMN om_poi_type.om_categories IS 'Categories displayed in Organic Maps search (data/categories.txt), e.g. category_transport';
COMMENT ON COLUMN om_poi_type.name_en IS 'Type name in English from Organic Maps data/strings/types_strings.txt';
COMMENT ON COLUMN om_poi_type.name_ru IS 'Type name in Russian from Organic Maps data/strings/types_strings.txt';
COMMENT ON COLUMN om_poi_type.name_th IS 'Type name in Thai from Organic Maps data/strings/types_strings.txt';
COMMIT;
