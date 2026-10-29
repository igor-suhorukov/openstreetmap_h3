package com.github.isuhorukov.osm.pgsnapshot;

import com.beust.jcommander.Parameter;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminOptions;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.postgresql.PGProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class CliParameters {
    public static final int DEFAULT_SPILL_ZSTD_LEVEL = 1;
    private static final Pattern JDBC_URL_SECRET = Pattern.compile(
            "(?i)([?&]" + Pattern.quote(PGProperty.PASSWORD.getName()) + "=)[^&]*");

    @Parameter(names = { "-source_pbf" }, required = true,order = 1,description = "Source path for OpenStreetMap data in PBF format")
    private String sourceFilePath;
    @Parameter(names = {"-scale_approx_calc"}, description = "Approximate scale calculation. Value 'false' - distance in meter")
    private boolean scaleApproximation = false;
    @Parameter(names = {"-collect_only_statistics"}, description = "Collect only statistics from data - partition distribution")
    private boolean collectOnlyStat = false;
    @Parameter(names = {"-skip_buildings"}, description = "Skip any ways with 'building' tag")
    private boolean skipBuildings = false;
    @Parameter(names = {"-skip_highway"}, description = "Skip any ways with 'highway' tag")
    private boolean skipHighway = false;
    @Parameter(names = {"-preserve_all_nodes"}, description = "Preserve all nodes information in case of 'true' or only nodes with tags or referenced from relations in other case")
    private boolean preserveAllNodes = false;
    @Parameter(names = {"-arrow_format"}, description = "In case of not null parameter save data in Arrow serialization: ARROW_IPC or PARQUET")
    private ArrowFormat arrowFormat;
    @Parameter(names = {"-geoparquet_version"}, description = "Way line geometry in Parquet files: V1 - WKB with GeoParquet 1.0.0 metadata (default), BOTH - GeoParquet 1.0.0 metadata and the Parquet GEOMETRY type, V2 - GeoParquet 2.0.0 metadata and the Parquet GEOMETRY type, BLOB - WKB without GeoParquet metadata. Requires -arrow_format PARQUET")
    private GeoParquetVersion geoParquetVersion;
    @Parameter(names = {"-parquet_compression"}, description = "Compression of Parquet files: ZSTD (default), LZ4 (written as the Parquet LZ4_RAW codec) or SNAPPY. Requires -arrow_format PARQUET")
    private ParquetCompression parquetCompression;
    @Parameter(names = {"-arrow_ipc_compression"}, description = "Compression of Arrow IPC files: NONE (default) or ZSTD. Readers need ZSTD support of the Arrow IPC format, e.g. arrow-compression in Arrow Java. Requires -arrow_format ARROW_IPC")
    private IpcCompression arrowIpcCompression;
    @Parameter(names = {"-spill_compression"}, description = "Compression of the intermediate Arrow IPC files that collect rows per H3 cell before partition files are written: ZSTD (default) or NONE. NONE is a little faster and takes about twice the disk space. Requires -arrow_format")
    private IpcCompression spillCompression;
    @Parameter(names = {"-spill_zstd_level"}, description = "ZSTD level of the intermediate Arrow IPC files, from -131072 (fastest) to 22 (smallest), default 1. Requires -spill_compression ZSTD")
    private Integer spillZstdLevel;
    @Parameter(names = {"-result_in_tsv"}, arity = 1, description = "Save result data in TabSeparatedValue format for PostgreSQL COPY")
    private boolean savePostgresqlTsv = true;
    @Parameter(names = {"-columnar_storage"}, description = "Use columnar storage in PostgreSql tables for nodes/ways/multipolygon")
    private boolean columnarStorage = false;
    @Parameter(names = {"-osmium_docker"}, arity = 1, description = "Run osmium tool from docker image")
    private boolean invokeDockerCommand = true;
    @Parameter(names = {"-worker_threads"}, description = "Worker threads count for data processing")
    private int workers=4;
    @Parameter(names = {"-pg_script_count"}, description = "Script count for PostgreSQL parallel COPY, or COPY connection count with -pg_jdbc_url")
    private int scriptCount  = 4;
    @Parameter(names = {"-pg_jdbc_url"}, description = "Load data straight into PostgreSQL by JDBC COPY streams without TSV files and import scripts, e.g. jdbc:postgresql://localhost:5432/osmworld?user=postgres. The database must exist. The password is taken from the URL, ~/.pgpass or the PGPASSWORD environment variable")
    private String postgresqlJdbcUrl;
    @Parameter(names = {"-data_partition_ratio"}, description = "Filling ratio from maximum size of partition. This parameter change PostgreSQL partitions count")
    private double thresholdPercentFromMaxPartition = 0.48;
    @Parameter(names = {"-parallel_multipolygon_export"}, arity = 1, description = "Run osmium multipolygon export concurrently with data processing. Use 'false' when memory is not enough for the second osmium node location index")
    private boolean parallelMultipolygonExport = true;
    @Parameter(names = {"-updatable"}, description = "Store object versions, line geometry hashes, id indexes and the import journal, so the database can be updated later with -update")
    private boolean updatable = false;
    @Parameter(names = {"-update"}, description = "Update a database loaded with -updatable from a newer PBF: only new, changed and deleted objects are written. Requires -pg_jdbc_url")
    private boolean update = false;
    @Parameter(names = {"-admin_units"}, description = "Build the directory of administrative units (boundary=administrative relations with admin_level) from the PBF and write the column admin_units bigint[] with the ids of the leaf units of buildings, POIs, address points, stops and platforms, parking, toilets, ATMs, fuel stations, protected areas and places. Works with TSV files and -arrow_format")
    private boolean adminUnits = false;
    @Parameter(names = {"-admin_unit_shares"}, description = "Also write the column admin_unit_shares real[] with the shares of the object in the units of admin_units. Requires -admin_units")
    private boolean adminUnitShares = false;
    @Parameter(names = {"-admin_units_export"}, description = "Directory for the admin unit directory without other data: admin_units.jsonl, admin_units.geojsonl, admin_regions.geojsonl, admin_units.parquet, admin_regions.parquet and admin_units_state.json. Requires -admin_units")
    private String adminUnitsExport;
    @Parameter(names = {"-admin_units_only"}, description = "Only build and export the admin unit directory, without nodes, ways and multipolygons. Requires -admin_units and -admin_units_export")
    private boolean adminUnitsOnly = false;
    @Parameter(names = {"-admin_units_cache"}, description = "Directory that keeps the built admin unit directory between loads; it is reused while the boundaries and the directory parameters stay the same. Requires -admin_units")
    private String adminUnitsCache;
    @Parameter(names = {"-name_lang"}, description = "Comma separated order of languages for the admin unit name: name:<language> for each language, 'name' for the plain name tag; default name,en. Requires -admin_units")
    private String nameLang;
    @Parameter(names = {"-parent_share"}, description = "Minimal share of the unit area inside its geometric parent, default 0.9. Requires -admin_units")
    private Double parentShare;
    @Parameter(names = {"-subarea_share"}, description = "Minimal share of the unit area inside the parent of its subarea link, default 0.5. Requires -admin_units")
    private Double subareaShare;
    @Parameter(names = {"-min_share"}, description = "A unit is kept for an area or line if the object share in it is at least this value (default 0.01) or the piece is at least -min_area_m2 or -min_length_m. Requires -admin_units")
    private Double minShare;
    @Parameter(names = {"-min_area_m2"}, description = "Minimal area of an object piece in a unit, square meters, default 10000. Requires -admin_units")
    private Double minAreaM2;
    @Parameter(names = {"-min_length_m"}, description = "Minimal length of a line piece in a unit, meters, default 100. Requires -admin_units")
    private Double minLengthM;
    @Parameter(names = {"-help", "--help"}, help = true, description = "Information about command line parameters")
    private boolean help;

    public String getSourceFilePath() { return sourceFilePath; }
    public boolean isScaleApproximation() { return scaleApproximation; }
    public boolean isCollectOnlyStat() { return collectOnlyStat; }
    public boolean isSkipBuildings() { return skipBuildings; }
    public boolean isSkipHighway() { return skipHighway; }
    public boolean isPreserveAllNodes() { return preserveAllNodes; }
    public ArrowFormat getArrowFormat() { return arrowFormat; }
    public GeoParquetVersion getGeoParquetVersion() { return geoParquetVersion != null ? geoParquetVersion : GeoParquetVersion.V1; }
    public boolean isGeoParquetVersionSet() { return geoParquetVersion != null; }
    public ParquetCompression getParquetCompression() { return parquetCompression != null ? parquetCompression : ParquetCompression.ZSTD; }
    public boolean isParquetCompressionSet() { return parquetCompression != null; }
    public IpcCompression getArrowIpcCompression() { return arrowIpcCompression != null ? arrowIpcCompression : IpcCompression.NONE; }
    public boolean isArrowIpcCompressionSet() { return arrowIpcCompression != null; }
    public IpcCompression getSpillCompression() { return spillCompression != null ? spillCompression : IpcCompression.ZSTD; }
    public boolean isSpillCompressionSet() { return spillCompression != null; }
    public int getSpillZstdLevel() { return spillZstdLevel != null ? spillZstdLevel : DEFAULT_SPILL_ZSTD_LEVEL; }
    public boolean isSpillZstdLevelSet() { return spillZstdLevel != null; }
    public boolean isSavePostgresqlTsv() { return savePostgresqlTsv || isLoadPostgresqlJdbc(); }
    public boolean isColumnarStorage() { return columnarStorage; }
    public boolean isInvokeDockerCommand() { return invokeDockerCommand; }
    public int getWorkers() { return workers; }
    public int getScriptCount() { return scriptCount; }
    public double getThresholdPercentFromMaxPartition() { return thresholdPercentFromMaxPartition; }
    public boolean isParallelMultipolygonExport() { return parallelMultipolygonExport; }
    public String getPostgresqlJdbcUrl() { return postgresqlJdbcUrl; }
    public boolean isUpdate() { return update; }
    public boolean isHelp() { return help; }
    public boolean isAdminUnits() { return adminUnits; }
    public boolean isAdminUnitShares() { return adminUnitShares; }
    public String getAdminUnitsExport() { return adminUnitsExport; }
    public boolean isAdminUnitsOnly() { return adminUnitsOnly; }
    public String getAdminUnitsCache() { return adminUnitsCache; }
    public List<String> getNameLang() {
        if (nameLang == null) {
            return AdminOptions.DEFAULT_NAME_LANG;
        }
        List<String> languages = new ArrayList<>();
        for (String language : nameLang.split(",")) {
            if (!language.trim().isEmpty()) {
                languages.add(language.trim());
            }
        }
        return languages;
    }
    public double getParentShare() { return parentShare != null ? parentShare : AdminOptions.DEFAULT_PARENT_SHARE; }
    public double getSubareaShare() { return subareaShare != null ? subareaShare : AdminOptions.DEFAULT_SUBAREA_SHARE; }
    public double getMinShare() { return minShare != null ? minShare : AdminOptions.DEFAULT_MIN_SHARE; }
    public double getMinAreaM2() { return minAreaM2 != null ? minAreaM2 : AdminOptions.DEFAULT_MIN_AREA_M2; }
    public double getMinLengthM() { return minLengthM != null ? minLengthM : AdminOptions.DEFAULT_MIN_LENGTH_M; }

    public boolean isAdminUnitsOptionSet() {
        return adminUnitShares || adminUnitsExport != null || adminUnitsOnly || adminUnitsCache != null
                || nameLang != null || parentShare != null || subareaShare != null || minShare != null
                || minAreaM2 != null || minLengthM != null;
    }

    public boolean isSaveArrow() {
        return arrowFormat != null;
    }

    public boolean isLoadPostgresqlJdbc() {
        return postgresqlJdbcUrl != null;
    }

    public boolean isUpdatable() {
        return updatable || update;
    }

    public static String maskPassword(String jdbcUrl) {
        return jdbcUrl == null ? null : JDBC_URL_SECRET.matcher(jdbcUrl).replaceAll("$1***");
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("sourceFilePath", sourceFilePath)
                .append("scaleApproximation", scaleApproximation)
                .append("collectOnlyStat", collectOnlyStat)
                .append("skipBuildings", skipBuildings)
                .append("skipHighway", skipHighway)
                .append("preserveAllNodes", preserveAllNodes)
                .append("arrowFormat", arrowFormat)
                .append("geoParquetVersion", geoParquetVersion)
                .append("parquetCompression", parquetCompression)
                .append("arrowIpcCompression", arrowIpcCompression)
                .append("spillCompression", spillCompression)
                .append("spillZstdLevel", spillZstdLevel)
                .append("savePostgresqlTsv", savePostgresqlTsv)
                .append("columnarStorage", columnarStorage)
                .append("invokeDockerCommand", invokeDockerCommand)
                .append("workers", workers)
                .append("scriptCount", scriptCount)
                .append("thresholdPercentFromMaxPartition", thresholdPercentFromMaxPartition)
                .append("parallelMultipolygonExport", parallelMultipolygonExport)
                .append("postgresqlJdbcUrl", maskPassword(postgresqlJdbcUrl))
                .append("updatable", updatable)
                .append("update", update)
                .append("adminUnits", adminUnits)
                .append("adminUnitShares", adminUnitShares)
                .append("adminUnitsExport", adminUnitsExport)
                .append("adminUnitsOnly", adminUnitsOnly)
                .append("adminUnitsCache", adminUnitsCache)
                .append("nameLang", nameLang)
                .append("parentShare", parentShare)
                .append("subareaShare", subareaShare)
                .append("minShare", minShare)
                .append("minAreaM2", minAreaM2)
                .append("minLengthM", minLengthM)
                .toString();
    }
}
