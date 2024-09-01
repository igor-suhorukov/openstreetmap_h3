package com.github.isuhorukov.osm.pgsnapshot;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class CliParameters {
    @Parameter(names = { "-source_pbf" }, required = true,order = 1,description = "Source path for OpenStreetMap data in PBF format")
    String sourceFilePath;
    @Parameter(names = {"-scale_approx_calc"}, description = "Approximate scale calculation. Value 'false' - distance in meter")
    boolean scaleApproximation = false;
    @Parameter(names = {"-collect_only_statistics"}, description = "Collect only statistics from data - partition distribution")
    boolean collectOnlyStat = false;
    @Parameter(names = {"-skip_buildings"}, description = "Skip any ways with 'building' tag")
    boolean skipBuildings = false;
    @Parameter(names = {"-skip_highway"}, description = "Skip any ways with 'highway' tag")
    boolean skipHighway = false;
    @Parameter(names = {"-preserve_all_nodes"}, description = "Preserve all nodes information in case of 'true' or only nodes with tags or referenced from relations in other case")
    boolean preserveAllNodes = false;
    @Parameter(names = {"-arrow_format"}, description = "In case of not null parameter save data in Arrow serialization: ARROW_IPC or PARQUET")
    ArrowFormat arrowFormat;
    @Parameter(names = {"-result_in_tsv"}, arity = 1, description = "Save result data in TabSeparatedValue format for PostgreSQL COPY")
    boolean savePostgresqlTsv = true;
    @Parameter(names = {"-columnar_storage"}, description = "Use columnar storage in PostgreSql tables for nodes/ways/multipolygon")
    boolean columnarStorage = false;
    @Parameter(names = {"-osmium_docker"}, arity = 1, description = "Run osmium tool from docker image")
    boolean invokeDockerCommand = true;
    @Parameter(names = {"-worker_threads"}, description = "Worker threads count for data processing")
    int workers=4;
    @Parameter(names = {"-pg_script_count"}, description = "Script count for PostgreSQL parallel COPY")
    int scriptCount  = 4;
    @Parameter(names = {"-data_partition_ratio"}, description = "Filling ratio from maximum size of partition. This parameter change PostgreSQL partitions count")
    double thresholdPercentFromMaxPartition = 0.48;
    @Parameter(names = "-help", help = true, description = "Information about command line parameters")
    boolean help;

    public boolean isSaveArrow() {
        return arrowFormat != null;
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
                .append("savePostgresqlTsv", savePostgresqlTsv)
                .append("columnarStorage", columnarStorage)
                .append("invokeDockerCommand", invokeDockerCommand)
                .append("workers", workers)
                .append("scriptCount", scriptCount)
                .append("thresholdPercentFromMaxPartition", thresholdPercentFromMaxPartition)
                .toString();
    }
}
