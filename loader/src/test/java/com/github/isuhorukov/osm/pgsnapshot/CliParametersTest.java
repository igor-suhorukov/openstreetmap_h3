package com.github.isuhorukov.osm.pgsnapshot;

import com.beust.jcommander.JCommander;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CliParametersTest {

    @Test
    void parse_allOptions_gettersAndToString() {
        CliParameters parameters = new CliParameters();
        JCommander.newBuilder().addObject(parameters).build().parse("-source_pbf", "a.osm.pbf", "-scale_approx_calc",
                "-collect_only_statistics", "-skip_buildings", "-skip_highway", "-preserve_all_nodes",
                "-arrow_format", "PARQUET", "-geoparquet_version", "BOTH", "-result_in_tsv", "false", "-columnar_storage",
                "-osmium_docker", "false",
                "-worker_threads", "3", "-pg_script_count", "5", "-data_partition_ratio", "0.5",
                "-parallel_multipolygon_export", "false");

        assertEquals("a.osm.pbf", parameters.getSourceFilePath());
        assertTrue(parameters.isScaleApproximation());
        assertTrue(parameters.isCollectOnlyStat());
        assertTrue(parameters.isSkipBuildings());
        assertTrue(parameters.isSkipHighway());
        assertTrue(parameters.isPreserveAllNodes());
        assertEquals(ArrowFormat.PARQUET, parameters.getArrowFormat());
        assertEquals(GeoParquetVersion.BOTH, parameters.getGeoParquetVersion());
        assertTrue(parameters.isGeoParquetVersionSet());
        assertTrue(parameters.isSaveArrow());
        assertFalse(parameters.isSavePostgresqlTsv());
        assertTrue(parameters.isColumnarStorage());
        assertFalse(parameters.isInvokeDockerCommand());
        assertEquals(3, parameters.getWorkers());
        assertEquals(5, parameters.getScriptCount());
        assertEquals(0.5, parameters.getThresholdPercentFromMaxPartition());
        assertFalse(parameters.isParallelMultipolygonExport());
        assertFalse(parameters.isHelp());
        String text = parameters.toString();
        assertTrue(text.contains("\"sourceFilePath\":\"a.osm.pbf\""), text);
        assertTrue(text.contains("\"workers\":3"), text);
        assertTrue(text.contains("\"arrowFormat\":\"PARQUET\""), text);
        assertTrue(text.contains("\"geoParquetVersion\":\"BOTH\""), text);
        assertTrue(text.contains("\"parallelMultipolygonExport\":false"), text);
    }

    @Test
    void parse_spillOptions_gettersAndToString() {
        CliParameters parameters = new CliParameters();
        JCommander.newBuilder().addObject(parameters).build().parse("-source_pbf", "a.osm.pbf", "-arrow_format",
                "PARQUET", "-spill_compression", "ZSTD", "-spill_zstd_level", "7");

        assertEquals(SpillCompression.ZSTD, parameters.getSpillCompression());
        assertTrue(parameters.isSpillCompressionSet());
        assertEquals(7, parameters.getSpillZstdLevel());
        assertTrue(parameters.isSpillZstdLevelSet());
        String text = parameters.toString();
        assertTrue(text.contains("\"spillCompression\":\"ZSTD\",\"spillZstdLevel\":7"), text);
    }

    @Test
    void parse_noSpillOptions_zstdLevelOneByDefault() {
        CliParameters parameters = new CliParameters();
        JCommander.newBuilder().addObject(parameters).build().parse("-source_pbf", "a.osm.pbf", "-arrow_format",
                "PARQUET");

        assertEquals(SpillCompression.ZSTD, parameters.getSpillCompression());
        assertFalse(parameters.isSpillCompressionSet());
        assertEquals(CliParameters.DEFAULT_SPILL_ZSTD_LEVEL, parameters.getSpillZstdLevel());
        assertEquals(1, parameters.getSpillZstdLevel());
        assertFalse(parameters.isSpillZstdLevelSet());
    }

    @Test
    void parse_pgJdbcUrl_postgresqlRowsWithoutTsvFilesAndMaskedPassword() {
        CliParameters parameters = new CliParameters();
        JCommander.newBuilder().addObject(parameters).build().parse("-source_pbf", "a.osm.pbf", "-result_in_tsv", "false",
                "-pg_jdbc_url", "jdbc:postgresql://localhost:5432/osmworld?user=postgres&password=secret&ssl=false");

        assertTrue(parameters.isLoadPostgresqlJdbc());
        assertTrue(parameters.isSavePostgresqlTsv());
        assertEquals("jdbc:postgresql://localhost:5432/osmworld?user=postgres&password=secret&ssl=false",
                parameters.getPostgresqlJdbcUrl());
        String text = parameters.toString();
        assertTrue(text.contains("password=***&ssl=false"), text);
        assertFalse(text.contains("secret"), text);
    }

    @Test
    void parse_updatableAndUpdate_bothEnableVersionColumns() {
        CliParameters updatable = new CliParameters();
        JCommander.newBuilder().addObject(updatable).build().parse("-source_pbf", "a.osm.pbf", "-updatable");
        CliParameters update = new CliParameters();
        JCommander.newBuilder().addObject(update).build().parse("-source_pbf", "a.osm.pbf", "-update",
                "-pg_jdbc_url", "jdbc:postgresql://localhost/osmworld");

        assertTrue(updatable.isUpdatable());
        assertFalse(updatable.isUpdate());
        assertTrue(update.isUpdatable());
        assertTrue(update.isUpdate());
        assertTrue(updatable.toString().contains("\"updatable\":true,\"update\":false"), updatable.toString());
        assertTrue(update.toString().contains("\"updatable\":false,\"update\":true"), update.toString());
    }

    @Test
    void maskPassword_caseInsensitiveParameter() {
        assertNull(CliParameters.maskPassword(null));
        assertEquals("jdbc:postgresql://h/db?PASSWORD=***", CliParameters.maskPassword("jdbc:postgresql://h/db?PASSWORD=x"));
        assertEquals("jdbc:postgresql://h/db?user=u", CliParameters.maskPassword("jdbc:postgresql://h/db?user=u"));
    }

    @Test
    void parse_defaults() {
        CliParameters parameters = new CliParameters();
        JCommander.newBuilder().addObject(parameters).build().parse("-source_pbf", "a.osm.pbf");

        assertTrue(parameters.isSavePostgresqlTsv());
        assertTrue(parameters.isInvokeDockerCommand());
        assertFalse(parameters.isSaveArrow());
        assertNull(parameters.getArrowFormat());
        assertEquals(GeoParquetVersion.V1, parameters.getGeoParquetVersion());
        assertFalse(parameters.isGeoParquetVersionSet());
        assertEquals(4, parameters.getWorkers());
        assertEquals(4, parameters.getScriptCount());
        assertEquals(0.48, parameters.getThresholdPercentFromMaxPartition());
        assertTrue(parameters.isParallelMultipolygonExport());
        assertFalse(parameters.isLoadPostgresqlJdbc());
        assertNull(parameters.getPostgresqlJdbcUrl());
        assertFalse(parameters.isUpdatable());
        assertFalse(parameters.isUpdate());
    }
}
