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
                "-arrow_format", "PARQUET", "-result_in_tsv", "false", "-columnar_storage", "-osmium_docker", "false",
                "-worker_threads", "3", "-pg_script_count", "5", "-data_partition_ratio", "0.5");

        assertEquals("a.osm.pbf", parameters.getSourceFilePath());
        assertTrue(parameters.isScaleApproximation());
        assertTrue(parameters.isCollectOnlyStat());
        assertTrue(parameters.isSkipBuildings());
        assertTrue(parameters.isSkipHighway());
        assertTrue(parameters.isPreserveAllNodes());
        assertEquals(ArrowFormat.PARQUET, parameters.getArrowFormat());
        assertTrue(parameters.isSaveArrow());
        assertFalse(parameters.isSavePostgresqlTsv());
        assertTrue(parameters.isColumnarStorage());
        assertFalse(parameters.isInvokeDockerCommand());
        assertEquals(3, parameters.getWorkers());
        assertEquals(5, parameters.getScriptCount());
        assertEquals(0.5, parameters.getThresholdPercentFromMaxPartition());
        assertFalse(parameters.isHelp());
        String text = parameters.toString();
        assertTrue(text.contains("\"sourceFilePath\":\"a.osm.pbf\""), text);
        assertTrue(text.contains("\"workers\":3"), text);
        assertTrue(text.contains("\"arrowFormat\":\"PARQUET\""), text);
    }

    @Test
    void parse_defaults() {
        CliParameters parameters = new CliParameters();
        JCommander.newBuilder().addObject(parameters).build().parse("-source_pbf", "a.osm.pbf");

        assertTrue(parameters.isSavePostgresqlTsv());
        assertTrue(parameters.isInvokeDockerCommand());
        assertFalse(parameters.isSaveArrow());
        assertNull(parameters.getArrowFormat());
        assertEquals(4, parameters.getWorkers());
        assertEquals(4, parameters.getScriptCount());
        assertEquals(0.48, parameters.getThresholdPercentFromMaxPartition());
    }
}
