package com.github.isuhorukov.osm.pgsnapshot;

import com.beust.jcommander.JCommander;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminColumns;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminOptions;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminUnits;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdminUnitsOptionsTest {

    private static CliParameters parse(String options) {
        CliParameters parameters = new CliParameters();
        String arguments = "-source_pbf a.osm.pbf " + options;
        JCommander.newBuilder().addObject(parameters).build().parse(arguments.trim().split(" "));
        return parameters;
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "-admin_unit_shares|require -admin_units",
            "-admin_units_cache /tmp/c|require -admin_units",
            "-name_lang en|require -admin_units",
            "-min_area_m2 5|require -admin_units",
            "-admin_units -update -pg_jdbc_url jdbc:postgresql://h/d|not with -update yet",
            "-admin_units -admin_units_only -admin_units_export /tmp/e -pg_jdbc_url jdbc:postgresql://h/d|"
                    + "-admin_units_only can't be combined with -pg_jdbc_url",
            "-admin_units -updatable|can't be combined with -updatable",
            "-admin_units -collect_only_statistics|can't be combined with -collect_only_statistics",
            "-admin_units -admin_units_only|-admin_units_only requires -admin_units_export",
            "-admin_units -name_lang ,|-name_lang must list at least one language",
            "-admin_units -parent_share 0|-parent_share and -subarea_share",
            "-admin_units -subarea_share 1.5|-parent_share and -subarea_share",
            "-admin_units -min_share 2|-min_share must be between 0 and 1",
            "-admin_units -min_length_m -1|must not be negative"
    })
    void optionsError_rejectsInvalidAdminUnitOptions(String options, String message) {
        String error = OsmPbfTransformation.optionsError(parse(options));
        assertTrue(error != null && error.contains(message), error);
    }

    @Test
    void optionsError_acceptsValidAdminUnitOptions() {
        assertNull(OsmPbfTransformation.optionsError(parse("")));
        assertNull(OsmPbfTransformation.optionsError(parse("-admin_units -admin_unit_shares -arrow_format PARQUET "
                + "-admin_units_export /tmp/e -admin_units_only -admin_units_cache /tmp/c -name_lang th,en "
                + "-parent_share 0.8 -subarea_share 0.6 -min_share 0 -min_area_m2 0 -min_length_m 50")));
        assertNull(OsmPbfTransformation.optionsError(parse("-admin_units -admin_unit_shares "
                + "-pg_jdbc_url jdbc:postgresql://h/d -admin_units_export /tmp/e -admin_units_cache /tmp/c")));
    }

    @Test
    void parse_adminUnitOptions_gettersOptionsAndToString() {
        CliParameters parameters = parse("-admin_units -admin_unit_shares -admin_units_export /tmp/e "
                + "-admin_units_cache /tmp/c -name_lang th,,en -parent_share 0.8 -subarea_share 0.6 -min_share 0.02 "
                + "-min_area_m2 500 -min_length_m 50");
        assertTrue(parameters.isAdminUnits());
        assertTrue(parameters.isAdminUnitsOptionSet());
        assertFalse(parameters.isAdminUnitsOnly());
        assertEquals("/tmp/e", parameters.getAdminUnitsExport());
        assertEquals("/tmp/c", parameters.getAdminUnitsCache());
        AdminOptions options = AdminUnits.options(parameters);
        assertEquals(List.of("th", "en"), options.getNameLang());
        assertEquals(0.8, options.getParentShare());
        assertEquals(0.6, options.getSubareaShare());
        assertEquals(0.02, options.getMinShare());
        assertEquals(500, options.getMinAreaM2());
        assertEquals(50, options.getMinLengthM());
        assertEquals(AdminColumns.UNITS_AND_SHARES, options.getColumns());
        assertTrue(parameters.toString().contains("\"adminUnits\":true,\"adminUnitShares\":true"));
    }

    @Test
    void parse_defaults_adminUnitsOff() {
        CliParameters parameters = parse("");
        assertFalse(parameters.isAdminUnits());
        assertFalse(parameters.isAdminUnitsOptionSet());
        AdminOptions options = AdminUnits.options(parameters);
        assertEquals(AdminOptions.DEFAULT_NAME_LANG, options.getNameLang());
        assertEquals(AdminColumns.NONE, options.getColumns());
        assertEquals(0.9, options.getParentShare());
        assertEquals(10000, options.getMinAreaM2());
    }
}
