package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

public final class AdminExport {

    public static final String UNITS_JSONL = "admin_units.jsonl";
    public static final String UNITS_GEOJSONL = "admin_units.geojsonl";
    public static final String REGIONS_GEOJSONL = "admin_regions.geojsonl";
    public static final String STATE_JSON = "admin_units_state.json";
    private static final int BUFFER_SIZE = 1 << 20;
    private static final int COORDINATE_SCALE = 7;

    private AdminExport() {
    }

    public static void write(AdminUnits adminUnits, File directory, String importStateJson, String importStateKey)
            throws IOException {
        Files.createDirectories(directory.toPath());
        writeUnits(adminUnits, directory);
        writeRegions(adminUnits, directory);
        writeState(adminUnits, directory, importStateJson, importStateKey);
    }

    public static void writeUnits(AdminUnits adminUnits, File directory) throws IOException {
        AdminDirectory adminDirectory = adminUnits.getDirectory();
        List<AdminUnit> units = adminDirectory.getUnits();
        try (Writer jsonl = writer(new File(directory, UNITS_JSONL));
             Writer geojsonl = writer(new File(directory, UNITS_GEOJSONL))) {
            for (AdminUnit unit : units) {
                StringBuilder properties = properties(adminDirectory, unit);
                jsonl.append(properties).append('\n');
                if (unit.hasGeometry()) {
                    StringBuilder feature = new StringBuilder("{\"type\":\"Feature\",\"properties\":")
                            .append(properties).append(",\"geometry\":");
                    geoJson(feature, DirectoryBuilder.geometry(unit));
                    geojsonl.append(feature).append("}\n");
                }
            }
        }
    }

    public static void writeRegions(AdminUnits adminUnits, File directory) throws IOException {
        AdminDirectory adminDirectory = adminUnits.getDirectory();
        int[] partNumbers = adminDirectory.regionPartNumbers();
        try (Writer regions = writer(new File(directory, REGIONS_GEOJSONL))) {
            for (int region = 0; region < partNumbers.length; region++) {
                AdminUnit unit = adminDirectory.regionUnit(region);
                StringBuilder feature = new StringBuilder("{\"type\":\"Feature\",\"properties\":{\"unit_id\":")
                        .append(unit.getId()).append(",\"part_no\":").append(partNumbers[region]).append(",\"admin_level\":")
                        .append(unit.getLevel()).append("},\"geometry\":");
                geoJson(feature, adminDirectory.region(region));
                regions.append(feature).append("}\n");
            }
        }
    }

    public static void writeState(AdminUnits adminUnits, File directory, String importStateJson,
                                  String importStateKey) throws IOException {
        try (Writer state = writer(new File(directory, STATE_JSON))) {
            StringBuilder json = new StringBuilder("{");
            Json.string(json, importStateKey);
            json.append(':').append(importStateJson).append(',');
            Json.string(json, AdminUnits.METADATA_KEY);
            json.append(':').append(adminUnits.reportMetadataJson()).append("}\n");
            state.append(json);
        }
    }

    static StringBuilder properties(AdminDirectory directory, AdminUnit unit) {
        List<AdminUnit> units = directory.getUnits();
        StringBuilder json = new StringBuilder(512);
        json.append("{\"id\":").append(unit.getId()).append(",\"admin_level\":").append(unit.getLevel())
                .append(",\"parent_id\":");
        AdminUnit parent = unit.getParent() >= 0 ? units.get(unit.getParent()) : null;
        json.append(parent == null ? "null" : Long.toString(parent.getId())).append(",\"parent_source\":");
        Json.string(json, parent == null ? null : String.valueOf(unit.getParentSource()));
        json.append(",\"parent_share\":").append(parent != null && unit.hasGeometry() && parent.hasGeometry()
                ? Float.toString((float) unit.getParentShare()) : "null");
        long[] path = new long[unit.getPath().length];
        for (int i = 0; i < path.length; i++) {
            path[i] = units.get(unit.getPath()[i]).getId();
        }
        json.append(",\"path\":");
        Json.longs(json, path);
        json.append(",\"depth\":").append(unit.getDepth()).append(",\"children\":").append(unit.getChildren())
                .append(",\"name\":");
        Json.string(json, AdminNames.name(unit.getTags(), directory.getOptions().getNameLang()));
        json.append(",\"names\":");
        Json.object(json, AdminNames.names(unit.getTags()));
        json.append(",\"tags\":");
        Json.object(json, unit.getTags());
        json.append(",\"has_geometry\":").append(unit.hasGeometry()).append(",\"area_m2\":")
                .append(unit.hasGeometry() ? AdminOptions.number(unit.getAreaM2()) : "null")
                .append(",\"own_area_m2\":")
                .append(unit.hasGeometry() ? AdminOptions.number(unit.getOwnAreaM2()) : "null")
                .append(",\"h3_3\":");
        Json.shorts(json, unit.getCells());
        return json.append('}');
    }

    static void geoJson(StringBuilder json, Geometry geometry) {
        if (geometry instanceof Polygon) {
            json.append("{\"type\":\"Polygon\",\"coordinates\":");
            polygon(json, (Polygon) geometry);
            json.append('}');
        } else if (geometry instanceof MultiPolygon) {
            json.append("{\"type\":\"MultiPolygon\",\"coordinates\":[");
            for (int i = 0; i < geometry.getNumGeometries(); i++) {
                json.append(i == 0 ? "" : ",");
                polygon(json, (Polygon) geometry.getGeometryN(i));
            }
            json.append("]}");
        } else {
            throw new IllegalArgumentException("Unexpected admin unit geometry " + geometry.getGeometryType());
        }
    }

    private static void polygon(StringBuilder json, Polygon polygon) {
        json.append('[');
        ring(json, polygon.getExteriorRing().getCoordinateSequence());
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            json.append(',');
            ring(json, polygon.getInteriorRingN(i).getCoordinateSequence());
        }
        json.append(']');
    }

    private static void ring(StringBuilder json, CoordinateSequence ring) {
        json.append('[');
        for (int i = 0; i < ring.size(); i++) {
            json.append(i == 0 ? "[" : ",[").append(coordinate(ring.getX(i))).append(',')
                    .append(coordinate(ring.getY(i))).append(']');
        }
        json.append(']');
    }

    static String coordinate(double degrees) {
        return BigDecimal.valueOf(GridGeometry.grid(degrees), COORDINATE_SCALE).stripTrailingZeros().toPlainString();
    }

    private static Writer writer(File file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(file.toPath()),
                StandardCharsets.UTF_8), BUFFER_SIZE);
    }
}
