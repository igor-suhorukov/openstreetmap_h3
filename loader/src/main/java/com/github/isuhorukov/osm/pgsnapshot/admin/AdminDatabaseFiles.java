package com.github.isuhorukov.osm.pgsnapshot.admin;

import com.github.isuhorukov.osm.pgsnapshot.Serializer;
import com.github.isuhorukov.osm.pgsnapshot.util.HStoreFormatSerializer;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class AdminDatabaseFiles {

    public static final String UNIT_TSV = "unit.tsv";
    public static final String UNIT_REGION_TSV = "unit_region.tsv";
    public static final String STATE_TSV = "state.tsv";
    private static final String NULL = HStoreFormatSerializer.NULL_STRING;
    private static final int BUFFER_SIZE = 1 << 20;

    private AdminDatabaseFiles() {
    }

    public static void write(AdminUnits adminUnits, File directory) throws IOException {
        Files.createDirectories(directory.toPath());
        writeUnits(adminUnits, directory);
        writeRegions(adminUnits, directory);
        writeState(adminUnits, directory);
    }

    public static void writeUnits(AdminUnits adminUnits, File directory) throws IOException {
        AdminDirectory adminDirectory = adminUnits.getDirectory();
        List<AdminUnit> units = adminDirectory.getUnits();
        List<String> nameLang = adminDirectory.getOptions().getNameLang();
        try (Writer writer = writer(new File(directory, UNIT_TSV))) {
            for (AdminUnit unit : units) {
                writer.write(unitRow(units, unit, nameLang));
            }
        }
    }

    public static void writeRegions(AdminUnits adminUnits, File directory) throws IOException {
        AdminDirectory adminDirectory = adminUnits.getDirectory();
        WKBWriter wkbWriter = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN, true);
        int[] partNumbers = adminDirectory.regionPartNumbers();
        try (Writer writer = writer(new File(directory, UNIT_REGION_TSV))) {
            for (int region = 0; region < partNumbers.length; region++) {
                AdminUnit unit = adminDirectory.regionUnit(region);
                Polygon polygon = adminDirectory.region(region);
                polygon.setSRID(Serializer.SRID);
                writer.write(unit.getId() + "\t" + partNumbers[region] + "\t" + unit.getLevel() + "\t"
                        + WKBWriter.toHex(wkbWriter.write(polygon)) + "\n");
            }
        }
    }

    public static void writeState(AdminUnits adminUnits, File directory) throws IOException {
        try (Writer writer = writer(new File(directory, STATE_TSV))) {
            StringBuilder row = new StringBuilder("load\t").append(NULL).append('\t');
            HStoreFormatSerializer.escapeString(row, adminUnits.reportMetadataJson());
            row.append('\t').append(adminUnits.getDirectory().getUnits().size()).append('\t')
                    .append(adminUnits.boundObjects()).append('\n');
            writer.write(row.toString());
        }
    }

    static String unitRow(List<AdminUnit> units, AdminUnit unit, List<String> nameLang) {
        StringBuilder row = new StringBuilder(256);
        row.append(unit.getId()).append('\t').append(unit.getLevel()).append('\t');
        if (unit.getParent() >= 0) {
            AdminUnit parent = units.get(unit.getParent());
            row.append(parent.getId()).append('\t').append(unit.getParentSource()).append('\t');
            row.append(unit.hasGeometry() && parent.hasGeometry() ? Float.toString((float) unit.getParentShare())
                    : NULL);
        } else {
            row.append(NULL).append('\t').append(NULL).append('\t').append(NULL);
        }
        row.append("\t{");
        int[] path = unit.getPath();
        for (int i = 0; i < path.length; i++) {
            row.append(i == 0 ? "" : ",").append(units.get(path[i]).getId());
        }
        row.append("}\t").append(unit.getDepth()).append('\t').append(unit.getChildren()).append('\t');
        HStoreFormatSerializer.escapeString(row, AdminNames.name(unit.getTags(), nameLang));
        row.append('\t');
        HStoreFormatSerializer.renderTags(tags(AdminNames.names(unit.getTags())), row);
        row.append('\t');
        HStoreFormatSerializer.renderTags(tags(unit.getTags()), row);
        row.append("\t{");
        short[] cells = unit.getCells();
        for (int i = 0; i < cells.length; i++) {
            row.append(i == 0 ? "" : ",").append(cells[i]);
        }
        row.append("}\t");
        if (unit.hasGeometry()) {
            row.append(WKBWriter.toHex(unit.getEwkb())).append('\t').append(AdminOptions.number(unit.getAreaM2()))
                    .append('\t').append(AdminOptions.number(unit.getOwnAreaM2()));
        } else {
            row.append(NULL).append('\t').append(NULL).append('\t').append(NULL);
        }
        return row.append('\n').toString();
    }

    private static List<Tag> tags(Map<String, String> values) {
        List<Tag> tags = new ArrayList<>(values.size());
        for (Map.Entry<String, String> value : values.entrySet()) {
            tags.add(new Tag(value.getKey(), value.getValue()));
        }
        return tags;
    }

    private static Writer writer(File file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(file.toPath()),
                StandardCharsets.UTF_8), BUFFER_SIZE);
    }
}
