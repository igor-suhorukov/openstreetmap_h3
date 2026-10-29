package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import java.util.List;
import java.util.SortedMap;

public final class AdminDirectory {

    private final List<AdminUnit> units;
    private final CompactIndex index;
    private final AdminOptions options;
    private final String fingerprint;

    AdminDirectory(List<AdminUnit> units, CompactIndex index, AdminOptions options, String fingerprint) {
        this.units = units;
        this.index = index;
        this.options = options;
        this.fingerprint = fingerprint;
    }

    public List<AdminUnit> getUnits() {
        return units;
    }

    public AdminOptions getOptions() {
        return options;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    CompactIndex getIndex() {
        return index;
    }

    public int regionCount() {
        return index.pieceCount();
    }

    public AdminUnit regionUnit(int region) {
        return units.get(index.pieceUnit(region));
    }

    public Polygon region(int region) {
        return index.piece(region);
    }

    public long unitsWithGeometry() {
        return units.stream().filter(AdminUnit::hasGeometry).count();
    }

    long unitId(int unit) {
        return units.get(unit).getId();
    }

    public Geometry geometry(AdminUnit unit) {
        return unit.hasGeometry() ? DirectoryBuilder.geometry(unit) : null;
    }

    public String name(AdminUnit unit) {
        return AdminNames.name(unit.getTags(), options.getNameLang());
    }

    public SortedMap<String, String> names(AdminUnit unit) {
        return AdminNames.names(unit.getTags());
    }

    public AdminUnit parent(AdminUnit unit) {
        return unit.getParent() >= 0 ? units.get(unit.getParent()) : null;
    }

    public Float parentShare(AdminUnit unit) {
        AdminUnit parent = parent(unit);
        return parent != null && unit.hasGeometry() && parent.hasGeometry() ? (float) unit.getParentShare() : null;
    }

    public int[] regionPartNumbers() {
        int[] partNumbers = new int[index.pieceCount()];
        for (int region = 0; region < partNumbers.length; region++) {
            boolean sameUnit = region > 0 && index.pieceUnit(region) == index.pieceUnit(region - 1);
            partNumbers[region] = sameUnit ? partNumbers[region - 1] + 1 : 1;
        }
        return partNumbers;
    }

    public long[] pathIds(AdminUnit unit) {
        int[] path = unit.getPath();
        long[] ids = new long[path.length];
        for (int i = 0; i < path.length; i++) {
            ids[i] = units.get(path[i]).getId();
        }
        return ids;
    }
}
