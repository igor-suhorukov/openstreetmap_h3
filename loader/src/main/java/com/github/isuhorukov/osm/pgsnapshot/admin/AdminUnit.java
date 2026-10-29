package com.github.isuhorukov.osm.pgsnapshot.admin;

import java.util.SortedMap;

public final class AdminUnit {

    public static final char PARENT_GEOMETRY = 'G';
    public static final char PARENT_SUBAREA = 'S';
    static final char NO_PARENT = ' ';
    private static final short[] NO_CELLS = new short[0];

    private final int index;
    private final long id;
    private final int level;
    private final SortedMap<String, String> tags;
    private byte[] ewkb;
    private double areaM2;
    private double areaDegrees;
    private int subareaParent = -1;
    private int parent = -1;
    private char parentSource = NO_PARENT;
    private double parentShare;
    private int[] path;
    private int children;
    private double ownAreaM2;
    private short[] cells = NO_CELLS;

    AdminUnit(int index, long id, int level, SortedMap<String, String> tags) {
        this.index = index;
        this.id = id;
        this.level = level;
        this.tags = tags;
    }

    public int getIndex() {
        return index;
    }

    public long getId() {
        return id;
    }

    public int getLevel() {
        return level;
    }

    public SortedMap<String, String> getTags() {
        return tags;
    }

    public boolean hasGeometry() {
        return ewkb != null;
    }

    public byte[] getEwkb() {
        return ewkb;
    }

    void setEwkb(byte[] ewkb) {
        this.ewkb = ewkb;
    }

    public double getAreaM2() {
        return areaM2;
    }

    void setArea(double areaM2, double areaDegrees) {
        this.areaM2 = areaM2;
        this.areaDegrees = areaDegrees;
    }

    double getAreaDegrees() {
        return areaDegrees;
    }

    int getSubareaParent() {
        return subareaParent;
    }

    void setSubareaParent(int subareaParent) {
        this.subareaParent = subareaParent;
    }

    public int getParent() {
        return parent;
    }

    public char getParentSource() {
        return parentSource;
    }

    public double getParentShare() {
        return parentShare;
    }

    void setParent(int parent, char parentSource, double parentShare) {
        this.parent = parent;
        this.parentSource = parentSource;
        this.parentShare = parentShare;
    }

    public int[] getPath() {
        return path;
    }

    void setPath(int[] path) {
        this.path = path;
    }

    public int getDepth() {
        return path.length - 1;
    }

    public int getChildren() {
        return children;
    }

    void addChild() {
        children++;
    }

    public double getOwnAreaM2() {
        return ownAreaM2;
    }

    void setOwnAreaM2(double ownAreaM2) {
        this.ownAreaM2 = ownAreaM2;
    }

    public short[] getCells() {
        return cells;
    }

    void setCells(short[] cells) {
        this.cells = cells;
    }
}
