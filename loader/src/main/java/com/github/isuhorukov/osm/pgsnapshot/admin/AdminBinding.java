package com.github.isuhorukov.osm.pgsnapshot.admin;

public final class AdminBinding {

    private static final String NULL = "\\N";

    private final long[] unitIds;
    private final float[] shares;

    public AdminBinding(long[] unitIds, float[] shares) {
        if (unitIds.length != shares.length) {
            throw new IllegalArgumentException("admin_units and admin_unit_shares differ in length");
        }
        this.unitIds = unitIds;
        this.shares = shares;
    }

    public long[] getUnitIds() {
        return unitIds;
    }

    public float[] getShares() {
        return shares;
    }

    public static void appendTsv(StringBuilder row, AdminBinding binding, AdminColumns columns) {
        if (columns.isEnabled()) {
            row.append('\t');
            appendUnits(row, binding);
        }
        if (columns.hasShares()) {
            row.append('\t');
            appendShares(row, binding);
        }
    }

    private static void appendUnits(StringBuilder row, AdminBinding binding) {
        if (binding == null) {
            row.append(NULL);
            return;
        }
        row.append('{');
        for (int i = 0; i < binding.unitIds.length; i++) {
            row.append(i == 0 ? "" : ",").append(binding.unitIds[i]);
        }
        row.append('}');
    }

    private static void appendShares(StringBuilder row, AdminBinding binding) {
        if (binding == null) {
            row.append(NULL);
            return;
        }
        row.append('{');
        for (int i = 0; i < binding.shares.length; i++) {
            row.append(i == 0 ? "" : ",").append(binding.shares[i]);
        }
        row.append('}');
    }
}
