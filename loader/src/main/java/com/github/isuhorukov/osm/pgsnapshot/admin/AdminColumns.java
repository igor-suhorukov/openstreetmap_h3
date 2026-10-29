package com.github.isuhorukov.osm.pgsnapshot.admin;

public enum AdminColumns {
    NONE,
    UNITS,
    UNITS_AND_SHARES;

    public static final String UNITS_COLUMN = "admin_units";
    public static final String SHARES_COLUMN = "admin_unit_shares";

    public static AdminColumns of(boolean units, boolean shares) {
        if (!units) {
            return NONE;
        }
        return shares ? UNITS_AND_SHARES : UNITS;
    }

    public boolean isEnabled() {
        return this != NONE;
    }

    public boolean hasShares() {
        return this == UNITS_AND_SHARES;
    }

    public String copyColumns() {
        switch (this) {
            case UNITS:
                return "," + UNITS_COLUMN;
            case UNITS_AND_SHARES:
                return "," + UNITS_COLUMN + "," + SHARES_COLUMN;
            default:
                return "";
        }
    }

    public String columnDefinitions() {
        switch (this) {
            case UNITS:
                return ",\n    " + UNITS_COLUMN + " bigint[]";
            case UNITS_AND_SHARES:
                return ",\n    " + UNITS_COLUMN + " bigint[],\n    " + SHARES_COLUMN + " real[]";
            default:
                return "";
        }
    }

    public String inlineColumnDefinitions() {
        switch (this) {
            case UNITS:
                return ", " + UNITS_COLUMN + " bigint[]";
            case UNITS_AND_SHARES:
                return ", " + UNITS_COLUMN + " bigint[], " + SHARES_COLUMN + " real[]";
            default:
                return "";
        }
    }

    public String columnComments() {
        if (!isEnabled()) {
            return "";
        }
        StringBuilder comments = new StringBuilder();
        for (String table : new String[]{"nodes", "ways", "multipolygon"}) {
            comments.append("\nCOMMENT ON COLUMN ").append(table).append('.').append(UNITS_COLUMN)
                    .append(" IS 'Ids of the leaf administrative units (admin.unit.id, OSM relation ids) of the object: the primary unit first, then by descending share. NULL for objects that are not bound or lie outside all units';");
            if (hasShares()) {
                comments.append("\nCOMMENT ON COLUMN ").append(table).append('.').append(SHARES_COLUMN)
                        .append(" IS 'Shares of the object area (or length of open lines) in the units of admin_units, same order and length; 1 for nodes. Shares are not normalized';");
            }
        }
        return comments.toString();
    }
}
