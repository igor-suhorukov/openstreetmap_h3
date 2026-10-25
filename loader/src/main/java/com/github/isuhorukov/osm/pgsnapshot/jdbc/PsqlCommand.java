package com.github.isuhorukov.osm.pgsnapshot.jdbc;

final class PsqlCommand {

    private final String sql;
    private final String metaCommand;
    private final String argument;
    private final boolean gset;

    private PsqlCommand(String sql, String metaCommand, String argument, boolean gset) {
        this.sql = sql;
        this.metaCommand = metaCommand;
        this.argument = argument;
        this.gset = gset;
    }

    static PsqlCommand sql(String sql, boolean gset) {
        return new PsqlCommand(sql, null, null, gset);
    }

    static PsqlCommand meta(String metaCommand, String argument) {
        return new PsqlCommand(null, metaCommand, argument, false);
    }

    boolean isMeta() {
        return metaCommand != null;
    }

    String getSql() {
        return sql;
    }

    String getMetaCommand() {
        return metaCommand;
    }

    String getArgument() {
        return argument;
    }

    boolean isGset() {
        return gset;
    }

    @Override
    public String toString() {
        if (!isMeta()) {
            return sql;
        }
        return argument.isEmpty() ? "\\" + metaCommand : "\\" + metaCommand + " " + argument;
    }
}
