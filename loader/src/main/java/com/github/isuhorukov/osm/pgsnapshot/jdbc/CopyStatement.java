package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class CopyStatement {

    private static final Pattern FILE_SOURCE = Pattern.compile(
            "(?is)^(copy\\s.+?\\sfrom\\s+)'(/input/[^']+)'(.*)$");
    private static final Pattern ZCAT_PROGRAM_SOURCE = Pattern.compile(
            "(?is)^(copy\\s.+?\\sfrom\\s+)program\\s+\\$\\$\\s*zcat\\s+(/input/[^\\s$]+)\\s*\\$\\$(.*)$");

    private final String sql;
    private final String inputPath;
    private final boolean gzip;

    private CopyStatement(String sql, String inputPath, boolean gzip) {
        this.sql = sql;
        this.inputPath = inputPath;
        this.gzip = gzip;
    }

    static CopyStatement parse(String sql) {
        Matcher file = FILE_SOURCE.matcher(sql);
        if (file.matches()) {
            return new CopyStatement(file.group(1) + "STDIN" + file.group(3), file.group(2), false);
        }
        Matcher program = ZCAT_PROGRAM_SOURCE.matcher(sql);
        if (program.matches()) {
            return new CopyStatement(program.group(1) + "STDIN" + program.group(3), program.group(2), true);
        }
        return null;
    }

    String getSql() {
        return sql;
    }

    String getInputPath() {
        return inputPath;
    }

    boolean isGzip() {
        return gzip;
    }
}
