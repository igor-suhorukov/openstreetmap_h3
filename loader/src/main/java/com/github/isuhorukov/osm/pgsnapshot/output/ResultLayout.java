package com.github.isuhorukov.osm.pgsnapshot.output;

import java.io.File;
import java.util.List;

public class ResultLayout {

    public static final String NODES_DIR = "nodes";
    public static final String RELATIONS_DIR = "relations";
    public static final String WAYS_DIR = "ways";
    public static final String MULTIPOLYGON_DIR = "multipolygon";
    public static final String SQL_DIR = "sql";
    public static final String SQL_FINALIZE_DIR = "sql_finalize";
    public static final String ARROW_DIR = "arrow";
    public static final String IMPORT_RELATED_METADATA_DIR = "import_related_metadata";
    public static final String STATIC_DIR = "static";
    public static final String SPILL_DIR = "spill";
    public static final String SPILL_FILE_EXTENSION = ".arrows";
    public static final List<String> SPILL_ENTITY_DIRS = List.of(NODES_DIR, WAYS_DIR, RELATIONS_DIR, MULTIPOLYGON_DIR);

    private ResultLayout() {
    }

    public static String resultDirectoryNameFromSource(File sourcePbfFile) {
        return sourcePbfFile.getName().replace(".osm", "").replace(".pbf", "_loc_ways");
    }

    public static File prepareResultDirectories(File resultDir, boolean savePostgresqlTsv, boolean saveArrow) {
        resultDir.mkdir();
        if (savePostgresqlTsv) {
            File nodesDir = new File(resultDir, NODES_DIR);
            if (nodesDir.exists() && nodesDir.list().length > 0) {
                throw new IllegalArgumentException("Nodes directory contains files " + nodesDir.getAbsolutePath());
            }
            File waysDir = new File(resultDir, WAYS_DIR);
            if (waysDir.exists() && waysDir.list().length > 0) {
                throw new IllegalArgumentException("Ways directory contains files " + waysDir.getAbsolutePath());
            }
            File relationsDir = new File(resultDir, RELATIONS_DIR);
            if (relationsDir.exists() && relationsDir.list().length > 0) {
                throw new IllegalArgumentException("Relations directory contains files " + relationsDir.getAbsolutePath());
            }
            checkAndMakeMultipolygonDirectory(resultDir);
            nodesDir.mkdir();
            waysDir.mkdir();
            relationsDir.mkdir();
            new File(resultDir, SQL_DIR).mkdirs();
            new File(resultDir, SQL_FINALIZE_DIR).mkdirs();
        }
        new File(resultDir, IMPORT_RELATED_METADATA_DIR).mkdirs();
        if (saveArrow) {
            prepareArrowDirectories(new File(resultDir, ARROW_DIR));
        }
        File staticDir = new File(resultDir, STATIC_DIR);
        staticDir.mkdir();
        return resultDir;
    }

    private static void prepareArrowDirectories(File arrowDir) {
        arrowDir.mkdir();
        new File(arrowDir, NODES_DIR).mkdir();
        new File(arrowDir, WAYS_DIR).mkdir();
        new File(arrowDir, RELATIONS_DIR).mkdir();
        new File(arrowDir, MULTIPOLYGON_DIR).mkdir();
        File spillDir = new File(arrowDir, SPILL_DIR);
        for (String entityDir : SPILL_ENTITY_DIRS) {
            File spillEntityDir = new File(spillDir, entityDir);
            if (spillEntityDir.exists() && spillEntityDir.list().length > 0) {
                throw new IllegalArgumentException("Spill directory contains files " + spillEntityDir.getAbsolutePath());
            }
            spillEntityDir.mkdirs();
        }
    }

    public static String spillFileName(short h33) {
        return String.format("%05d", h33) + SPILL_FILE_EXTENSION;
    }

    public static File checkAndMakeMultipolygonDirectory(File resultDir) {
        File multipolygonDir = new File(resultDir, MULTIPOLYGON_DIR);
        if (multipolygonDir.exists() && multipolygonDir.list().length > 0) {
            throw new IllegalArgumentException("Multipolygon directory contains files " + multipolygonDir.getAbsolutePath());
        }
        multipolygonDir.mkdirs();
        return multipolygonDir;
    }
}
