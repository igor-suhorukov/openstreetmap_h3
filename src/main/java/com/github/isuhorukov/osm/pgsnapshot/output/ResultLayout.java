package com.github.isuhorukov.osm.pgsnapshot.output;

import java.io.File;
import java.io.IOException;

public class ResultLayout {

    public static final String NODES_DIR = "nodes";
    public static final String RELATIONS_DIR = "relations";
    public static final String WAYS_DIR = "ways";
    public static final String MULTIPOLYGON_DIR = "multipolygon";
    public static final String SQL_DIR = "sql";
    public static final String ARROW_DIR = "arrow";
    public static final String IMPORT_RELATED_METADATA_DIR = "import_related_metadata";
    public static final String STATIC_DIR = "static";

    private ResultLayout() {
    }

    public static String resultDirectoryNameFromSource(File inputDirectory) {
        return inputDirectory.getName().replace("_blocks", "");
    }

    public static File prepareResultDirectories(File resultDir, boolean savePostgresqlTsv, boolean saveArrow) throws IOException {
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
        }
        new File(resultDir, IMPORT_RELATED_METADATA_DIR).mkdirs();
        if (saveArrow) {
            File arrowDir = new File(resultDir, ARROW_DIR);
            arrowDir.mkdir();
            new File(arrowDir, NODES_DIR).mkdir();
            new File(arrowDir, WAYS_DIR).mkdir();
            new File(arrowDir, RELATIONS_DIR).mkdir();
        }
        File staticDir = new File(resultDir, STATIC_DIR);
        staticDir.mkdir();
        return resultDir;
    }

    public static File checkAndMakeMultipolygonDirectory(File resultDir) {
        File multipolygonDir = new File(resultDir, MULTIPOLYGON_DIR);
        if (multipolygonDir.exists() && multipolygonDir.list().length > 0) {
            throw new IllegalArgumentException("Multipolygon directory contains files " + multipolygonDir.getAbsolutePath());
        }
        multipolygonDir.mkdirs();
        return multipolygonDir;
    }

    public static File blockResultDirectory(long nodeRecords, long wayRecords, long relationCount, File resultDir) {
        String typeDir;
        if (nodeRecords > 0) {
            typeDir = NODES_DIR;
        } else if (wayRecords > 0) {
            typeDir = WAYS_DIR;
        } else if (relationCount > 0) {
            typeDir = RELATIONS_DIR;
        } else {
            throw new IllegalArgumentException("unknown block type");
        }
        return new File(resultDir, typeDir);
    }
}
