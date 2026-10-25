package com.github.isuhorukov.osm.pgsnapshot.output;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

class ResourceCopierTest {

    @TempDir
    Path tempDir;

    private File prepareStaticDir() {
        File staticDir = new File(tempDir.toFile(), ResultLayout.STATIC_DIR);
        staticDir.mkdirs();
        return tempDir.toFile();
    }

    @Test
    void copyResource_nonExistentClasspathResource_throwsIllegalArgument() {
        File dest = new File(tempDir.toFile(), "out.txt");
        assertThrows(IllegalArgumentException.class,
                () -> ResourceCopier.copyResource("/no_such_resource_xyz.json", dest));
    }

    @Test
    void copyOsmiumSettings_copiesOsmiumExportJson() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyOsmiumSettings(resultDir);

        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        File copied = new File(staticDir, "osmium_export.json");
        assertTrue(copied.exists());
        assertTrue(copied.length() > 0);
    }

    @Test
    void copyResources_nonColumnar_substituteIsEmpty() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyResources(resultDir, false);

        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        String dbInit = Files.readString(new File(staticDir, "database_init.sql").toPath());
        assertFalse(dbInit.contains("CREATE EXTENSION citus;"),
                "non-columnar init should not contain citus extension");
        assertFalse(dbInit.contains("${substitute}"),
                "placeholder should be replaced");

        String multipolygon = Files.readString(new File(staticDir, "multipolygon.sql").toPath());
        assertFalse(multipolygon.contains("USING COLUMNAR"));
        assertFalse(multipolygon.contains("${substitute}"));

        String afterInit = Files.readString(new File(staticDir, "database_after_init.sql").toPath());
        assertFalse(afterInit.contains("columnar"));
        assertFalse(afterInit.contains("${substitute}"));
    }

    @Test
    void copyResources_withoutMultipolygonTagKeys_noJoinWithRelations() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyResources(resultDir, false);

        String multipolygon = Files.readString(
                new File(new File(resultDir, ResultLayout.STATIC_DIR), "multipolygon.sql").toPath());
        assertTrue(multipolygon.contains("\ntags,\n"));
        assertTrue(multipolygon.contains("c.id=h.id order by 1;"));
        assertFalse(multipolygon.contains("from relations"));
        assertFalse(multipolygon.contains("${"));
    }

    @Test
    void copyResources_multipolygonTagKeys_takenFromRelations() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyResources(resultDir, false, new TreeSet<>(Set.of("is.poi", "is.o'k")));

        String multipolygon = Files.readString(
                new File(new File(resultDir, ResultLayout.STATIC_DIR), "multipolygon.sql").toPath());
        assertTrue(multipolygon.contains("\ncoalesce(p.tags || r.extension_tags, p.tags) tags,\n"));
        assertTrue(multipolygon.contains("c.id=h.id left join (select id, slice(tags, ARRAY['is.o''k','is.poi']) extension_tags"
                + " from relations where tags ?| ARRAY['is.o''k','is.poi']) r on r.id=p.id order by 1;"), multipolygon);
        assertFalse(multipolygon.contains("${"));
    }

    @Test
    void copyResources_columnar_substituteContainsCitusAndColumnar() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyResources(resultDir, true);

        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        String dbInit = Files.readString(new File(staticDir, "database_init.sql").toPath());
        assertTrue(dbInit.contains("CREATE EXTENSION citus;"));
        assertFalse(dbInit.contains("${substitute}"));

        String multipolygon = Files.readString(new File(staticDir, "multipolygon.sql").toPath());
        assertTrue(multipolygon.contains("USING COLUMNAR"));
        assertFalse(multipolygon.contains("${substitute}"));

        String afterInit = Files.readString(new File(staticDir, "database_after_init.sql").toPath());
        assertFalse(afterInit.contains("alter_table_set_access_method"));
        assertFalse(afterInit.contains("${substitute}"));
    }

    private File prepareImportDirs() {
        File resultDir = tempDir.toFile();
        ResultLayout.prepareResultDirectories(resultDir, true, false);
        return resultDir;
    }

    @Test
    void copyImportScripts_writesLoadAndFinalizeScripts() throws IOException {
        File resultDir = prepareImportDirs();

        ResourceCopier.copyImportScripts(resultDir, false);

        File sqlDir = new File(resultDir, ResultLayout.SQL_DIR);
        String relations = Files.readString(new File(sqlDir, "relations_import.sql").toPath());
        assertTrue(relations.contains("TRUNCATE relations, relation_members;"));
        assertTrue(relations.contains("FROM '/input/relations/00001.tsv' DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV FREEZE;"));
        String ways32767 = Files.readString(new File(sqlDir, "ways_32767_import.sql").toPath());
        assertTrue(ways32767.contains("create table ways_32767 as select"));
        assertFalse(ways32767.contains("ATTACH PARTITION"));
        assertFalse(ways32767.contains("${substitute}"));
        assertFalse(ways32767.contains("alter_table_set_access_method"));

        File sqlFinalizeDir = new File(resultDir, ResultLayout.SQL_FINALIZE_DIR);
        String waysDefault = Files.readString(new File(sqlFinalizeDir, "ways_default_partition.sql").toPath());
        assertTrue(waysDefault.contains("ALTER TABLE  ways ATTACH PARTITION  \"ways_32767\" DEFAULT;"));
        assertTrue(waysDefault.contains("ANALYZE ONLY ways;"));
        assertTrue(Files.readString(new File(sqlFinalizeDir, "nodes_analyze.sql").toPath()).contains("ANALYZE ONLY nodes;"));
        assertTrue(Files.readString(new File(sqlFinalizeDir, "multipolygon_import.sql").toPath())
                .contains("\\i /input/static/multipolygon.sql\nANALYZE multipolygon;"));
    }

    @Test
    void copyImportScripts_columnar_convertsDefaultWaysPartition() throws IOException {
        File resultDir = prepareImportDirs();

        ResourceCopier.copyImportScripts(resultDir, true);

        String ways32767 = Files.readString(
                new File(new File(resultDir, ResultLayout.SQL_DIR), "ways_32767_import.sql").toPath());
        assertTrue(ways32767.contains("SELECT alter_table_set_access_method('ways_32767', 'columnar');\nCOMMIT;"));
    }

    @Test
    void copyResources_h3PolyGzCopied() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyResources(resultDir, false);

        File h3Poly = new File(new File(resultDir, ResultLayout.STATIC_DIR), "h3_poly.tsv.gz");
        assertTrue(h3Poly.exists());
        assertTrue(h3Poly.length() > 0);
    }
}
