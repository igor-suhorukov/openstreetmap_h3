package com.github.isuhorukov.osm.pgsnapshot.output;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
        assertTrue(multipolygon.contains("\nCREATE UNIQUE INDEX idx_multipolygon_id_h3_3 ON multipolygon using btree(id,h3_3);\n"),
                multipolygon);

        String afterInit = Files.readString(new File(staticDir, "database_after_init.sql").toPath());
        assertFalse(afterInit.contains("columnar"));
        assertFalse(afterInit.contains("${substitute}"));
    }

    @Test
    void copyResources_withoutMultipolygonTagKeys_noJoinWithRelations() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyResources(resultDir, false);

        String multipolygon = Files.readString(
                new File(new File(resultDir, ResultLayout.STATIC_DIR), "multipolygon_insert.sql").toPath());
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
                new File(new File(resultDir, ResultLayout.STATIC_DIR), "multipolygon_insert.sql").toPath());
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
        assertTrue(waysDefault.contains("ALTER TABLE  ways ATTACH PARTITION  \"ways_32767\" DEFAULT;\n"
                + "CREATE UNIQUE INDEX idx_ways_id ON ways (id, h3_3);\n"), waysDefault);
        assertTrue(waysDefault.contains("ANALYZE ONLY ways;"));
        String nodesAnalyze = Files.readString(new File(sqlFinalizeDir, "nodes_analyze.sql").toPath());
        int nodesIndex = nodesAnalyze.indexOf("CREATE UNIQUE INDEX idx_nodes_id ON nodes (id, h3_3);");
        assertTrue(nodesIndex >= 0 && nodesIndex < nodesAnalyze.indexOf("ANALYZE ONLY nodes;"), nodesAnalyze);
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

    @Test
    void copiedScripts_sameBytesAsBefore() throws Exception {
        File resultDir = prepareImportDirs();

        ResourceCopier.copyOsmiumSettings(resultDir);
        ResourceCopier.copyResources(resultDir, false);
        ResourceCopier.copyImportScripts(resultDir, false);

        assertEquals("c130cbc588a91ee414184f966eb77726", md5(resultDir, "static/osmium_export.json"));
        assertEquals("4c88c2c2f57396ccf0609e3eefb59708", md5(resultDir, "static/multipolygon.sql"));
        assertEquals("34ede1e42a6ff8efe6a05a8108c3a3ff", md5(resultDir, "static/multipolygon_insert.sql"));
        assertEquals("680d35581dc024ff1006b22067a96c89", md5(resultDir, "static/h3_poly.tsv.gz"));
        assertEquals("b64984f6c5d15c4fcc180faa03e08d44", md5(resultDir, "static/database_init.sql"));
        assertEquals("5f2ffc1ccbdd46fe8869b4012a5828a9", md5(resultDir, "static/database_after_init.sql"));
        assertEquals("70453ad3f7f0a5c34b5b628a8ce4509d", md5(resultDir, "sql/ways_32767_import.sql"));
        assertEquals("b01daaf3a801bd3e7fb1dac67c3a4294", md5(resultDir, "sql/relations_import.sql"));
        assertEquals("092c85628d8ca4beea6adce393bf7ba8", md5(resultDir, "sql_finalize/ways_default_partition.sql"));
        assertEquals("127293d7bee38258b8d04a4d83b17365", md5(resultDir, "sql_finalize/nodes_analyze.sql"));
        assertEquals("2b81dceb7d17a0156a1f6a0539b9a90f", md5(resultDir, "sql_finalize/multipolygon_import.sql"));
    }

    @Test
    void scripts_sameTextAsCopiedFiles() throws Exception {
        File resultDir = prepareImportDirs();
        Set<String> tagKeys = Set.of("is.poi");

        ResourceCopier.copyOsmiumSettings(resultDir);
        ResourceCopier.copyResources(resultDir, true, tagKeys);
        ResourceCopier.copyImportScripts(resultDir, true);

        Path result = resultDir.toPath();
        assertEquals(ResourceCopier.databaseInitScript(true), Files.readString(result.resolve("static/database_init.sql")));
        assertEquals(ResourceCopier.multipolygonScript(true), Files.readString(result.resolve("static/multipolygon.sql")));
        assertEquals(ResourceCopier.multipolygonInsertScript(tagKeys), Files.readString(result.resolve("static/multipolygon_insert.sql")));
        assertEquals(ResourceCopier.ways32767ImportScript(true), Files.readString(result.resolve("sql/ways_32767_import.sql")));
        assertEquals(ResourceCopier.relationsImportScript(false), Files.readString(result.resolve("sql/relations_import.sql")));
        assertArrayEquals(ResourceCopier.resourceBytes("/osmium_export.json"), Files.readAllBytes(result.resolve("static/osmium_export.json")));
    }

    @Test
    void copyResources_updatable_versionColumnsJournalAndImportState() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyResources(resultDir, false, true, Set.of());

        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        String dbInit = Files.readString(new File(staticDir, "database_init.sql").toPath());
        assertTrue(dbInit.contains("tags hstore);\nALTER TABLE nodes ADD COLUMN version integer NOT NULL;\n"), dbInit);
        assertTrue(dbInit.contains("ALTER TABLE ways ADD COLUMN version integer NOT NULL, ADD COLUMN geom_hash bigint NOT NULL;"));
        assertTrue(dbInit.contains("CREATE TABLE osm_import_state("));
        assertTrue(dbInit.contains("CREATE TABLE osm_change_log("));
        assertTrue(dbInit.indexOf("ALTER TABLE nodes") < dbInit.indexOf("CREATE OR REPLACE VIEW geometry_global_view"));
        assertFalse(dbInit.contains("${updatable_schema}"));
        String afterInit = Files.readString(new File(staticDir, "database_after_init.sql").toPath());
        assertTrue(afterInit.contains("ANALYZE osm_stat_ways_3_3;\nCOPY osm_import_state(operation,source_pbf,"
                + "replication_timestamp,replication_sequence,replication_url,parameters) FROM "
                + "'/input/import_related_metadata/osm_import_state.tsv' DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV;\n"), afterInit);
        assertFalse(afterInit.contains("${"));
    }

    @Test
    void copyResources_notUpdatable_noJournal() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyResources(resultDir, false, false, Set.of());

        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        assertFalse(Files.readString(new File(staticDir, "database_init.sql").toPath()).contains("osm_import_state"));
        assertFalse(Files.readString(new File(staticDir, "database_after_init.sql").toPath()).contains("osm_import_state"));
    }

    @Test
    void copyImportScripts_updatable_versionColumnsAndIdIndexes() throws IOException {
        File resultDir = prepareImportDirs();

        ResourceCopier.copyImportScripts(resultDir, false, true);

        File sqlDir = new File(resultDir, ResultLayout.SQL_DIR);
        String relations = Files.readString(new File(sqlDir, "relations_import.sql").toPath());
        assertTrue(relations.contains("COPY relations(id,tags,version) FROM '/input/relations/00000.tsv'"));
        assertTrue(relations.contains("PRIMARY KEY (relation_id, sequence_id);\n"
                + "ALTER TABLE ONLY relations ADD CONSTRAINT pk_relations PRIMARY KEY (id) INCLUDE (version);\nCOMMIT;"), relations);
        String ways32767 = Files.readString(new File(sqlDir, "ways_32767_import.sql").toPath());
        assertTrue(ways32767.contains("h3_8_regions,h3_3_multi_regions,tags,version,geom_hash) FROM '/input/ways/32767.tsv'"));
        assertTrue(ways32767.contains("h3.h3_3_multi_regions,version,geom_hash  from preimport_ways_32767 p"));
        assertTrue(ways32767.contains("alter table ways_32767 alter COLUMN version set NOT NULL;\n"
                + "alter table ways_32767 alter COLUMN geom_hash set NOT NULL;\n"));
        assertTrue(ways32767.contains("array_agg(h3_3 ORDER BY h3_3) h3_3_multi_regions"));
        assertTrue(ways32767.contains("union select wl.id, unnest(wl.h3_3_multi_regions) from \"preimport_ways_32767\" wl"), ways32767);
        assertFalse(ways32767.contains("is.h33idxs"), ways32767);
        File sqlFinalizeDir = new File(resultDir, ResultLayout.SQL_FINALIZE_DIR);
        String waysDefault = Files.readString(new File(sqlFinalizeDir, "ways_default_partition.sql").toPath());
        assertTrue(waysDefault.contains("DEFAULT;\nCREATE UNIQUE INDEX idx_ways_id ON ways (id, h3_3) INCLUDE (version, geom_hash);\n"),
                waysDefault);
        String nodesAnalyze = Files.readString(new File(sqlFinalizeDir, "nodes_analyze.sql").toPath());
        int nodesIndex = nodesAnalyze.indexOf("CREATE UNIQUE INDEX idx_nodes_id ON nodes (id, h3_3) INCLUDE (version);");
        assertTrue(nodesIndex >= 0 && nodesIndex < nodesAnalyze.indexOf("ANALYZE ONLY nodes;"), nodesAnalyze);
        for (File script : List.of(new File(sqlDir, "relations_import.sql"), new File(sqlDir, "ways_32767_import.sql"),
                new File(sqlFinalizeDir, "ways_default_partition.sql"), new File(sqlFinalizeDir, "nodes_analyze.sql"))) {
            assertFalse(Files.readString(script.toPath()).contains("${"), script.getName());
        }
    }

    @Test
    void updateScripts_columnsAndImportStateSubstituted() throws IOException {
        for (String script : List.of(ResourceCopier.UPDATE_PREPARE_SQL, ResourceCopier.UPDATE_DELTA_SQL,
                ResourceCopier.UPDATE_APPLY_SQL, ResourceCopier.UPDATE_MULTIPOLYGON_SQL,
                ResourceCopier.UPDATE_AFFECTED_RELATIONS_SQL, ResourceCopier.UPDATE_AREA_RELATIONS_SQL)) {
            assertFalse(ResourceCopier.updateScript(script).contains("${"), script);
        }
        String delta = ResourceCopier.updateScript(ResourceCopier.UPDATE_DELTA_SQL);
        assertTrue(delta.contains("COPY osm_delta_nodes(h3_3,h3_8,id,geom,tags,version) FROM '/input/delta/nodes.tsv'"));
        assertTrue(delta.contains("COPY osm_delta_ways(h3_3,h3_8,id,closed,building,highway,scale,centre,bbox,linestring,"
                + "points,h3_8_regions,h3_3_multi_regions,tags,version,geom_hash) FROM '/input/delta/ways.tsv'"));
        assertTrue(delta.contains("COPY osm_delta_relations(id,tags,version) FROM '/input/delta/relations.tsv'"));
        String apply = ResourceCopier.updateScript(ResourceCopier.UPDATE_APPLY_SQL);
        assertTrue(apply.startsWith("BEGIN;\n\nCOPY osm_import_state("), apply);
        assertFalse(apply.contains("\nCOMMIT;"));
        assertTrue(apply.contains("SELECT w.id, unnest(w.h3_3_multi_regions) FROM osm_delta_ways w WHERE w.h3_3 = 32767"), apply);
        assertTrue(apply.contains("INSERT INTO ways(h3_3,h3_8,id,closed,building,highway,scale,centre,bbox,linestring,points,"
                + "h3_8_regions,h3_3_multi_regions,tags,version,geom_hash) SELECT h3_3,h3_8,id,closed,building,highway,scale,"
                + "centre,bbox,linestring,points,h3_8_regions,h3_3_multi_regions,tags,version,geom_hash FROM osm_delta_ways;\n"), apply);
        assertFalse(apply.contains("is.h33idxs"), apply);
        assertTrue(ResourceCopier.updateScript(ResourceCopier.UPDATE_MULTIPOLYGON_SQL).endsWith("COMMIT;\n"));
    }

    private static String md5(File resultDir, String path) throws Exception {
        byte[] hash = java.security.MessageDigest.getInstance("MD5").digest(Files.readAllBytes(resultDir.toPath().resolve(path)));
        StringBuilder hex = new StringBuilder();
        for (byte b : hash) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }
}
