package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.output.ResourceCopier;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class StreamedScriptTest {

    private static List<String> sql(List<PsqlCommand> commands) {
        return commands.stream().filter(command -> !command.isMeta()).map(PsqlCommand::getSql).collect(Collectors.toList());
    }

    @Test
    void of_relationsImportScript_prefixCopiesAndSuffix() throws Exception {
        StreamedScript script = StreamedScript.of(ResourceCopier.relationsImportScript(false));

        assertEquals(List.of("SET application_name = 'psql sql/relations_import.sql'", "BEGIN",
                "TRUNCATE relations, relation_members"), sql(script.getPrefix()));
        assertEquals(List.of("/input/relations/00000.tsv", "/input/relations/00001.tsv"),
                List.copyOf(script.getCopySqlByInputPath().keySet()));
        assertEquals("COPY relations(id,tags) FROM STDIN DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV FREEZE",
                script.getCopySqlByInputPath().get("/input/relations/00000.tsv"));
        assertEquals(List.of("ALTER TABLE ONLY relation_members ADD CONSTRAINT pk_relation_members PRIMARY KEY (relation_id, sequence_id)",
                "COMMIT", "ANALYZE relations", "ANALYZE relation_members"), sql(script.getSuffix()));
    }

    @Test
    void of_ways32767ImportScript_streamedPreimportTable() throws Exception {
        StreamedScript script = StreamedScript.of(ResourceCopier.ways32767ImportScript(false));

        assertEquals("CREATE TABLE \"preimport_ways_32767\"  (like ways)", sql(script.getPrefix()).get(2));
        assertTrue(script.getCopySqlByInputPath().get("/input/ways/32767.tsv")
                .startsWith("COPY \"preimport_ways_32767\"(h3_3,h3_8, id,closed,building,highway,scale,centre,bbox,linestring,points,h3_8_regions,tags) FROM STDIN"));
        List<String> suffix = sql(script.getSuffix());
        assertTrue(suffix.get(0).startsWith("create table preimport_ways_large_h3_3"), suffix.get(0));
        assertEquals(List.of("drop table preimport_ways_32767", "COMMIT", "ANALYZE ways_32767"),
                suffix.subList(suffix.size() - 3, suffix.size()));
    }

    @Test
    void of_ways32767ImportScriptColumnar_convertsDefaultPartition() throws Exception {
        List<String> suffix = sql(StreamedScript.of(ResourceCopier.ways32767ImportScript(true)).getSuffix());

        assertTrue(suffix.contains("SELECT alter_table_set_access_method('ways_32767', 'columnar')"), suffix.toString());
    }

    @Test
    void of_updatableRelationsImportScript_versionColumnAndPrimaryKey() throws Exception {
        StreamedScript script = StreamedScript.of(ResourceCopier.relationsImportScript(true));

        assertEquals("COPY relations(id,tags,version) FROM STDIN DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV FREEZE",
                script.getCopySqlByInputPath().get("/input/relations/00000.tsv"));
        assertEquals("ALTER TABLE ONLY relations ADD CONSTRAINT pk_relations PRIMARY KEY (id) INCLUDE (version)",
                sql(script.getSuffix()).get(1));
    }

    @Test
    void of_updatableWays32767ImportScript_versionColumnsNotNull() throws Exception {
        StreamedScript script = StreamedScript.of(ResourceCopier.ways32767ImportScript(false, true));

        assertTrue(script.getCopySqlByInputPath().get("/input/ways/32767.tsv")
                .contains("h3_8_regions,tags,version,geom_hash) FROM STDIN"));
        List<String> suffix = sql(script.getSuffix());
        assertTrue(suffix.contains("alter table ways_32767 alter COLUMN version set NOT NULL"), suffix.toString());
        assertTrue(suffix.contains("alter table ways_32767 alter COLUMN geom_hash set NOT NULL"), suffix.toString());
    }

    @Test
    void of_updateDeltaScript_allDeltaTablesStreamedInOneTransaction() throws Exception {
        StreamedScript script = StreamedScript.of(ResourceCopier.updateScript(ResourceCopier.UPDATE_DELTA_SQL));

        assertEquals(List.of("BEGIN", "TRUNCATE osm_delta_nodes, osm_delta_ways, osm_delta_relations, "
                + "osm_delta_relation_members, osm_delta_changes"), sql(script.getPrefix()));
        assertEquals(List.of(DeltaBlockWriter.NODES, DeltaBlockWriter.WAYS, DeltaBlockWriter.RELATIONS,
                DeltaBlockWriter.RELATION_MEMBERS, DeltaBlockWriter.CHANGES),
                List.copyOf(script.getCopySqlByInputPath().keySet()));
        assertEquals("COPY osm_delta_changes(object_type,id,operation) FROM STDIN DELIMITER E'\\t' ESCAPE '\\' "
                + "NULL '\\N' CSV FREEZE", script.getCopySqlByInputPath().get(DeltaBlockWriter.CHANGES));
        assertEquals(List.of("COMMIT"), sql(script.getSuffix()));
    }

    @Test
    void of_statementBetweenStreamedCopies_throws() {
        assertThrows(IllegalArgumentException.class, () -> StreamedScript.of(
                "BEGIN;\nCOPY a FROM '/input/a.tsv';\nANALYZE a;\nCOPY b FROM '/input/b.tsv';\nCOMMIT;"));
    }

    @Test
    void of_scriptWithoutCopy_throws() {
        assertThrows(IllegalArgumentException.class, () -> StreamedScript.of("BEGIN;\nCOMMIT;"));
    }
}
