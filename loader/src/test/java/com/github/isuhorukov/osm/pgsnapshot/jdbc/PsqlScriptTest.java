package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.output.ResourceCopier;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class PsqlScriptTest {

    private static List<String> commands(String script) {
        return PsqlScript.parse(script).getCommands().stream()
                .map(command -> (command.isGset() ? "GSET " : "") + command)
                .collect(Collectors.toList());
    }

    @Test
    void parse_semicolons_trimmedStatementsWithoutEmptyOnes() {
        assertEquals(List.of("SELECT 1", "SELECT 2"), commands(" SELECT 1;;\n  SELECT 2 ;\n"));
    }

    @Test
    void parse_lastStatementWithoutSemicolon_included() {
        assertEquals(List.of("BEGIN", "COMMIT"), commands("BEGIN;\nCOMMIT"));
    }

    @Test
    void parse_dollarQuotedBodies_semicolonsInside() {
        assertEquals(List.of("CREATE FUNCTION f(bigint) RETURNS int AS $$ select ($1 & 1)::int; $$ LANGUAGE SQL",
                        "DO $body$ BEGIN PERFORM 1; $$ inner $$; END; $body$", "SELECT 3"),
                commands("CREATE FUNCTION f(bigint) RETURNS int AS $$ select ($1 & 1)::int; $$ LANGUAGE SQL;\n"
                        + "DO $body$ BEGIN PERFORM 1; $$ inner $$; END; $body$;\nSELECT 3;"));
    }

    @Test
    void parse_positionalParameter_isNotDollarQuote() {
        assertEquals(List.of("PREPARE p(int) AS SELECT $1", "SELECT 2"), commands("PREPARE p(int) AS SELECT $1; SELECT 2;"));
    }

    @Test
    void parse_identifierWithDollar_isNotDollarQuote() {
        assertEquals(List.of("SELECT a$b$c FROM t", "SELECT 2"), commands("SELECT a$b$c FROM t; SELECT 2;"));
    }

    @Test
    void parse_standardStringWithBackslash_endsAtQuote() {
        assertEquals(List.of("COPY t FROM STDIN DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV", "SELECT 1"),
                commands("COPY t FROM STDIN DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV;\nSELECT 1;"));
    }

    @Test
    void parse_escapeStringWithEscapedQuote_semicolonInside() {
        assertEquals(List.of("SELECT E'it\\'s; fine'", "SELECT e'\\\\'", "SELECT 2"),
                commands("SELECT E'it\\'s; fine'; SELECT e'\\\\'; SELECT 2;"));
    }

    @Test
    void parse_doubledQuotesAndQuotedIdentifiers_semicolonsInside() {
        assertEquals(List.of("SELECT 'it''s; fine'", "CREATE TABLE \"a;\"\"b\" (x int)", "SELECT 3"),
                commands("SELECT 'it''s; fine'; CREATE TABLE \"a;\"\"b\" (x int); SELECT 3;"));
    }

    @Test
    void parse_comments_removed() {
        assertEquals(List.of("SELECT 1", "SELECT   2", "SELECT '-- not a comment'"),
                commands("-- comment; not a statement\nSELECT 1; -- trailing; comment\n"
                        + "SELECT /* block; /* nested; */ still */ 2;\nSELECT '-- not a comment';"));
    }

    @Test
    void parse_metaCommands_atLineStart() {
        assertEquals(List.of("\\timing on", "SELECT 1", "\\c osmworld", "\\i /input/static/x.sql", "\\endif"),
                commands("\\timing on\nSELECT 1;\n\\c osmworld\n\\i   /input/static/x.sql  \n\\endif"));
    }

    @Test
    void parse_gsetAndConditionals() {
        assertEquals(List.of("GSET SELECT true AS flag", "\\if :flag", "SELECT 2", "\\else", "SELECT 3", "\\endif"),
                commands("SELECT true AS flag \\gset\n\\if :flag\nSELECT 2;\n\\else\nSELECT 3;\n\\endif\n"));
    }

    @Test
    void parse_gTerminator_plainStatement() {
        assertEquals(List.of("SELECT 1"), commands("SELECT 1 \\g\n"));
    }

    @Test
    void parse_unsupportedMetaCommandInsideStatement_throws() {
        assertThrows(IllegalArgumentException.class, () -> PsqlScript.parse("SELECT 1 \\timing\n"));
        assertThrows(IllegalArgumentException.class, () -> PsqlScript.parse("SELECT 1 \\gset prefix_\n"));
    }

    @Test
    void parse_gsetWithoutQuery_standaloneMetaCommand() {
        assertEquals(List.of("SELECT 1", "\\gset"), commands("SELECT 1; \\gset"));
    }

    @Test
    void parse_unterminatedText_throws() {
        assertThrows(IllegalArgumentException.class, () -> PsqlScript.parse("SELECT 'open"));
        assertThrows(IllegalArgumentException.class, () -> PsqlScript.parse("SELECT \"open"));
        assertThrows(IllegalArgumentException.class, () -> PsqlScript.parse("DO $$ BEGIN"));
        assertThrows(IllegalArgumentException.class, () -> PsqlScript.parse("SELECT 1 /* open"));
    }

    @Test
    void afterLastConnect_commandsAfterConnect() {
        PsqlScript script = PsqlScript.parse("ALTER SYSTEM SET x = 1;\n\\c osmworld\nSELECT 1;\n\\connect osmworld\nSELECT 2;");

        assertEquals(List.of("SELECT 2"), script.afterLastConnect().stream().map(PsqlCommand::toString).collect(Collectors.toList()));
        assertEquals(5, script.getCommands().size());
        assertEquals(1, PsqlScript.parse("SELECT 1").afterLastConnect().size());
    }

    @Test
    void parse_databaseInitScript_schemaAfterConnect() throws Exception {
        List<PsqlCommand> commands = PsqlScript.parse(ResourceCopier.databaseInitScript(false)).afterLastConnect();
        List<String> sql = commands.stream().filter(command -> !command.isMeta()).map(PsqlCommand::getSql)
                .collect(Collectors.toList());

        assertEquals("CREATE EXTENSION IF NOT EXISTS postgis", sql.get(0));
        assertTrue(sql.stream().noneMatch(statement -> statement.startsWith("ALTER SYSTEM")
                || statement.startsWith("create database")), sql.toString());
        assertTrue(commands.stream().anyMatch(command -> command.isMeta() && "i".equals(command.getMetaCommand())
                && "/input/static/multipolygon_tables.sql".equals(command.getArgument())));
        assertTrue(sql.stream().anyMatch(statement -> statement.startsWith("copy h3_3_bounds_complex")
                && statement.contains("PROGRAM $$ zcat /input/static/h3_poly.tsv.gz$$")));
        assertEquals(5, sql.stream().filter(statement -> statement.startsWith("CREATE FUNCTION to_")).count());
        assertTrue(sql.stream().anyMatch(statement -> statement.startsWith("DO $$") && statement.endsWith("$$")));
        assertEquals("COMMIT", sql.get(sql.size() - 1));
    }

    @Test
    void parse_waysDefaultPartitionScript_conditionalAnalyze() throws Exception {
        List<String> commands = commands(ResourceCopier.resource("/" + ResourceCopier.WAYS_DEFAULT_PARTITION_SQL));

        assertEquals(List.of("SET application_name = 'psql sql_finalize/ways_default_partition.sql'", "\\timing on",
                "ALTER TABLE  ways ATTACH PARTITION  \"ways_32767\" DEFAULT",
                "GSET SELECT current_setting('server_version_num')::int >= 180000 AS analyze_only",
                "\\if :analyze_only", "ANALYZE ONLY ways", "\\else", "ANALYZE ways", "\\endif"), commands);
    }
}
