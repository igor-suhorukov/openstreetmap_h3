package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PsqlRunnerTest {

    private final List<String> executed = new ArrayList<>();
    private final Map<String, String> copied = new LinkedHashMap<>();
    private final InputFiles inputFiles = new InputFiles();
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;

    @BeforeEach
    void mockConnection() throws Exception {
        connection = mock(Connection.class);
        statement = mock(Statement.class);
        resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        PGConnection pgConnection = mock(PGConnection.class);
        CopyManager copyManager = mock(CopyManager.class);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(anyString())).thenAnswer(invocation -> executed.add(invocation.getArgument(0)));
        when(statement.executeQuery(anyString())).thenAnswer(invocation -> {
            executed.add("query " + invocation.getArgument(0));
            return resultSet;
        });
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn("flag");
        when(resultSet.getString(1)).thenReturn("t");
        when(connection.unwrap(PGConnection.class)).thenReturn(pgConnection);
        when(pgConnection.getCopyAPI()).thenReturn(copyManager);
        when(copyManager.copyIn(anyString(), any(InputStream.class), anyInt())).thenAnswer(invocation -> {
            InputStream data = invocation.getArgument(1);
            copied.put(invocation.getArgument(0), new String(data.readAllBytes(), StandardCharsets.UTF_8));
            return 1L;
        });
    }

    private static byte[] gzip(String content) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return bytes.toByteArray();
    }

    private void run(String script) throws Exception {
        new PsqlRunner(inputFiles).run(connection, script);
    }

    @Test
    void run_conditionalsIncludeAndCopies_executedAfterConnect() throws Exception {
        inputFiles.add("static/included.sql", "\\timing on\n\\c osmworld\nCREATE TABLE included();");
        inputFiles.add("static/data.tsv", "1\tx\n");
        inputFiles.add("static/data.tsv.gz", gzip("2\ty\n"));

        run("ALTER SYSTEM SET work_mem = '1MB';\n\\c osmworld\n"
                + "SELECT true AS flag \\gset\n"
                + "\\if :flag\nCREATE TABLE yes();\n\\if false\nCREATE TABLE nested();\n\\else\nCREATE TABLE nested_else();\n\\endif\n"
                + "\\else\nCREATE TABLE no();\n\\i /input/static/missing.sql\n\\endif\n"
                + "\\i /input/static/included.sql\n"
                + "COPY t FROM '/input/static/data.tsv' CSV;\n"
                + "copy g from PROGRAM $$ zcat /input/static/data.tsv.gz$$ CSV;\n");

        assertEquals(List.of("query SELECT true AS flag", "CREATE TABLE yes()", "CREATE TABLE nested_else()",
                "CREATE TABLE included()"), executed);
        assertEquals(Map.of("COPY t FROM STDIN CSV", "1\tx\n", "copy g from STDIN CSV", "2\ty\n"), copied);
        verify(statement, atLeastOnce()).setEscapeProcessing(false);
    }

    @Test
    void run_unsupportedMetaCommands_throw() {
        assertThrows(IllegalArgumentException.class, () -> run("\\set x 1"));
        assertThrows(IllegalArgumentException.class, () -> run("SELECT 1;\n\\gset"));
    }

    @Test
    void run_unbalancedConditionals_throw() {
        assertThrows(IllegalArgumentException.class, () -> run("\\else"));
        assertThrows(IllegalArgumentException.class, () -> run("\\endif"));
        assertThrows(IllegalArgumentException.class, () -> run("\\if true\nSELECT 1;"));
        assertThrows(IllegalArgumentException.class, () -> run("\\if true\n\\else\n\\else\n\\endif"));
    }

    @Test
    void run_unknownVariableOrBoolean_throw() {
        assertThrows(IllegalArgumentException.class, () -> run("\\if :unknown\n\\endif"));
        assertThrows(IllegalArgumentException.class, () -> run("\\if maybe\n\\endif"));
    }

    @Test
    void run_missingInputFile_throws() {
        assertThrows(IllegalArgumentException.class, () -> run("COPY t FROM '/input/static/missing.tsv';"));
    }

    @Test
    void run_failedStatement_messageWithStatementAndSqlState() throws Exception {
        when(statement.execute("DROP TABLE missing")).thenThrow(new SQLException("does not exist", "42P01"));

        SQLException exception = assertThrows(SQLException.class, () -> run("DROP TABLE missing;"));

        assertTrue(exception.getMessage().startsWith("Failed SQL statement: DROP TABLE missing: does not exist"),
                exception.getMessage());
        assertEquals("42P01", exception.getSQLState());
    }

    @Test
    void run_gsetWithoutRows_throws() throws Exception {
        when(resultSet.next()).thenReturn(false);

        SQLException exception = assertThrows(SQLException.class, () -> run("SELECT 1 AS x \\gset"));

        assertTrue(exception.getMessage().contains("returned no rows"), exception.getMessage());
    }

    @Test
    void run_gsetWithSeveralRows_throws() throws Exception {
        when(resultSet.next()).thenReturn(true, true);

        SQLException exception = assertThrows(SQLException.class, () -> run("SELECT 1 AS x \\gset"));

        assertTrue(exception.getMessage().contains("more than one row"), exception.getMessage());
    }

    @Test
    void parseBoolean_psqlValues() {
        for (String value : List.of("t", "true", "YES", "on", "1")) {
            assertTrue(PsqlRunner.parseBoolean(value), value);
        }
        for (String value : List.of("f", "False", "no", "off", "0")) {
            assertFalse(PsqlRunner.parseBoolean(value), value);
        }
        for (String value : List.of("o", "", "maybe")) {
            assertThrows(IllegalArgumentException.class, () -> PsqlRunner.parseBoolean(value), value);
        }
    }

    @Test
    void abbreviate_longMultilineStatement_singleLineWithEllipsis() {
        String abbreviated = PsqlRunner.abbreviate("SELECT\n  " + "x, ".repeat(100) + "1");

        assertTrue(abbreviated.startsWith("SELECT x, x, "), abbreviated);
        assertTrue(abbreviated.endsWith("..."), abbreviated);
        assertEquals(163, abbreviated.length());
        assertEquals("SELECT 1", PsqlRunner.abbreviate("SELECT\n 1"));
    }
}
