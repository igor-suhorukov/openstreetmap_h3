package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPInputStream;

final class PsqlRunner {

    private static final Logger log = LoggerFactory.getLogger(PsqlRunner.class);
    private static final int MAX_LOGGED_SQL_LENGTH = 160;
    private static final long SLOW_STATEMENT_MILLIS = 1000;
    private static final int COPY_BUFFER_SIZE = 1 << 16;

    private final InputFiles inputFiles;
    private final Map<String, String> variables = new HashMap<>();

    PsqlRunner(InputFiles inputFiles) {
        this.inputFiles = inputFiles;
    }

    void run(Connection connection, String script) throws SQLException, IOException {
        run(connection, PsqlScript.parse(script).afterLastConnect());
    }

    void run(Connection connection, List<PsqlCommand> commands) throws SQLException, IOException {
        Deque<Branch> branches = new ArrayDeque<>();
        for (PsqlCommand command : commands) {
            if (command.isMeta()) {
                meta(connection, command, branches);
            } else if (isActive(branches)) {
                execute(connection, command);
            }
        }
        if (!branches.isEmpty()) {
            throw new IllegalArgumentException("\\if without \\endif");
        }
    }

    static CopyManager copyManager(Connection connection) throws SQLException {
        return connection.unwrap(PGConnection.class).getCopyAPI();
    }

    static String abbreviate(String sql) {
        String singleLine = sql.replaceAll("\\s+", " ");
        return singleLine.length() <= MAX_LOGGED_SQL_LENGTH ? singleLine : singleLine.substring(0, MAX_LOGGED_SQL_LENGTH) + "...";
    }

    static boolean parseBoolean(String value) {
        String text = value.trim().toLowerCase(Locale.ROOT);
        if (!text.isEmpty()) {
            if ("true".startsWith(text) || "yes".startsWith(text) || "1".equals(text)
                    || (text.length() > 1 && "on".startsWith(text))) {
                return true;
            }
            if ("false".startsWith(text) || "no".startsWith(text) || "0".equals(text)
                    || (text.length() > 1 && "off".startsWith(text))) {
                return false;
            }
        }
        throw new IllegalArgumentException("Unrecognized boolean value: " + value);
    }

    private void meta(Connection connection, PsqlCommand command, Deque<Branch> branches)
            throws SQLException, IOException {
        switch (command.getMetaCommand()) {
            case "if":
                branches.push(newBranch(isActive(branches), command.getArgument()));
                break;
            case "else":
                requireBranch(branches, command).toElse();
                break;
            case "endif":
                requireBranch(branches, command);
                branches.pop();
                break;
            case "i":
            case "include":
                if (isActive(branches)) {
                    run(connection, inputFiles.read(command.getArgument()));
                }
                break;
            case "timing":
            case "c":
            case "connect":
                break;
            default:
                throw new IllegalArgumentException("Unsupported psql meta-command " + command);
        }
    }

    private Branch newBranch(boolean parentActive, String argument) {
        return new Branch(parentActive, parentActive && booleanArgument(argument));
    }

    private boolean booleanArgument(String argument) {
        if (!argument.startsWith(":")) {
            return parseBoolean(argument);
        }
        String value = variables.get(argument.substring(1));
        if (value == null) {
            throw new IllegalArgumentException("psql variable is not set: " + argument);
        }
        return parseBoolean(value);
    }

    private static Branch requireBranch(Deque<Branch> branches, PsqlCommand command) {
        if (branches.isEmpty()) {
            throw new IllegalArgumentException(command + " without \\if");
        }
        return branches.peek();
    }

    private static boolean isActive(Deque<Branch> branches) {
        return branches.isEmpty() || branches.peek().active;
    }

    static void executeSql(Connection connection, String sql) throws SQLException {
        long start = System.currentTimeMillis();
        try (Statement statement = connection.createStatement()) {
            statement.setEscapeProcessing(false);
            statement.execute(sql);
        } catch (SQLException e) {
            throw failed(sql, e);
        }
        logTime(start, sql);
    }

    private void execute(Connection connection, PsqlCommand command) throws SQLException, IOException {
        String sql = command.getSql();
        CopyStatement copy = CopyStatement.parse(sql);
        if (copy == null && !command.isGset()) {
            executeSql(connection, sql);
            return;
        }
        long start = System.currentTimeMillis();
        try {
            if (copy != null) {
                copy(connection, copy);
            } else {
                gset(connection, sql);
            }
        } catch (SQLException e) {
            throw failed(sql, e);
        }
        logTime(start, sql);
    }

    private static SQLException failed(String sql, SQLException e) {
        return new SQLException("Failed SQL statement: " + abbreviate(sql) + ": " + e.getMessage(), e.getSQLState(), e);
    }

    private static void logTime(long start, String sql) {
        long time = System.currentTimeMillis() - start;
        boolean slow = time >= SLOW_STATEMENT_MILLIS;
        if (slow && log.isInfoEnabled()) {
            log.info("{} ms: {}", time, abbreviate(sql));
        } else if (!slow && log.isDebugEnabled()) {
            log.debug("{} ms: {}", time, abbreviate(sql));
        }
    }

    private void copy(Connection connection, CopyStatement copy) throws SQLException, IOException {
        try (InputStream source = inputFiles.open(copy.getInputPath());
             InputStream data = copy.isGzip() ? new GZIPInputStream(source, COPY_BUFFER_SIZE) : source) {
            copyManager(connection).copyIn(copy.getSql(), data, COPY_BUFFER_SIZE);
        }
    }

    private void gset(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                throw new SQLException("\\gset query returned no rows");
            }
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int column = 1; column <= metaData.getColumnCount(); column++) {
                variables.put(metaData.getColumnLabel(column), resultSet.getString(column));
            }
            if (resultSet.next()) {
                throw new SQLException("\\gset query returned more than one row");
            }
        }
    }

    private static final class Branch {
        private final boolean parentActive;
        private boolean active;
        private boolean taken;
        private boolean inElse;

        private Branch(boolean parentActive, boolean active) {
            this.parentActive = parentActive;
            this.active = active;
            this.taken = active;
        }

        private void toElse() {
            if (inElse) {
                throw new IllegalArgumentException("\\else after \\else");
            }
            inElse = true;
            active = parentActive && !taken;
            taken = true;
        }
    }
}
