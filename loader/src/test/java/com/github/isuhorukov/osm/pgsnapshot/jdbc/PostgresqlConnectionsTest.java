package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PostgresqlConnectionsTest {

    @Test
    void constructor_notPostgresqlUrl_throws() {
        Map<String, String> environment = Map.of();
        assertThrows(IllegalArgumentException.class, () -> new PostgresqlConnections(null, environment));
        assertThrows(IllegalArgumentException.class, () -> new PostgresqlConnections("jdbc:duckdb:", environment));
    }

    @Test
    void constructor_invalidUrl_throwsWithoutPassword() {
        Map<String, String> environment = Map.of();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new PostgresqlConnections("jdbc:postgresql://localhost:port/osmworld?password=secret", environment));

        assertFalse(exception.getMessage().contains("secret"), exception.getMessage());
    }

    @Test
    void constructor_passwordEnvironmentVariable_usedWhenUrlHasNoPassword() {
        PostgresqlConnections connections = new PostgresqlConnections("jdbc:postgresql://localhost/osmworld?user=postgres",
                Map.of(PostgresqlConnections.PASSWORD_ENVIRONMENT_VARIABLE, "osmworld"));

        assertEquals("osmworld", connections.getProperties().getProperty("password"));
    }

    @Test
    void constructor_passwordInUrl_environmentVariableIgnored() {
        PostgresqlConnections connections = new PostgresqlConnections(
                "jdbc:postgresql://localhost/osmworld?user=postgres&password=fromurl",
                Map.of(PostgresqlConnections.PASSWORD_ENVIRONMENT_VARIABLE, "osmworld"));

        assertNull(connections.getProperties().getProperty("password"));
    }

    @Test
    void open_unreachableServer_throwsSqlException() {
        PostgresqlConnections connections = new PostgresqlConnections(
                "jdbc:postgresql://127.0.0.1:1/osmworld?connectTimeout=2", Map.of());

        assertThrows(SQLException.class, () -> connections.open("test"));
    }
}
