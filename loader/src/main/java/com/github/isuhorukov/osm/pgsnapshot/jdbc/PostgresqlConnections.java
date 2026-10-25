package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import org.postgresql.Driver;
import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

final class PostgresqlConnections {

    static final String URL_PREFIX = "jdbc:postgresql:";
    static final String PASSWORD_ENVIRONMENT_VARIABLE = "PGPASSWORD";
    private static final String APPLICATION_NAME = "osm-to-pgsnapshot";

    private final Driver driver = new Driver();
    private final String url;
    private final Properties properties = new Properties();

    PostgresqlConnections(String url, Map<String, String> environment) {
        if (url == null || !url.startsWith(URL_PREFIX)) {
            throw new IllegalArgumentException("pg_jdbc_url should start with " + URL_PREFIX);
        }
        Properties urlProperties = Driver.parseURL(url, null);
        if (urlProperties == null) {
            throw new IllegalArgumentException("Invalid pg_jdbc_url " + CliParameters.maskPassword(url));
        }
        this.url = url;
        String password = environment.get(PASSWORD_ENVIRONMENT_VARIABLE);
        if (!PGProperty.PASSWORD.isPresent(urlProperties) && password != null) {
            PGProperty.PASSWORD.set(properties, password);
        }
    }

    Connection open(String purpose) throws SQLException {
        Properties connectionProperties = new Properties();
        connectionProperties.putAll(properties);
        PGProperty.APPLICATION_NAME.set(connectionProperties, APPLICATION_NAME + " " + purpose);
        Connection connection = driver.connect(url, connectionProperties);
        if (connection == null) {
            throw new SQLException("PostgreSQL driver does not accept " + CliParameters.maskPassword(url));
        }
        return connection;
    }

    Properties getProperties() {
        return properties;
    }
}
