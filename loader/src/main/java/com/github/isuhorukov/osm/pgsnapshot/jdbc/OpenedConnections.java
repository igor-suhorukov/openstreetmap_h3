package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

final class OpenedConnections {

    private static final Logger log = LoggerFactory.getLogger(OpenedConnections.class);

    private final PostgresqlConnections connections;
    private final List<Connection> opened = new CopyOnWriteArrayList<>();

    OpenedConnections(PostgresqlConnections connections) {
        this.connections = connections;
    }

    Connection open(String purpose) throws SQLException {
        Connection connection = connections.open(purpose);
        opened.add(connection);
        return connection;
    }

    void closeAll(boolean abort) {
        for (Connection connection : opened) {
            try {
                if (abort) {
                    connection.abort(Runnable::run);
                } else {
                    connection.close();
                }
            } catch (SQLException e) {
                log.warn("Failed to close PostgreSQL connection", e);
            }
        }
    }
}
