package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;

final class ScriptCopyWorker extends CopyWorker {

    private final StreamedScript script;
    private final PsqlRunner runner;

    ScriptCopyWorker(String name, Connection connection, int queueCapacity, long pendingBytesLimit,
                     Consumer<Throwable> failureListener, StreamedScript script, PsqlRunner runner) throws SQLException {
        super(name, connection, queueCapacity, pendingBytesLimit, failureListener);
        this.script = script;
        this.runner = runner;
    }

    @Override
    protected void begin() throws SQLException, IOException {
        runner.run(getConnection(), script.getPrefix());
    }

    @Override
    protected String copySql(String target) {
        String copySql = script.getCopySqlByInputPath().get(target);
        if (copySql == null) {
            throw new IllegalArgumentException("Script has no COPY from " + target);
        }
        return copySql;
    }

    @Override
    protected void finish() throws SQLException, IOException {
        runner.run(getConnection(), script.getSuffix());
    }
}
