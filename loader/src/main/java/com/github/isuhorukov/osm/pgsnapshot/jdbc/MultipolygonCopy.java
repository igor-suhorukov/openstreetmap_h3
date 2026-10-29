package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.PipedProcess;
import com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonRows;
import com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonTags;
import org.postgresql.copy.CopyIn;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;

final class MultipolygonCopy {

    static final String COPY_SQL = "COPY preimport_multipolygon FROM STDIN";

    private MultipolygonCopy() {
    }

    static long copyRelations(InputStream osmiumOutput, CopyIn copyIn, MultipolygonTags multipolygonTags)
            throws IOException, SQLException {
        return MultipolygonRows.copyRelations(osmiumOutput, copyIn::writeToCopy, multipolygonTags);
    }

    static long copyExport(Connection connection, PipedProcess export, MultipolygonTags multipolygonTags)
            throws IOException, SQLException, InterruptedException {
        CopyIn copyIn = PsqlRunner.copyManager(connection).copyIn(COPY_SQL);
        long rows = copyRelations(export.getInputStream(), copyIn, multipolygonTags);
        export.waitForSuccess();
        copyIn.endCopy();
        return rows;
    }
}
