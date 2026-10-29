package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.ExternalProcessing;
import com.github.isuhorukov.osm.pgsnapshot.PipedProcess;
import com.github.isuhorukov.osm.pgsnapshot.output.ResourceCopier;
import com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

final class MultipolygonSubset {

    private static final Logger log = LoggerFactory.getLogger(MultipolygonSubset.class);
    private static final String SUBSET_FILE = "relations.osm.pbf";
    private static final FileAttribute<Set<PosixFilePermission>> OWNER_ONLY =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));

    private final ExternalProcessing externalProcessing;
    private final MultipolygonTags multipolygonTags;
    private final int workers;
    private final AtomicReference<PipedProcess> export = new AtomicReference<>();
    private volatile boolean cancelled;

    MultipolygonSubset(ExternalProcessing externalProcessing, MultipolygonTags multipolygonTags, int workers) {
        this.externalProcessing = externalProcessing;
        this.multipolygonTags = multipolygonTags;
        this.workers = workers;
    }

    long copy(Connection connection, PsqlRunner runner, File sourcePbfFile)
            throws IOException, InterruptedException, SQLException {
        long start = System.currentTimeMillis();
        runner.run(connection, ResourceCopier.updateScript(ResourceCopier.UPDATE_AFFECTED_RELATIONS_SQL));
        List<Long> relationIds = areaRelations(connection);
        log.info("{} affected area relations found in {} ms", relationIds.size(), System.currentTimeMillis() - start);
        if (relationIds.isEmpty()) {
            return 0;
        }
        Path directory = Files.createTempDirectory("osm-update-multipolygon", OWNER_ONLY);
        File subset = directory.resolve(SUBSET_FILE).toFile();
        try {
            externalProcessing.extractRelations(sourcePbfFile, relationIds, subset);
            checkCancelled();
            long rows;
            try (PipedProcess subsetExport = externalProcessing.startMultipolygonExport(subset)) {
                export.set(subsetExport);
                checkCancelled();
                rows = MultipolygonCopy.copyExport(connection, subsetExport, multipolygonTags, workers);
            } finally {
                export.set(null);
            }
            log.info("{} multipolygon relations of the subset copied in {} ms", rows, System.currentTimeMillis() - start);
            return rows;
        } finally {
            Files.deleteIfExists(subset.toPath());
            Files.deleteIfExists(directory);
        }
    }

    void cancel() {
        cancelled = true;
        PipedProcess subsetExport = export.get();
        if (subsetExport != null) {
            subsetExport.close();
        }
    }

    private void checkCancelled() throws InterruptedException {
        if (cancelled) {
            throw new InterruptedException("multipolygon export cancelled");
        }
    }

    private static List<Long> areaRelations(Connection connection) throws IOException, SQLException {
        List<Long> relationIds = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     ResourceCopier.resource("/" + ResourceCopier.UPDATE_AREA_RELATIONS_SQL))) {
            while (resultSet.next()) {
                relationIds.add(resultSet.getLong(1));
            }
        }
        return relationIds;
    }
}
