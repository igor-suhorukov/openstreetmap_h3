package com.github.isuhorukov.osm.pgsnapshot.admin;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.ExternalProcessing;
import com.github.isuhorukov.osm.pgsnapshot.PipedProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public final class AdminUnits {

    public static final String METADATA_KEY = "openstreetmap_h3:admin_units";
    public static final String OUTPUT_DIRECTORY = "admin_units";
    static final String ADMIN_PBF = "admin.osm.pbf";
    private static final Logger log = LoggerFactory.getLogger(AdminUnits.class);
    private static final String SHA_256 = "SHA-256";

    private final AdminDirectory directory;
    private final AdminBinder binder;
    private boolean finished;

    AdminUnits(AdminDirectory directory) {
        this.directory = directory;
        this.binder = new AdminBinder(directory);
    }

    public static AdminOptions options(CliParameters parameters) {
        return new AdminOptions(parameters.getNameLang(), parameters.getParentShare(), parameters.getSubareaShare(),
                parameters.getMinShare(), parameters.getMinAreaM2(), parameters.getMinLengthM(),
                AdminColumns.of(parameters.isAdminUnits(), parameters.isAdminUnitShares()));
    }

    public static AdminUnits prepare(CliParameters parameters, ExternalProcessing externalProcessing,
                                     File sourcePbfFile) throws IOException, InterruptedException {
        AdminOptions options = options(parameters);
        long start = System.currentTimeMillis();
        MessageDigest relationsDigest = digest();
        MessageDigest polygonsDigest = digest();
        AdminSource source;
        Path prepass = Files.createTempDirectory(sourcePbfFile.getAbsoluteFile().getParentFile().toPath(),
                "admin_units_prepass");
        File adminPbf = prepass.resolve(ADMIN_PBF).toFile();
        try {
            externalProcessing.extractAdminBoundaries(sourcePbfFile, adminPbf);
            try (PipedProcess relations = externalProcessing.startRelationsOpl(adminPbf)) {
                source = AdminSource.readRelations(relations.getInputStream(), relationsDigest);
                relations.waitForSuccess();
            }
            try (PipedProcess polygons = externalProcessing.startMultipolygonExport(adminPbf)) {
                source.readPolygons(polygons.getInputStream(), polygonsDigest);
                polygons.waitForSuccess();
            }
        } finally {
            Files.deleteIfExists(adminPbf.toPath());
            Files.deleteIfExists(prepass);
        }
        String fingerprint = fingerprint(relationsDigest.digest(), polygonsDigest.digest(), options);
        log.info("admin unit prepass: {} units in {} ms, fingerprint {}", source.getUnits().size(),
                System.currentTimeMillis() - start, fingerprint);
        Path cache = parameters.getAdminUnitsCache() != null ? new File(parameters.getAdminUnitsCache()).toPath()
                : null;
        return new AdminUnits(build(source.getUnits(), options, fingerprint, cache, parameters.getWorkers()));
    }

    static AdminDirectory build(List<AdminUnit> units, AdminOptions options, String fingerprint, Path cache,
                                int workers) throws IOException, InterruptedException {
        try (WorkerPool pool = new WorkerPool(workers)) {
            CompactIndex index = cache != null ? AdminCache.load(cache, fingerprint, units, options) : null;
            if (index != null) {
                DirectoryBuilder.parseGeometries(units, null, pool);
                DirectoryBuilder.assignPaths(units);
            } else {
                DirectoryBuilder.OwnPieces own = DirectoryBuilder.build(units, options, pool);
                index = CompactIndex.of(units, options, own.getPolygons(), own.getUnits());
                if (cache != null) {
                    AdminCache.save(cache, fingerprint, units, index);
                }
            }
            return new AdminDirectory(units, index, options, fingerprint);
        }
    }

    static String fingerprint(byte[] relations, byte[] polygons, AdminOptions options) {
        MessageDigest digest = digest();
        digest.update(relations);
        digest.update(polygons);
        digest.update(options.fingerprintParameters().getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder("sha256:");
        for (byte b : digest.digest()) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance(SHA_256);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public AdminDirectory getDirectory() {
        return directory;
    }

    public AdminBinder getBinder() {
        return binder;
    }

    public AdminColumns getColumns() {
        return directory.getOptions().getColumns();
    }

    public long boundObjects() {
        return binder.getReport().boundObjects();
    }

    public synchronized void finish() {
        if (!finished) {
            binder.getCells().assignCells(directory.getUnits());
            finished = true;
        }
    }

    public String metadataJson() {
        return metadataJson(false);
    }

    public String reportMetadataJson() {
        return metadataJson(true);
    }

    private String metadataJson(boolean report) {
        AdminOptions options = directory.getOptions();
        StringBuilder json = new StringBuilder("{\"version\":").append(AdminOptions.KEY_VERSION)
                .append(",\"rules_version\":").append(AdminOptions.RULES_VERSION).append(",\"fingerprint\":");
        Json.string(json, directory.getFingerprint());
        json.append(",\"columns\":{\"units\":");
        Json.string(json, AdminColumns.UNITS_COLUMN);
        json.append(",\"shares\":");
        Json.string(json, options.getColumns().hasShares() ? AdminColumns.SHARES_COLUMN : null);
        json.append("},\"directory\":");
        options.appendDirectoryJson(json);
        json.append(",\"binding\":");
        options.appendBindingJson(json);
        if (report) {
            json.append(",\"report\":{\"units\":").append(directory.getUnits().size())
                    .append(",\"units_with_geometry\":").append(directory.unitsWithGeometry())
                    .append(",\"regions\":").append(directory.regionCount()).append(",\"objects\":");
            binder.getReport().appendJson(json);
            json.append('}');
        }
        return json.append('}').toString();
    }
}
