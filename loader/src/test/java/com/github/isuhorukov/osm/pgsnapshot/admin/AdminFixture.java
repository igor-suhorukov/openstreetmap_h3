package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

final class AdminFixture {

    static final double LON = 98.0;
    static final double LAT = 18.0;

    private final StringBuilder opl = new StringBuilder();
    private final StringBuilder polygons = new StringBuilder();

    AdminFixture unit(long id, int level, String wkt, String... members) {
        relation(id, "boundary=administrative,admin_level=" + level + ",name=unit" + id + ",name:en=Unit%20%" + id,
                members);
        Geometry geometry = read(wkt);
        MultiPolygon multiPolygon = geometry instanceof Polygon
                ? GridGeometry.FACTORY.createMultiPolygon(new Polygon[]{(Polygon) geometry}) : (MultiPolygon) geometry;
        multiPolygon.setSRID(4326);
        polygons.append(WKBWriter.toHex(new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN, true).write(multiPolygon)))
                .append("\trelation\t").append(id).append("\t\"boundary\"=>\"administrative\"\n");
        return this;
    }

    AdminFixture unitWithoutPolygon(long id, int level, String... members) {
        relation(id, "boundary=administrative,admin_level=" + level + ",name=unit" + id, members);
        return this;
    }

    AdminFixture relation(long id, String tags, String... members) {
        opl.append('r').append(id).append(" v1 dV c1 t2025-01-01T00:00:00Z i1 utest T").append(tags).append(" M")
                .append(String.join(",", members)).append('\n');
        return this;
    }

    static String box(double minX, double minY, double maxX, double maxY) {
        return String.format(Locale.ROOT, "POLYGON ((%.7f %.7f, %.7f %.7f, %.7f %.7f, %.7f %.7f, %.7f %.7f))",
                LON + minX, LAT + minY, LON + maxX, LAT + minY, LON + maxX, LAT + maxY, LON + minX, LAT + maxY,
                LON + minX, LAT + minY);
    }

    static Geometry read(String wkt) {
        try {
            return new WKTReader(GridGeometry.FACTORY).read(wkt);
        } catch (ParseException e) {
            throw new IllegalArgumentException(wkt, e);
        }
    }

    AdminUnits build(AdminOptions options) {
        return build(options, null);
    }

    AdminUnits build(AdminOptions options, Path cache) {
        try {
            MessageDigest relationsDigest = MessageDigest.getInstance("SHA-256");
            MessageDigest polygonsDigest = MessageDigest.getInstance("SHA-256");
            AdminSource source = AdminSource.readRelations(
                    new ByteArrayInputStream(opl.toString().getBytes(StandardCharsets.UTF_8)), relationsDigest);
            source.readPolygons(new ByteArrayInputStream(polygons.toString().getBytes(StandardCharsets.UTF_8)),
                    polygonsDigest);
            String fingerprint = AdminUnits.fingerprint(relationsDigest.digest(), polygonsDigest.digest(), options);
            return new AdminUnits(AdminUnits.build(source.getUnits(), options, fingerprint, cache, 2));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
