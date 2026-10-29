package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.TopologyException;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

import java.util.Collection;
import java.util.List;

public final class AdminBinder {

    private final AdminDirectory directory;
    private final CompactIndex index;
    private final AdminCellCollector cells = new AdminCellCollector();
    private final AdminReport report = new AdminReport();

    AdminBinder(AdminDirectory directory) {
        this.directory = directory;
        this.index = directory.getIndex();
    }

    public AdminColumns getColumns() {
        return directory.getOptions().getColumns();
    }

    public AdminBinding node(short h33, double longitude, double latitude, Collection<Tag> tags) {
        if (!AdminObjectFilter.isBound(tags)) {
            return null;
        }
        return result(AdminReport.Kind.NODES, h33, index.point(longitude, latitude));
    }

    public AdminBinding way(short h33, List<WayNode> wayNodes, Collection<Tag> tags) {
        if (wayNodes.isEmpty() || !AdminObjectFilter.isBound(tags)) {
            return null;
        }
        Coordinate[] coordinates = new Coordinate[wayNodes.size()];
        Envelope envelope = new Envelope();
        for (int i = 0; i < coordinates.length; i++) {
            WayNode wayNode = wayNodes.get(i);
            coordinates[i] = new Coordinate(wayNode.getLongitude(), wayNode.getLatitude());
            envelope.expandToInclude(coordinates[i]);
        }
        boolean closed = coordinates.length >= 4 && coordinates[0].equals2D(coordinates[coordinates.length - 1]);
        UnitShares shares;
        try {
            if (closed) {
                shares = index.geometry(GridGeometry.FACTORY.createPolygon(coordinates), true);
            } else if (coordinates.length >= 2) {
                shares = index.geometry(GridGeometry.FACTORY.createLineString(coordinates), false);
            } else {
                shares = index.point(coordinates[0].x, coordinates[0].y);
            }
        } catch (TopologyException | IllegalArgumentException e) {
            report.fallback(AdminReport.Kind.WAYS);
            shares = index.point(envelope.centre().x, envelope.centre().y);
        }
        return result(AdminReport.Kind.WAYS, h33, shares);
    }

    public AdminBinding multipolygon(short h33, Geometry geometry, Collection<Tag> tags) {
        if (geometry == null || geometry.isEmpty() || !AdminObjectFilter.isBound(tags)) {
            return null;
        }
        UnitShares shares;
        try {
            shares = index.geometry(geometry, true);
        } catch (TopologyException | IllegalArgumentException e) {
            report.fallback(AdminReport.Kind.MULTIPOLYGONS);
            Coordinate centre = geometry.getEnvelopeInternal().centre();
            shares = index.point(centre.x, centre.y);
        }
        return result(AdminReport.Kind.MULTIPOLYGONS, h33, shares);
    }

    private AdminBinding result(AdminReport.Kind kind, short h33, UnitShares shares) {
        report.count(kind, shares.size());
        if (shares.size() == 0) {
            return null;
        }
        long[] ids = new long[shares.size()];
        float[] values = new float[shares.size()];
        for (int i = 0; i < ids.length; i++) {
            int unit = shares.unit(i);
            cells.add(unit, h33);
            ids[i] = directory.unitId(unit);
            values[i] = shares.share(i);
        }
        return new AdminBinding(ids, values);
    }

    AdminReport getReport() {
        return report;
    }

    AdminCellCollector getCells() {
        return cells;
    }
}
