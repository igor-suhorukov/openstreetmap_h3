package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.admin.AdminBinder;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminBinding;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminColumns;
import com.github.isuhorukov.osm.pgsnapshot.arrow.ArrowSpill;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowMultipolygon;
import com.github.isuhorukov.osm.pgsnapshot.util.GeometryH33;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

final class MultipolygonBatchProcessor {

    private final MultipolygonTags multipolygonTags;
    private final boolean tsv;
    private final ArrowSpill arrowSpill;
    private final GeometryH33 geometryH33;
    private final AdminBinder adminBinder;
    private final AdminColumns adminColumns;

    MultipolygonBatchProcessor(MultipolygonTags multipolygonTags, boolean tsv, ArrowSpill arrowSpill,
                               GeometryH33 geometryH33) {
        this(multipolygonTags, tsv, arrowSpill, geometryH33, null);
    }

    MultipolygonBatchProcessor(MultipolygonTags multipolygonTags, boolean tsv, ArrowSpill arrowSpill,
                               GeometryH33 geometryH33, AdminBinder adminBinder) {
        this.multipolygonTags = multipolygonTags;
        this.tsv = tsv;
        this.arrowSpill = arrowSpill;
        this.geometryH33 = geometryH33;
        this.adminBinder = adminBinder;
        this.adminColumns = adminBinder != null ? adminBinder.getColumns() : AdminColumns.NONE;
    }

    Result process(MultipolygonRows.Batch batch) {
        Geometry[] geometries = batch.geometries();
        short[][] cells = geometryH33.cells(geometries);
        List<List<Tag>> tags = adminBinder != null || arrowSpill != null ? tags(batch) : null;
        AdminBinding[] bindings = adminBinder != null ? bind(geometries, cells, tags) : null;
        ByteBuffer tsvRows = tsv ? batch.importRows(multipolygonTags, cells, bindings, adminColumns) : null;
        if (arrowSpill == null) {
            return new Result(tsvRows, Collections.emptyMap());
        }
        WKBWriter wkbWriter = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
        List<ArrowMultipolygon> multipolygons = new ArrayList<>(batch.size());
        for (int row = 0; row < batch.size(); row++) {
            multipolygons.add(new ArrowMultipolygon(MultipolygonTags.relationId(batch.bytes(), batch.start(row),
                    batch.end(row)), GeometryH33.h33(cells[row]), wkbWriter.write(geometries[row]), tags.get(row),
                    bindings != null ? bindings[row] : null));
        }
        return new Result(tsvRows, arrowSpill.encodeMultipolygons(multipolygons));
    }

    private List<List<Tag>> tags(MultipolygonRows.Batch batch) {
        List<List<Tag>> tags = new ArrayList<>(batch.size());
        for (int row = 0; row < batch.size(); row++) {
            tags.add(multipolygonTags.tags(batch.bytes(), batch.start(row), batch.end(row)));
        }
        return tags;
    }

    private AdminBinding[] bind(Geometry[] geometries, short[][] cells, List<List<Tag>> tags) {
        AdminBinding[] bindings = new AdminBinding[geometries.length];
        for (int row = 0; row < geometries.length; row++) {
            bindings[row] = adminBinder.multipolygon(GeometryH33.h33(cells[row]), geometries[row], tags.get(row));
        }
        return bindings;
    }

    static final class Result {
        private final ByteBuffer tsvRows;
        private final Map<Short, byte[]> spillChunks;

        private Result(ByteBuffer tsvRows, Map<Short, byte[]> spillChunks) {
            this.tsvRows = tsvRows;
            this.spillChunks = spillChunks;
        }

        ByteBuffer getTsvRows() {
            return tsvRows;
        }

        Map<Short, byte[]> getSpillChunks() {
            return spillChunks;
        }
    }
}
