package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.arrow.ArrowSpill;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowMultipolygon;
import com.github.isuhorukov.osm.pgsnapshot.util.GeometryH33;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

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

    MultipolygonBatchProcessor(MultipolygonTags multipolygonTags, boolean tsv, ArrowSpill arrowSpill,
                               GeometryH33 geometryH33) {
        this.multipolygonTags = multipolygonTags;
        this.tsv = tsv;
        this.arrowSpill = arrowSpill;
        this.geometryH33 = geometryH33;
    }

    Result process(MultipolygonRows.Batch batch) {
        Geometry[] geometries = batch.geometries();
        short[][] cells = geometryH33.cells(geometries);
        ByteBuffer tsvRows = tsv ? batch.importRows(multipolygonTags, cells) : null;
        if (arrowSpill == null) {
            return new Result(tsvRows, Collections.emptyMap());
        }
        WKBWriter wkbWriter = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
        List<ArrowMultipolygon> multipolygons = new ArrayList<>(batch.size());
        for (int row = 0; row < batch.size(); row++) {
            int rowStart = batch.start(row);
            int rowEnd = batch.end(row);
            multipolygons.add(new ArrowMultipolygon(MultipolygonTags.relationId(batch.bytes(), rowStart, rowEnd),
                    GeometryH33.h33(cells[row]), wkbWriter.write(geometries[row]),
                    multipolygonTags.tags(batch.bytes(), rowStart, rowEnd)));
        }
        return new Result(tsvRows, arrowSpill.encodeMultipolygons(multipolygons));
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
