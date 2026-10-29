package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.arrow.ArrowSpill;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowMultipolygon;
import com.github.isuhorukov.osm.pgsnapshot.util.GeometryH33;
import com.uber.h3core.H3Core;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
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
    private final H3Core h3Core;

    MultipolygonBatchProcessor(MultipolygonTags multipolygonTags, boolean tsv, ArrowSpill arrowSpill, H3Core h3Core) {
        this.multipolygonTags = multipolygonTags;
        this.tsv = tsv;
        this.arrowSpill = arrowSpill;
        this.h3Core = h3Core;
    }

    Result process(MultipolygonRows.Batch batch) {
        ByteBuffer tsvRows = tsv ? batch.enrichedRows(multipolygonTags) : null;
        if (arrowSpill == null) {
            return new Result(tsvRows, Collections.emptyMap());
        }
        WKBReader wkbReader = new WKBReader();
        WKBWriter wkbWriter = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
        List<ArrowMultipolygon> multipolygons = new ArrayList<>(batch.size());
        for (int row = 0; row < batch.size(); row++) {
            multipolygons.add(multipolygon(batch.bytes(), batch.start(row), batch.end(row), wkbReader, wkbWriter));
        }
        return new Result(tsvRows, arrowSpill.encodeMultipolygons(multipolygons));
    }

    private ArrowMultipolygon multipolygon(byte[] row, int rowStart, int rowEnd, WKBReader wkbReader,
                                           WKBWriter wkbWriter) {
        long id = MultipolygonTags.relationId(row, rowStart, rowEnd);
        int wkbEnd = MultipolygonRows.indexOf(row, rowStart, rowEnd, (byte) '\t');
        Geometry geometry;
        try {
            geometry = wkbReader.read(hexBytes(row, rowStart, wkbEnd, id));
        } catch (ParseException e) {
            throw new IllegalStateException("Invalid WKB of multipolygon " + id, e);
        }
        return new ArrowMultipolygon(id, GeometryH33.of(geometry, h3Core), wkbWriter.write(geometry),
                multipolygonTags.tags(row, rowStart, rowEnd));
    }

    static byte[] hexBytes(byte[] row, int start, int end, long id) {
        if ((end - start) % 2 != 0) {
            throw new IllegalStateException("Odd length of hex WKB of multipolygon " + id);
        }
        byte[] bytes = new byte[(end - start) / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (hexDigit(row[start + 2 * i], id) << 4 | hexDigit(row[start + 2 * i + 1], id));
        }
        return bytes;
    }

    private static int hexDigit(byte digit, long id) {
        if (digit >= '0' && digit <= '9') {
            return digit - '0';
        }
        if (digit >= 'A' && digit <= 'F') {
            return digit - 'A' + 10;
        }
        if (digit >= 'a' && digit <= 'f') {
            return digit - 'a' + 10;
        }
        throw new IllegalStateException("Invalid hex WKB of multipolygon " + id);
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
