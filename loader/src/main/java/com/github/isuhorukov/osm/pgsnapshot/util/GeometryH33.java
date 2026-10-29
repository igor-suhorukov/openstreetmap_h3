package com.github.isuhorukov.osm.pgsnapshot.util;

import com.uber.h3core.H3Core;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Geometry;

public final class GeometryH33 {

    public static final short MULTIPLE_CELLS = Short.MAX_VALUE;

    private GeometryH33() {
    }

    public static short of(Geometry geometry, H3Core h3Core) {
        SingleCellFilter filter = new SingleCellFilter(h3Core);
        geometry.apply(filter);
        return filter.h33;
    }

    private static final class SingleCellFilter implements CoordinateSequenceFilter {
        private final H3Core h3Core;
        private boolean first = true;
        private boolean done;
        private short h33 = MULTIPLE_CELLS;

        private SingleCellFilter(H3Core h3Core) {
            this.h3Core = h3Core;
        }

        @Override
        public void filter(CoordinateSequence sequence, int index) {
            short cell = CompactH3.serialize3(h3Core.latLngToCell(sequence.getY(index), sequence.getX(index), 3));
            if (first) {
                h33 = cell;
                first = false;
            } else if (cell != h33) {
                h33 = MULTIPLE_CELLS;
                done = true;
            }
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public boolean isGeometryChanged() {
            return false;
        }
    }
}
