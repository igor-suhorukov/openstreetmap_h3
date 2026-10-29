package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.admin.AdminBinding;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminColumns;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;

final class AdminArrowColumns {

    private final ListVector units;
    private final ListVector shares;

    private AdminArrowColumns(ListVector units, ListVector shares) {
        this.units = units;
        this.shares = shares;
    }

    static AdminArrowColumns of(VectorSchemaRoot root) {
        FieldVector units = root.getVector(AdminColumns.UNITS_COLUMN);
        if (units == null) {
            return null;
        }
        AdminArrowColumns columns = new AdminArrowColumns((ListVector) units,
                (ListVector) root.getVector(AdminColumns.SHARES_COLUMN));
        columns.units.allocateNew();
        if (columns.shares != null) {
            columns.shares.allocateNew();
        }
        return columns;
    }

    void write(int index, AdminBinding binding) {
        if (binding == null) {
            units.setNull(index);
            if (shares != null) {
                shares.setNull(index);
            }
            return;
        }
        long[] unitIds = binding.getUnitIds();
        int unitStart = units.startNewValue(index);
        BigIntVector unitValues = (BigIntVector) units.getDataVector();
        for (int i = 0; i < unitIds.length; i++) {
            unitValues.setSafe(unitStart + i, unitIds[i]);
        }
        units.endValue(index, unitIds.length);
        if (shares != null) {
            float[] values = binding.getShares();
            int shareStart = shares.startNewValue(index);
            Float4Vector shareValues = (Float4Vector) shares.getDataVector();
            for (int i = 0; i < values.length; i++) {
                shareValues.setSafe(shareStart + i, values[i]);
            }
            shares.endValue(index, values.length);
        }
    }

    void setValueCount(int count) {
        units.setValueCount(count);
        if (shares != null) {
            shares.setValueCount(count);
        }
    }
}
