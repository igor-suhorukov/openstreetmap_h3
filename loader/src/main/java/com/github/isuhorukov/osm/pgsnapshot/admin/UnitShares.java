package com.github.isuhorukov.osm.pgsnapshot.admin;

final class UnitShares {

    static final UnitShares NONE = new UnitShares(new int[0], new float[0]);

    private final int[] units;
    private final float[] shares;

    UnitShares(int[] units, float[] shares) {
        this.units = units;
        this.shares = shares;
    }

    int size() {
        return units.length;
    }

    int unit(int position) {
        return units[position];
    }

    float share(int position) {
        return shares[position];
    }
}
