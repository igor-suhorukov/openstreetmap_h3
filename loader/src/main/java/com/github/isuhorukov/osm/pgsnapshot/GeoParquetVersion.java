package com.github.isuhorukov.osm.pgsnapshot;

public enum GeoParquetVersion {
    V1, BOTH, V2, BLOB;

    public boolean isGeometry() {
        return this != BLOB;
    }
}
