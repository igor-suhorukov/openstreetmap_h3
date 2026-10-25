package com.github.isuhorukov.osm.pgsnapshot.jdbc;

public class UpdateRefusedException extends IllegalStateException {

    public UpdateRefusedException(String message) {
        super(message);
    }
}
