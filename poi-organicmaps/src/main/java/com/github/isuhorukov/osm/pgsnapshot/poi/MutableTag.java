package com.github.isuhorukov.osm.pgsnapshot.poi;

final class MutableTag {
    final String key;
    String value;

    MutableTag(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
