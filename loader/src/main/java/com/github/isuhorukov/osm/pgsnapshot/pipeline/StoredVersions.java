package com.github.isuhorukov.osm.pgsnapshot.pipeline;

public interface StoredVersions {

    StoredVersions NONE = new StoredVersions() {
        @Override
        public boolean unchangedNode(long id, int version) {
            return false;
        }

        @Override
        public boolean unchangedWay(long id, int version, long geomHash) {
            return false;
        }

        @Override
        public boolean unchangedRelation(long id, int version) {
            return false;
        }
    };

    boolean unchangedNode(long id, int version);

    boolean unchangedWay(long id, int version, long geomHash);

    boolean unchangedRelation(long id, int version);
}
