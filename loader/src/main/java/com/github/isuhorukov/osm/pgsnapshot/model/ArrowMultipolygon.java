package com.github.isuhorukov.osm.pgsnapshot.model;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collection;
import java.util.Map;

public class ArrowMultipolygon {
    private final long id;
    private final short h33;
    private final byte[] wkb;
    private final Map<String, String> tags;

    public ArrowMultipolygon(long id, short h33, byte[] wkb, Collection<Tag> entityTags) {
        this.id = id;
        this.h33 = h33;
        this.wkb = wkb;
        this.tags = TagsUtil.tagsToMap(entityTags);
    }

    public long getId() {
        return id;
    }

    public short getH33() {
        return h33;
    }

    public byte[] getWkb() {
        return wkb;
    }

    public Map<String, String> getTags() {
        return tags;
    }
}
