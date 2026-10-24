package com.github.isuhorukov.osm.pgsnapshot.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ArrowRelation {
    long id;
    Map<String, String> tags;
    List<ArrowRelationMember> relationMembers = new ArrayList<>();

    public ArrowRelation(long id, Map<String, String> tags) {
        this.id = id;
        this.tags = tags;
    }

    public long getId() {
        return id;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public List<ArrowRelationMember> getRelationMembers() {
        return relationMembers;
    }
}
