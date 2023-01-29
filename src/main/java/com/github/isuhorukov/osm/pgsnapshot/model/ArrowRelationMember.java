package com.github.isuhorukov.osm.pgsnapshot.model;

public class ArrowRelationMember {
    long memberId;
    char memberType;
    String memberRole;

    public ArrowRelationMember(long memberId, char memberType, String memberRole) {
        this.memberId = memberId;
        this.memberType = memberType;
        this.memberRole = memberRole;
    }

    public long getMemberId() {
        return memberId;
    }

    public char getMemberType() {
        return memberType;
    }

    public String getMemberRole() {
        return memberRole;
    }
}
