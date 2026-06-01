package com.github.isuhorukov.osm.pgsnapshot;

import net.postgis.jdbc.geometry.LineString;
import net.postgis.jdbc.geometry.Point;
import net.postgis.jdbc.geometry.Polygon;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collection;
import java.util.Set;

public final class WaySerializeData {
    private final short h33;
    private final long id;
    private final boolean closed;
    private final boolean nonValid;
    private final int h38;
    private final long[] pointsIdx;
    private final Set<Integer> wayIntersectionH38Indexes;
    private final Point centre;
    private final float scaleDim;
    private final Polygon bboxGeometry;
    private final LineString lineString;
    private final Collection<Tag> tags;

    private WaySerializeData(Builder b) {
        this.h33 = b.h33;
        this.id = b.id;
        this.closed = b.closed;
        this.nonValid = b.nonValid;
        this.h38 = b.h38;
        this.pointsIdx = b.pointsIdx;
        this.wayIntersectionH38Indexes = b.wayIntersectionH38Indexes;
        this.centre = b.centre;
        this.scaleDim = b.scaleDim;
        this.bboxGeometry = b.bboxGeometry;
        this.lineString = b.lineString;
        this.tags = b.tags;
    }

    public static final class Builder {
        private short h33;
        private long id;
        private boolean closed;
        private boolean nonValid;
        private int h38;
        private long[] pointsIdx;
        private Set<Integer> wayIntersectionH38Indexes;
        private Point centre;
        private float scaleDim;
        private Polygon bboxGeometry;
        private LineString lineString;
        private Collection<Tag> tags;

        public Builder h33(short v)                              { h33 = v; return this; }
        public Builder id(long v)                                { id = v; return this; }
        public Builder closed(boolean v)                         { closed = v; return this; }
        public Builder nonValid(boolean v)                       { nonValid = v; return this; }
        public Builder h38(int v)                                { h38 = v; return this; }
        public Builder pointsIdx(long[] v)                       { pointsIdx = v; return this; }
        public Builder wayIntersectionH38Indexes(Set<Integer> v) { wayIntersectionH38Indexes = v; return this; }
        public Builder centre(Point v)                           { centre = v; return this; }
        public Builder scaleDim(float v)                         { scaleDim = v; return this; }
        public Builder bboxGeometry(Polygon v)                   { bboxGeometry = v; return this; }
        public Builder lineString(LineString v)                  { lineString = v; return this; }
        public Builder tags(Collection<Tag> v)                   { tags = v; return this; }

        public WaySerializeData build()                          { return new WaySerializeData(this); }
    }

    public short getH33()                               { return h33; }
    public long getId()                                 { return id; }
    public boolean isClosed()                           { return closed; }
    public boolean isNonValid()                         { return nonValid; }
    public int getH38()                                 { return h38; }
    public long[] getPointsIdx()                        { return pointsIdx; }
    public Set<Integer> getWayIntersectionH38Indexes()  { return wayIntersectionH38Indexes; }
    public Point getCentre()                            { return centre; }
    public float getScaleDim()                          { return scaleDim; }
    public Polygon getBboxGeometry()                    { return bboxGeometry; }
    public LineString getLineString()                   { return lineString; }
    public Collection<Tag> getTags()                    { return tags; }
}
