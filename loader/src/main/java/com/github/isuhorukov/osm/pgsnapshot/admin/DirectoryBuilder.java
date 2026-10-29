package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.operation.overlayng.OverlayNG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

final class DirectoryBuilder {

    private static final Logger log = LoggerFactory.getLogger(DirectoryBuilder.class);
    private static final double PROTRUDING_SHARE = 0.999;

    private final List<AdminUnit> units;
    private final AdminOptions options;
    private final WorkerPool pool;
    private final Geometry[] geometries;
    private final List<List<Piece>> unitPieces;
    private final List<Map<Integer, Double>> shares;
    private final List<Piece> pieces = new ArrayList<>();
    private final STRtree pieceTree = new STRtree();

    private DirectoryBuilder(List<AdminUnit> units, AdminOptions options, WorkerPool pool) {
        this.units = units;
        this.options = options;
        this.pool = pool;
        this.geometries = new Geometry[units.size()];
        this.unitPieces = new ArrayList<>(units.size());
        this.shares = new ArrayList<>(units.size());
        for (int i = 0; i < units.size(); i++) {
            unitPieces.add(List.of());
            shares.add(Map.of());
        }
    }

    static OwnPieces build(List<AdminUnit> units, AdminOptions options, WorkerPool pool)
            throws InterruptedException {
        return new DirectoryBuilder(units, options, pool).build();
    }

    private OwnPieces build() throws InterruptedException {
        long start = System.currentTimeMillis();
        parseGeometries(units, geometries, pool);
        long parsed = System.currentTimeMillis();
        pool.parallel(units.size(), index -> {
            if (geometries[index] != null) {
                List<Piece> parts = new ArrayList<>();
                for (Polygon polygon : GridGeometry.clipSubdivide(geometries[index])) {
                    parts.add(new Piece(index, units.get(index).getLevel(), polygon));
                }
                unitPieces.set(index, parts);
            }
        });
        for (List<Piece> parts : unitPieces) {
            for (Piece piece : parts) {
                pieces.add(piece);
                pieceTree.insert(piece.envelope, piece);
            }
        }
        pieceTree.build();
        long subdivided = System.currentTimeMillis();
        pool.parallel(units.size(), index -> {
            if (geometries[index] != null) {
                shares.set(index, computeShares(units.get(index)));
            }
        });
        long shared = System.currentTimeMillis();
        buildTree();
        assignPaths(units);
        List<List<Polygon>> ownBySource = new ArrayList<>(pieces.size());
        for (int i = 0; i < pieces.size(); i++) {
            ownBySource.add(List.of());
        }
        pool.parallel(pieces.size(), index -> ownBySource.set(index, ownTerritory(pieces.get(index))));
        List<Polygon> ownPolygons = new ArrayList<>();
        List<Integer> ownUnits = new ArrayList<>();
        double[] ownArea = new double[units.size()];
        for (int i = 0; i < pieces.size(); i++) {
            int unit = pieces.get(i).unit;
            for (Polygon polygon : ownBySource.get(i)) {
                ownPolygons.add(polygon);
                ownUnits.add(unit);
                ownArea[unit] += GridGeometry.areaM2(polygon);
            }
        }
        for (AdminUnit unit : units) {
            unit.setOwnAreaM2(ownArea[unit.getIndex()]);
        }
        long end = System.currentTimeMillis();
        log.info("admin units {}, with polygon {}, pieces {}, own territory pieces {}; polygons {} ms, "
                        + "subdivision {} ms, shares {} ms, tree and own territory {} ms, total {} ms",
                units.size(), units.stream().filter(AdminUnit::hasGeometry).count(), pieces.size(),
                ownPolygons.size(), parsed - start, subdivided - parsed, shared - subdivided, end - shared,
                end - start);
        logTree();
        return new OwnPieces(ownPolygons, ownUnits.stream().mapToInt(Integer::intValue).toArray());
    }

    static void parseGeometries(List<AdminUnit> units, Geometry[] geometries, WorkerPool pool)
            throws InterruptedException {
        pool.parallel(units.size(), index -> {
            AdminUnit unit = units.get(index);
            if (unit.hasGeometry()) {
                Geometry geometry = geometry(unit);
                unit.setArea(GridGeometry.areaM2(geometry), geometry.getArea());
                if (geometries != null) {
                    geometries[index] = geometry;
                }
            }
        });
    }

    static Geometry geometry(AdminUnit unit) {
        try {
            return new WKBReader(GridGeometry.FACTORY).read(unit.getEwkb());
        } catch (ParseException e) {
            throw new IllegalStateException("Invalid polygon of admin unit relation " + unit.getId(), e);
        }
    }

    private Map<Integer, Double> computeShares(AdminUnit unit) {
        Map<Integer, Double> areas = new HashMap<>();
        for (Piece child : unitPieces.get(unit.getIndex())) {
            for (Object candidate : pieceTree.query(child.envelope)) {
                Piece ancestor = (Piece) candidate;
                double area = ancestor.level < unit.getLevel() ? intersectionArea(ancestor, child) : 0;
                if (area > 0) {
                    areas.merge(ancestor.unit, area, Double::sum);
                }
            }
        }
        Map<Integer, Double> result = new HashMap<>();
        for (Map.Entry<Integer, Double> area : areas.entrySet()) {
            result.put(area.getKey(), area.getValue() / unit.getAreaDegrees());
        }
        return result;
    }

    private static double intersectionArea(Piece ancestor, Piece child) {
        if (!ancestor.envelope.intersects(child.envelope)) {
            return 0;
        }
        if (ancestor.envelope.covers(child.envelope) && ancestor.prepared.covers(child.polygon)) {
            return child.area;
        }
        if (ancestor.prepared.intersects(child.polygon)) {
            return GridGeometry.overlay(ancestor.polygon, child.polygon, OverlayNG.INTERSECTION).getArea();
        }
        return 0;
    }

    private int ruleFive(AdminUnit unit, char[] source) {
        int best = -1;
        double bestShare = 0;
        for (Map.Entry<Integer, Double> share : new TreeMap<>(shares.get(unit.getIndex())).entrySet()) {
            AdminUnit candidate = units.get(share.getKey());
            if (share.getValue() < options.getParentShare()) {
                continue;
            }
            if (best < 0 || candidate.getLevel() > units.get(best).getLevel()
                    || (candidate.getLevel() == units.get(best).getLevel() && share.getValue() > bestShare)) {
                best = candidate.getIndex();
                bestShare = share.getValue();
            }
        }
        source[0] = best >= 0 ? AdminUnit.PARENT_GEOMETRY : AdminUnit.NO_PARENT;
        int subarea = unit.getSubareaParent();
        if (subarea >= 0 && units.get(subarea).getLevel() < unit.getLevel()) {
            double share = shares.get(unit.getIndex()).getOrDefault(subarea, 0.0);
            boolean acceptable = !units.get(subarea).hasGeometry() || !unit.hasGeometry()
                    || share >= options.getSubareaShare();
            if (acceptable && (best < 0 || units.get(subarea).getLevel() >= units.get(best).getLevel())) {
                best = subarea;
                source[0] = AdminUnit.PARENT_SUBAREA;
            }
        }
        return best;
    }

    private void buildTree() {
        char[] firstSource = new char[units.size()];
        int[] firstParent = firstParents(firstSource);
        int[] country = countries(firstParent);
        Map<Long, Map<Integer, Integer>> linkLevels = confirmedLinkLevels(country);
        for (AdminUnit unit : units) {
            int preferredLevel = majorityLevel(linkLevels.get(countryLevel(country[unit.getIndex()], unit.getLevel())));
            int chosen = preferredParent(unit, preferredLevel);
            int parent = chosen >= 0 ? chosen : firstParent[unit.getIndex()];
            char parentSource = firstSource[unit.getIndex()];
            if (chosen >= 0) {
                parentSource = chosen == unit.getSubareaParent() ? AdminUnit.PARENT_SUBAREA : AdminUnit.PARENT_GEOMETRY;
            }
            unit.setParent(parent, parentSource,
                    parent >= 0 ? shares.get(unit.getIndex()).getOrDefault(parent, 0.0) : 0);
        }
    }

    private int[] firstParents(char[] firstSource) {
        int[] firstParent = new int[units.size()];
        char[] source = new char[1];
        for (AdminUnit unit : units) {
            firstParent[unit.getIndex()] = ruleFive(unit, source);
            firstSource[unit.getIndex()] = source[0];
        }
        return firstParent;
    }

    private static int[] countries(int[] firstParent) {
        int[] country = new int[firstParent.length];
        for (int index = 0; index < firstParent.length; index++) {
            int root = index;
            while (firstParent[root] >= 0) {
                root = firstParent[root];
            }
            country[index] = root;
        }
        return country;
    }

    private Map<Long, Map<Integer, Integer>> confirmedLinkLevels(int[] country) {
        Map<Long, Map<Integer, Integer>> linkLevels = new HashMap<>();
        for (AdminUnit unit : units) {
            int subarea = unit.getSubareaParent();
            if (subarea >= 0 && isConfirmedLink(unit, units.get(subarea))) {
                linkLevels.computeIfAbsent(countryLevel(country[unit.getIndex()], unit.getLevel()), key -> new TreeMap<>())
                        .merge(units.get(subarea).getLevel(), 1, Integer::sum);
            }
        }
        return linkLevels;
    }

    private boolean isConfirmedLink(AdminUnit unit, AdminUnit parent) {
        return parent.getLevel() < unit.getLevel() && parent.hasGeometry() && unit.hasGeometry()
                && shares.get(unit.getIndex()).getOrDefault(parent.getIndex(), 0.0) >= options.getSubareaShare();
    }

    private int preferredParent(AdminUnit unit, int preferredLevel) {
        if (preferredLevel < 0 || !unit.hasGeometry()) {
            return -1;
        }
        int chosen = -1;
        double chosenShare = 0;
        for (Map.Entry<Integer, Double> share : new TreeMap<>(shares.get(unit.getIndex())).entrySet()) {
            if (units.get(share.getKey()).getLevel() == preferredLevel && share.getValue() >= options.getParentShare()
                    && share.getValue() > chosenShare) {
                chosen = share.getKey();
                chosenShare = share.getValue();
            }
        }
        return chosen;
    }

    private static long countryLevel(int country, int level) {
        return ((long) country << 8) | level;
    }

    private static int majorityLevel(Map<Integer, Integer> levels) {
        if (levels == null) {
            return -1;
        }
        int best = -1;
        int bestCount = 0;
        for (Map.Entry<Integer, Integer> level : levels.entrySet()) {
            if (level.getValue() >= bestCount) {
                best = level.getKey();
                bestCount = level.getValue();
            }
        }
        return best;
    }

    static void assignPaths(List<AdminUnit> units) {
        for (AdminUnit unit : units) {
            path(units, unit);
            if (unit.getParent() >= 0) {
                units.get(unit.getParent()).addChild();
            }
        }
    }

    private static int[] path(List<AdminUnit> units, AdminUnit unit) {
        if (unit.getPath() == null) {
            int[] parentPath = unit.getParent() < 0 ? new int[0] : path(units, units.get(unit.getParent()));
            int[] path = Arrays.copyOf(parentPath, parentPath.length + 1);
            path[parentPath.length] = unit.getIndex();
            unit.setPath(path);
        }
        return unit.getPath();
    }

    private boolean isDescendant(int candidate, int ancestor) {
        if (candidate == ancestor) {
            return false;
        }
        for (int index : units.get(candidate).getPath()) {
            if (index == ancestor) {
                return true;
            }
        }
        return false;
    }

    private List<Polygon> ownTerritory(Piece piece) {
        List<Geometry> descendants = new ArrayList<>();
        for (Object candidate : pieceTree.query(piece.envelope)) {
            Piece other = (Piece) candidate;
            if (other.unit == piece.unit || other.level <= piece.level || !other.envelope.intersects(piece.envelope)) {
                continue;
            }
            AdminUnit otherUnit = units.get(other.unit);
            boolean child = otherUnit.getParent() == piece.unit;
            boolean protruding = otherUnit.getParentShare() < PROTRUDING_SHARE && isDescendant(other.unit, piece.unit);
            if ((child || protruding) && piece.prepared.intersects(other.polygon)) {
                descendants.add(other.polygon);
            }
        }
        if (descendants.isEmpty()) {
            return List.of(piece.polygon);
        }
        return GridGeometry.clipSubdivide(GridGeometry.overlay(piece.polygon, GridGeometry.union(descendants),
                OverlayNG.DIFFERENCE));
    }

    private void logTree() {
        Map<String, Integer> sources = new TreeMap<>();
        int roots = 0;
        for (AdminUnit unit : units) {
            if (unit.getParent() < 0) {
                roots++;
            } else {
                sources.merge(unit.getLevel() + " <- " + units.get(unit.getParent()).getLevel() + " "
                        + unit.getParentSource(), 1, Integer::sum);
            }
        }
        log.info("admin unit tree (child level <- parent level, G geometry, S subarea): {}, roots {}", sources, roots);
    }

    static final class OwnPieces {
        private final List<Polygon> polygons;
        private final int[] units;

        OwnPieces(List<Polygon> polygons, int[] units) {
            this.polygons = polygons;
            this.units = units;
        }

        List<Polygon> getPolygons() {
            return polygons;
        }

        int[] getUnits() {
            return units;
        }
    }

    private static final class Piece {
        private final int unit;
        private final int level;
        private final Polygon polygon;
        private final Envelope envelope;
        private final double area;
        private final PreparedGeometry prepared;

        private Piece(int unit, int level, Polygon polygon) {
            this.unit = unit;
            this.level = level;
            this.polygon = polygon;
            this.envelope = polygon.getEnvelopeInternal();
            this.area = polygon.getArea();
            this.prepared = PreparedGeometryFactory.prepare(polygon);
        }
    }
}
