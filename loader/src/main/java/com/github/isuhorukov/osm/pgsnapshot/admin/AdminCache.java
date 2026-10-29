package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

final class AdminCache {

    static final String FILE_NAME = "admin_index.bin";
    private static final Logger log = LoggerFactory.getLogger(AdminCache.class);
    private static final String MAGIC = "openstreetmap_h3:admin_index";
    private static final int BUFFER_SIZE = 1 << 20;

    private AdminCache() {
    }

    static CompactIndex load(Path directory, String fingerprint, List<AdminUnit> units, AdminOptions options)
            throws IOException {
        Path file = directory.resolve(FILE_NAME);
        if (!Files.isRegularFile(file)) {
            log.info("admin unit cache {} not found, the directory is built", file);
            return null;
        }
        long start = System.currentTimeMillis();
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(file),
                BUFFER_SIZE))) {
            if (!MAGIC.equals(input.readUTF()) || input.readInt() != AdminOptions.FORMAT_VERSION
                    || !fingerprint.equals(input.readUTF()) || input.readInt() != units.size()) {
                log.info("admin unit cache {} has another fingerprint, the directory is rebuilt", file);
                return null;
            }
            int[] parents = new int[units.size()];
            char[] sources = new char[units.size()];
            double[] parentShares = new double[units.size()];
            double[] ownAreas = new double[units.size()];
            for (int i = 0; i < units.size(); i++) {
                if (input.readLong() != units.get(i).getId()) {
                    log.info("admin unit cache {} has other units, the directory is rebuilt", file);
                    return null;
                }
                parents[i] = input.readInt();
                sources[i] = input.readChar();
                parentShares[i] = input.readDouble();
                ownAreas[i] = input.readDouble();
            }
            long[] xy = readLongs(input);
            int[] ringStart = readInts(input);
            int[] pieceRing = readInts(input);
            int[] pieceUnit = readInts(input);
            int[] pieceBox = readInts(input);
            long[] cut = readLongs(input);
            for (AdminUnit unit : units) {
                int i = unit.getIndex();
                unit.setParent(parents[i], sources[i], parentShares[i]);
                unit.setOwnAreaM2(ownAreas[i]);
            }
            CompactIndex index = new CompactIndex(units, options,
                    new IndexArrays(xy, ringStart, pieceRing, pieceUnit, pieceBox, cut));
            log.info("admin unit directory loaded from cache {} in {} ms", file, System.currentTimeMillis() - start);
            return index;
        }
    }

    static void save(Path directory, String fingerprint, List<AdminUnit> units, CompactIndex index)
            throws IOException {
        long start = System.currentTimeMillis();
        Files.createDirectories(directory);
        Path file = directory.resolve(FILE_NAME);
        Path temporary = Files.createTempFile(directory, FILE_NAME, ".tmp");
        try {
            try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(temporary), BUFFER_SIZE))) {
                output.writeUTF(MAGIC);
                output.writeInt(AdminOptions.FORMAT_VERSION);
                output.writeUTF(fingerprint);
                output.writeInt(units.size());
                for (AdminUnit unit : units) {
                    output.writeLong(unit.getId());
                    output.writeInt(unit.getParent());
                    output.writeChar(unit.getParentSource());
                    output.writeDouble(unit.getParentShare());
                    output.writeDouble(unit.getOwnAreaM2());
                }
                IndexArrays arrays = index.getArrays();
                writeLongs(output, arrays.xy);
                writeInts(output, arrays.ringStart);
                writeInts(output, arrays.pieceRing);
                writeInts(output, arrays.pieceUnit);
                writeInts(output, arrays.pieceBox);
                writeLongs(output, arrays.cut);
            }
            Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(temporary);
        }
        log.info("admin unit directory saved to cache {} ({} bytes) in {} ms", file, Files.size(file),
                System.currentTimeMillis() - start);
    }

    private static void writeLongs(DataOutputStream output, long[] values) throws IOException {
        output.writeInt(values.length);
        for (long value : values) {
            output.writeLong(value);
        }
    }

    private static void writeInts(DataOutputStream output, int[] values) throws IOException {
        output.writeInt(values.length);
        for (int value : values) {
            output.writeInt(value);
        }
    }

    private static long[] readLongs(DataInputStream input) throws IOException {
        long[] values = new long[input.readInt()];
        for (int i = 0; i < values.length; i++) {
            values[i] = input.readLong();
        }
        return values;
    }

    private static int[] readInts(DataInputStream input) throws IOException {
        int[] values = new int[input.readInt()];
        for (int i = 0; i < values.length; i++) {
            values[i] = input.readInt();
        }
        return values;
    }
}
