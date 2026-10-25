package com.github.isuhorukov.osm.pgsnapshot.poi;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

final class RulesFingerprint {

    private static final List<String> DATA_FILES = List.of(OrganicMapsPoiClassifier.MAPPING_FILE,
            OrganicMapsPoiClassifier.REPLACED_TAGS_FILE, OrganicMapsPoiClassifier.CATEGORIES_FILE,
            OrganicMapsPoiClassifier.TYPE_NAMES_FILE);
    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    private RulesFingerprint() {
    }

    static String of(Path dataDirectory) throws IOException {
        MessageDigest digest = sha256();
        for (String dataFile : DATA_FILES) {
            Path path = dataDirectory.resolve(dataFile);
            update(digest, dataFile, Files.exists(path) ? Files.readAllBytes(path) : null);
        }
        update(digest, OrganicMapsPoiClassifier.RULES_RESOURCE, resource(OrganicMapsPoiClassifier.RULES_RESOURCE));
        return hex(digest.digest());
    }

    private static void update(MessageDigest digest, String name, byte[] content) {
        digest.update(name.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
        if (content == null) {
            digest.update((byte) 0);
        } else {
            digest.update((byte) 1);
            digest.update(sha256().digest(content));
        }
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream stream = RulesFingerprint.class.getResourceAsStream(name)) {
            if (stream == null) {
                throw new IllegalArgumentException("Resource not found: " + name);
            }
            return stream.readAllBytes();
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static String hex(byte[] bytes) {
        char[] chars = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            chars[2 * i] = HEX_DIGITS[(bytes[i] >> 4) & 0xF];
            chars[2 * i + 1] = HEX_DIGITS[bytes[i] & 0xF];
        }
        return new String(chars);
    }
}
