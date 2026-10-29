package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

public final class MultipolygonTags {

    public static final MultipolygonTags NONE = new MultipolygonTags(Collections.emptyList());
    static final byte[] UNCHANGED = new byte[0];

    private final List<KeyedExtension> extensions;

    private MultipolygonTags(List<KeyedExtension> extensions) {
        this.extensions = extensions;
    }

    public static MultipolygonTags of(List<TagExtension> tagExtensions) {
        List<KeyedExtension> extensions = new ArrayList<>();
        for (TagExtension tagExtension : tagExtensions) {
            Set<String> keys = Set.copyOf(tagExtension.multipolygonTagKeys());
            if (!keys.isEmpty()) {
                extensions.add(new KeyedExtension(tagExtension, keys));
            }
        }
        return extensions.isEmpty() ? NONE : new MultipolygonTags(List.copyOf(extensions));
    }

    public boolean isEmpty() {
        return extensions.isEmpty();
    }

    static int tagsStart(byte[] row, int rowStart, int rowEnd) {
        int tab = lastIndexOf(row, rowStart, rowEnd, (byte) '\t');
        if (tab < 0) {
            throw new IllegalStateException("No tags column in osmium export row: "
                    + new String(row, rowStart, Math.min(rowEnd - rowStart, 100), StandardCharsets.UTF_8));
        }
        return tab + 1;
    }

    byte[] enrichedTags(byte[] row, int rowStart, int rowEnd) {
        int tagsStart = tagsStart(row, rowStart, rowEnd);
        long id = id(row, rowStart, tagsStart - 1);
        boolean hasTags = hasTags(row, tagsStart, rowEnd);
        List<Tag> tags = hasTags ? new Hstore(id, copyUnescaped(row, tagsStart, rowEnd)).tags() : Collections.emptyList();
        Relation relation = new Relation(new CommonEntityData(id, 0, new Date(0), OsmUser.NONE, 0, tags),
                Collections.emptyList());
        StringBuilder addedPairs = new StringBuilder();
        for (KeyedExtension extension : extensions) {
            for (Tag tag : extension.tagExtension.additionalTags(relation)) {
                if (extension.keys.contains(tag.getKey())) {
                    appendPair(addedPairs, tag, hasTags || addedPairs.length() > 0);
                }
            }
        }
        if (addedPairs.length() == 0) {
            return UNCHANGED;
        }
        byte[] added = addedPairs.toString().getBytes(StandardCharsets.UTF_8);
        int keptLength = hasTags ? rowEnd - tagsStart : 0;
        byte[] enriched = Arrays.copyOfRange(row, tagsStart, tagsStart + keptLength + added.length);
        System.arraycopy(added, 0, enriched, keptLength, added.length);
        return enriched;
    }

    private static long id(byte[] row, int rowStart, int idEnd) {
        int idStart = lastIndexOf(row, rowStart, idEnd, (byte) '\t') + 1;
        try {
            return Long.parseLong(new String(row, idStart, idEnd - idStart, StandardCharsets.US_ASCII));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("No relation id in osmium export row", e);
        }
    }

    private static boolean hasTags(byte[] row, int tagsStart, int rowEnd) {
        int length = rowEnd - tagsStart;
        return length > 0 && !(length == 2 && row[tagsStart] == '\\' && row[tagsStart + 1] == 'N');
    }

    private static String copyUnescaped(byte[] row, int start, int end) {
        byte[] text = new byte[end - start];
        int length = 0;
        int i = start;
        while (i < end) {
            byte b = row[i++];
            if (b == '\\' && i < end) {
                b = unescaped(row[i++]);
            }
            text[length++] = b;
        }
        return new String(text, 0, length, StandardCharsets.UTF_8);
    }

    private static byte unescaped(byte escaped) {
        switch (escaped) {
            case 'b':
                return '\b';
            case 'f':
                return '\f';
            case 'n':
                return '\n';
            case 'r':
                return '\r';
            case 't':
                return '\t';
            case 'v':
                return 11;
            default:
                return escaped;
        }
    }

    private static void appendPair(StringBuilder pairs, Tag tag, boolean separator) {
        if (separator) {
            pairs.append(',');
        }
        appendQuoted(pairs, tag.getKey());
        pairs.append("=>");
        appendQuoted(pairs, tag.getValue());
    }

    private static void appendQuoted(StringBuilder pairs, String value) {
        pairs.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    pairs.append("\\\\\"");
                    break;
                case '\\':
                    pairs.append("\\\\\\\\");
                    break;
                case '\t':
                    pairs.append("\\t");
                    break;
                case '\n':
                    pairs.append("\\n");
                    break;
                case '\r':
                    pairs.append("\\r");
                    break;
                default:
                    pairs.append(c);
            }
        }
        pairs.append('"');
    }

    private static int lastIndexOf(byte[] row, int from, int to, byte value) {
        for (int i = to - 1; i >= from; i--) {
            if (row[i] == value) {
                return i;
            }
        }
        return -1;
    }

    private static final class KeyedExtension {
        private final TagExtension tagExtension;
        private final Set<String> keys;

        private KeyedExtension(TagExtension tagExtension, Set<String> keys) {
            this.tagExtension = tagExtension;
            this.keys = keys;
        }
    }

    private static final class Hstore {
        private static final String NULL = "NULL";

        private final long id;
        private final String text;
        private int position;

        private Hstore(long id, String text) {
            this.id = id;
            this.text = text;
        }

        private List<Tag> tags() {
            List<Tag> tags = new ArrayList<>();
            skipSpaces();
            while (position < text.length()) {
                String key = quoted();
                skipSpaces();
                expect('=');
                expect('>');
                skipSpaces();
                String value = nullValue() ? null : quoted();
                if (value != null) {
                    tags.add(new Tag(key, value));
                }
                skipSpaces();
                if (position < text.length()) {
                    expect(',');
                    skipSpaces();
                }
            }
            return tags;
        }

        private boolean nullValue() {
            if (text.startsWith(NULL, position)) {
                position += NULL.length();
                return true;
            }
            return false;
        }

        private String quoted() {
            expect('"');
            StringBuilder value = new StringBuilder();
            while (position < text.length()) {
                char c = text.charAt(position++);
                if (c == '"') {
                    return value.toString();
                }
                if (c == '\\' && position < text.length()) {
                    c = text.charAt(position++);
                }
                value.append(c);
            }
            throw malformed();
        }

        private void expect(char expected) {
            if (position >= text.length() || text.charAt(position) != expected) {
                throw malformed();
            }
            position++;
        }

        private void skipSpaces() {
            while (position < text.length() && text.charAt(position) == ' ') {
                position++;
            }
        }

        private IllegalStateException malformed() {
            return new IllegalStateException("Unexpected hstore tags of multipolygon relation " + id
                    + " at position " + position + ": " + text);
        }
    }
}
