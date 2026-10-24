package com.github.isuhorukov.osm.pgsnapshot.util;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collection;
import java.util.Iterator;

public class HStoreFormatSerializer {
    private HStoreFormatSerializer() {}

    public static final String NULL_STRING = "\\N";

    private static final String[] ESCAPED_CHARS = escapedChars(false);
    private static final String[] DEEP_ESCAPED_CHARS = escapedChars(true);

    public static void renderTags(Collection<Tag> entityTags, StringBuilder builder)
    {
        if(entityTags==null || entityTags.isEmpty()){
            builder.append(NULL_STRING);
            return;
        }
        builder.append('"');
        Iterator<Tag> iterator = entityTags.iterator();
        boolean first = true;
        while (iterator.hasNext()) {
            Tag tag = iterator.next();
            String key= tag.getKey();
            String value = tag.getValue();
            if (first) {
                first = false;
            } else {
                builder.append(',');
            }
            writeValue(builder, key);
            builder.append("=>");
            writeValue(builder, value);
        }
        builder.append('"');
    }

    private static void writeValue(StringBuilder result, String s) {
        if (s == null) {
            result.append("NULL");
            return;
        }
        result.append("\\\"");
        for (int i = 0; i < s.length(); i++) {
            appendEscapedChar(result, s.charAt(i), true);
        }
        result.append("\\\"");
    }

    public static void escapeString(StringBuilder result, String data) {
        if (data == null || data.isEmpty()) {
            result.append(HStoreFormatSerializer.NULL_STRING);
            return;
        }
        result.append("\"");
        for (char currentChar : data.toCharArray()) {
            appendEscapedChar(result, currentChar, false);
        }
        result.append("\"");
    }

    private static String[] escapedChars(boolean deep) {
        String[] escaped = new String['\\' + 1];
        escaped['\\'] = deep ? "\\\\\\\\" : "\\\\";
        escaped['"'] = deep ? "\\\\\\\"" : "\\\"";
        escaped[8] = "\\b";
        escaped[12] = "\\f";
        escaped[10] = "\\n";
        escaped[13] = "\\r";
        escaped[9] = "\\t";
        escaped[11] = "\\v";
        return escaped;
    }

    private static void appendEscapedChar(StringBuilder result, char c, boolean deep) {
        String[] escapedChars = deep ? DEEP_ESCAPED_CHARS : ESCAPED_CHARS;
        String escaped = c < escapedChars.length ? escapedChars[c] : null;
        if (escaped != null) {
            result.append(escaped);
        } else {
            result.append(c);
        }
    }
}
