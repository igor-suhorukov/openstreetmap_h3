package com.github.isuhorukov.osm.pgsnapshot.util;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collection;
import java.util.Iterator;

public class HStoreFormatSerializer {
    private HStoreFormatSerializer() {}

    public static final String NULL_STRING = "\\N";

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

    private static void appendEscapedChar(StringBuilder result, char c, boolean deep) {
        switch (c) {
            case '\\':
                result.append(deep ? "\\\\\\\\" : "\\\\");
                break;
            case 8:
                result.append("\\b");
                break;
            case 12:
                result.append("\\f");
                break;
            case 10:
                result.append("\\n");
                break;
            case 13:
                result.append("\\r");
                break;
            case 9:
                result.append("\\t");
                break;
            case 11:
                result.append("\\v");
                break;
            case '"':
                result.append(deep ? "\\\\\\\"" : "\\\"");
                break;
            default:
                result.append(c);
        }
    }
}
