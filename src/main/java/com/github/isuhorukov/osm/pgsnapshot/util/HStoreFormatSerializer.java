package com.github.isuhorukov.osm.pgsnapshot.util;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collection;
import java.util.Iterator;

public class HStoreFormatSerializer {
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
            //if ("created_by".equals(key)) continue; //filter "created_by"=>"JOSM" deprecated data
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
        for (int i=0; i<s.length(); i++) {
            char currentChar = s.charAt(i);
            switch (currentChar) {
                case '\\': // Slash
                    result.append("\\\\\\\\");
                    break;
                case 8: // Backspace
                    result.append("\\b");
                    break;
                case 12: // Form feed
                    result.append("\\f");
                    break;
                case 10: // Newline
                    result.append("\\n");
                    break;
                case 13: // Carriage return
                    result.append("\\r");
                    break;
                case 9: // Tab
                    result.append("\\t");
                    break;
                case 11: // Vertical tab
                    result.append("\\v");
                    break;
                case '"': // Quote
                    result.append("\\\\\\\"");
                    break;
                default:
                    result.append(currentChar);
            }
        }
        result.append("\\\"");
    }

    public static void escapeString(StringBuilder result, String data) {
        if (data == null || data.isEmpty()) {
            result.append(HStoreFormatSerializer.NULL_STRING);
            return;
        }
        char[] dataArray = data.toCharArray();
        result.append("\"");
        for (char currentChar : dataArray) {
            switch (currentChar) {
                case '\\': // Slash
                    result.append("\\\\");
                    break;
                case 8: // Backspace
                    result.append("\\b");
                    break;
                case 12: // Form feed
                    result.append("\\f");
                    break;
                case 10: // Newline
                    result.append("\\n");
                    break;
                case 13: // Carriage return
                    result.append("\\r");
                    break;
                case 9: // Tab
                    result.append("\\t");
                    break;
                case 11: // Vertical tab
                    result.append("\\v");
                    break;
                case '\"': // Quote
                    result.append("\\\"");
                    break;
                default:
                    result.append(currentChar);
            }
        }
        result.append("\"");
    }
}
