package com.github.isuhorukov.osm.pgsnapshot.admin;

import java.util.Map;

final class Json {

    private Json() {
    }

    static void string(StringBuilder json, String value) {
        if (value == null) {
            json.append("null");
            return;
        }
        json.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') {
                json.append('\\').append(c);
            } else if (c < 0x20) {
                json.append(String.format("\\u%04x", (int) c));
            } else {
                json.append(c);
            }
        }
        json.append('"');
    }

    static void object(StringBuilder json, Map<String, String> values) {
        json.append('{');
        boolean first = true;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            json.append(first ? "" : ",");
            string(json, entry.getKey());
            json.append(':');
            string(json, entry.getValue());
            first = false;
        }
        json.append('}');
    }

    static void longs(StringBuilder json, long[] values) {
        json.append('[');
        for (int i = 0; i < values.length; i++) {
            json.append(i == 0 ? "" : ",").append(values[i]);
        }
        json.append(']');
    }

    static void shorts(StringBuilder json, short[] values) {
        json.append('[');
        for (int i = 0; i < values.length; i++) {
            json.append(i == 0 ? "" : ",").append(values[i]);
        }
        json.append(']');
    }
}
