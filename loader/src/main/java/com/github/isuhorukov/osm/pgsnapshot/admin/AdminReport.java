package com.github.isuhorukov.osm.pgsnapshot.admin;

import java.util.concurrent.atomic.LongAdder;

final class AdminReport {

    enum Kind {
        NODES, WAYS, MULTIPOLYGONS
    }

    private final LongAdder[] bound = adders();
    private final LongAdder[] multiple = adders();
    private final LongAdder[] outside = adders();
    private final LongAdder[] fallbacks = adders();

    private static LongAdder[] adders() {
        LongAdder[] adders = new LongAdder[Kind.values().length];
        for (int i = 0; i < adders.length; i++) {
            adders[i] = new LongAdder();
        }
        return adders;
    }

    void count(Kind kind, int units) {
        if (units == 0) {
            outside[kind.ordinal()].increment();
            return;
        }
        bound[kind.ordinal()].increment();
        if (units > 1) {
            multiple[kind.ordinal()].increment();
        }
    }

    void fallback(Kind kind) {
        fallbacks[kind.ordinal()].increment();
    }

    long boundObjects() {
        long total = 0;
        for (LongAdder adder : bound) {
            total += adder.sum();
        }
        return total;
    }

    void appendJson(StringBuilder json) {
        json.append('{');
        for (Kind kind : Kind.values()) {
            int k = kind.ordinal();
            json.append(k == 0 ? "" : ",").append('"').append(kind.name().toLowerCase()).append("\":{\"bound\":")
                    .append(bound[k].sum()).append(",\"multiple\":").append(multiple[k].sum())
                    .append(",\"outside\":").append(outside[k].sum()).append(",\"fallbacks\":")
                    .append(fallbacks[k].sum()).append('}');
        }
        json.append('}');
    }
}
