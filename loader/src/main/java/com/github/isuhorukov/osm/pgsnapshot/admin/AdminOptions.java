package com.github.isuhorukov.osm.pgsnapshot.admin;

import java.math.BigDecimal;
import java.util.List;

public final class AdminOptions {

    public static final int FORMAT_VERSION = 1;
    public static final int RULES_VERSION = 1;
    public static final int KEY_VERSION = 1;
    public static final List<String> DEFAULT_NAME_LANG = List.of("name", "en");
    public static final double DEFAULT_PARENT_SHARE = 0.9;
    public static final double DEFAULT_SUBAREA_SHARE = 0.5;
    public static final double DEFAULT_MIN_SHARE = 0.01;
    public static final double DEFAULT_MIN_AREA_M2 = 10_000;
    public static final double DEFAULT_MIN_LENGTH_M = 100;

    private final List<String> nameLang;
    private final double parentShare;
    private final double subareaShare;
    private final double minShare;
    private final double minAreaM2;
    private final double minLengthM;
    private final AdminColumns columns;

    public AdminOptions(List<String> nameLang, double parentShare, double subareaShare, double minShare,
                        double minAreaM2, double minLengthM, AdminColumns columns) {
        this.nameLang = List.copyOf(nameLang);
        this.parentShare = parentShare;
        this.subareaShare = subareaShare;
        this.minShare = minShare;
        this.minAreaM2 = minAreaM2;
        this.minLengthM = minLengthM;
        this.columns = columns;
    }

    public static AdminOptions defaults(AdminColumns columns) {
        return new AdminOptions(DEFAULT_NAME_LANG, DEFAULT_PARENT_SHARE, DEFAULT_SUBAREA_SHARE, DEFAULT_MIN_SHARE,
                DEFAULT_MIN_AREA_M2, DEFAULT_MIN_LENGTH_M, columns);
    }

    public List<String> getNameLang() {
        return nameLang;
    }

    public double getParentShare() {
        return parentShare;
    }

    public double getSubareaShare() {
        return subareaShare;
    }

    public double getMinShare() {
        return minShare;
    }

    public double getMinAreaM2() {
        return minAreaM2;
    }

    public double getMinLengthM() {
        return minLengthM;
    }

    public AdminColumns getColumns() {
        return columns;
    }

    String fingerprintParameters() {
        return "format=" + FORMAT_VERSION + ";rules=" + RULES_VERSION + ";name_lang=" + String.join(",", nameLang)
                + ";parent_share=" + number(parentShare) + ";subarea_share=" + number(subareaShare);
    }

    void appendDirectoryJson(StringBuilder json) {
        json.append("{\"name_lang\":[");
        for (int i = 0; i < nameLang.size(); i++) {
            json.append(i == 0 ? "" : ",");
            Json.string(json, nameLang.get(i));
        }
        json.append("],\"parent_share\":").append(number(parentShare))
                .append(",\"subarea_share\":").append(number(subareaShare)).append('}');
    }

    void appendBindingJson(StringBuilder json) {
        json.append("{\"min_share\":").append(number(minShare))
                .append(",\"min_area_m2\":").append(number(minAreaM2))
                .append(",\"min_length_m\":").append(number(minLengthM)).append('}');
    }

    static String number(double value) {
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }
}
