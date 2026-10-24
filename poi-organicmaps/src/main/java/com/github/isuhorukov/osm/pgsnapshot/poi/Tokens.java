package com.github.isuhorukov.osm.pgsnapshot.poi;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

final class Tokens {

    private Tokens() {
    }

    static List<String> split(String text, String delimiters) {
        StringTokenizer tokenizer = new StringTokenizer(text, delimiters);
        List<String> tokens = new ArrayList<>(tokenizer.countTokens());
        while (tokenizer.hasMoreTokens()) {
            tokens.add(tokenizer.nextToken());
        }
        return tokens;
    }

    static String strip(String text, String characters) {
        int begin = 0;
        int end = text.length();
        while (begin < end && characters.indexOf(text.charAt(begin)) >= 0) {
            begin++;
        }
        while (end > begin && characters.indexOf(text.charAt(end - 1)) >= 0) {
            end--;
        }
        return text.substring(begin, end);
    }
}
