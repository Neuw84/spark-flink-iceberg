package com.benchmark;

import java.util.HashMap;
import java.util.Map;

/** Minimal --key value arg parser to keep the job dependency-light. */
final class ParamUtil {
    private final Map<String, String> values;

    private ParamUtil(Map<String, String> values) {
        this.values = values;
    }

    static ParamUtil fromArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            String k = args[i];
            if (k.startsWith("--")) {
                m.put(k.substring(2), args[i + 1]);
            }
        }
        return new ParamUtil(m);
    }

    String get(String key, String dflt) {
        return values.getOrDefault(key, dflt);
    }
}
