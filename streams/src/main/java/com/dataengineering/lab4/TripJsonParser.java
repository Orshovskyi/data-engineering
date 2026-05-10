package com.dataengineering.lab4;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

final class TripJsonParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TripJsonParser() {
    }

    static TripParse parse(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            JsonNode n = MAPPER.readTree(json);
            String start = text(n, "start_time");
            if (start == null || start.length() < 10) {
                return null;
            }
            String day = start.substring(0, 10);
            double dur = parseDuration(text(n, "tripduration"));
            if (!Double.isFinite(dur)) {
                return null;
            }
            String from = textOrEmpty(n, "from_station_name");
            String to = textOrEmpty(n, "to_station_name");
            return new TripParse(day, dur, from, to);
        } catch (Exception e) {
            return null;
        }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? null : v.asText(null);
    }

    private static String textOrEmpty(JsonNode n, String field) {
        String t = text(n, field);
        return t == null ? "" : t;
    }

    private static double parseDuration(String raw) {
        if (raw == null || raw.isBlank()) {
            return Double.NaN;
        }
        String t = raw.replace(",", "").trim();
        try {
            return Double.parseDouble(t);
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }
}
