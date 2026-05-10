package com.dataengineering.lab4;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@EnableKafkaStreams
public class TripAnalyticsTopology {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Bean
    public KStream<String, String> tripAnalytics(
            StreamsBuilder builder,
            @Value("${lab4.kafka.input-topic}") String inputTopic,
            @Value("${lab4.kafka.output.avg-duration-by-day}") String avgTopic,
            @Value("${lab4.kafka.output.trip-count-by-day}") String countTopic,
            @Value("${lab4.kafka.output.top-start-station-by-day}") String topStartTopic,
            @Value("${lab4.kafka.output.top3-stations-by-day}") String top3Topic) {

        KStream<String, String> source = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        JsonSerde<TripParse> tripSerde = new JsonSerde<>(TripParse.class);

        KStream<String, TripParse> trips = source
                .mapValues(TripJsonParser::parse)
                .filter((k, v) -> v != null && v.validForDurationAndCount());

        KStream<String, TripParse> byDay = trips.selectKey((k, v) -> v.day());

        // (a) Average trip duration per calendar day (seconds)
        byDay.groupByKey(Grouped.with(Serdes.String(), tripSerde))
                .aggregate(
                        () -> "0.0|0",
                        (day, trip, agg) -> mergeSumCount(agg, trip.durationSec()),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("lab4-duration-sum")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()))
                .toStream()
                .map((day, agg) -> {
                    try {
                        String[] p = agg.split("\\|");
                        double sum = Double.parseDouble(p[0]);
                        long cnt = Long.parseLong(p[1]);
                        double avg = cnt == 0 ? 0.0 : sum / cnt;
                        String json = MAPPER.writeValueAsString(Map.of(
                                "day", day,
                                "avgTripDurationSec", avg));
                        return KeyValue.pair(day, json);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .to(avgTopic, Produced.with(Serdes.String(), Serdes.String()));

        // (b) Trip count per day
        byDay.groupByKey(Grouped.with(Serdes.String(), tripSerde))
                .count(Materialized.as("lab4-trip-count-store"))
                .toStream()
                .map((day, cnt) -> {
                    try {
                        String json = MAPPER.writeValueAsString(Map.of(
                                "day", day,
                                "tripCount", cnt));
                        return KeyValue.pair(day, json);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .to(countTopic, Produced.with(Serdes.String(), Serdes.String()));

        // (c) Most popular start station per day
        KStream<String, TripParse> withStart = trips.filter((k, v) -> v.validForStartStation());
        KTable<String, Long> startCounts = withStart
                .map((k, v) -> KeyValue.pair(v.day() + "|" + v.fromStation(), 1L))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .count(Materialized.as("lab4-start-station-counts"));

        startCounts.toStream()
                .map((composite, cnt) -> {
                    int sep = composite.indexOf('|');
                    String day = composite.substring(0, sep);
                    String station = composite.substring(sep + 1);
                    return KeyValue.pair(day, station + "\t" + cnt);
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .reduce(
                        TripAnalyticsTopology::betterStationCount,
                        Materialized.as("lab4-top-start-reduce"))
                .toStream()
                .map((day, stationTabCnt) -> {
                    try {
                        String[] p = stationTabCnt.split("\t", 2);
                        String json = MAPPER.writeValueAsString(Map.of(
                                "day", day,
                                "topStartStation", p[0],
                                "tripCount", Long.parseLong(p[1])));
                        return KeyValue.pair(day, json);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .to(topStartTopic, Produced.with(Serdes.String(), Serdes.String()));

        // (d) Top 3 stations per day (start + end)
        KStream<String, Long> edgeEvents = trips
                .filter((k, v) -> v.validForEndpointStations())
                .flatMap((k, v) -> {
                    List<KeyValue<String, Long>> out = new ArrayList<>(2);
                    out.add(KeyValue.pair(v.day() + "|" + v.fromStation(), 1L));
                    out.add(KeyValue.pair(v.day() + "|" + v.toStation(), 1L));
                    return out;
                });

        KTable<String, Long> edgeCounts = edgeEvents
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .count(Materialized.as("lab4-edge-station-counts"));

        edgeCounts.toStream()
                .map((composite, cnt) -> {
                    int sep = composite.indexOf('|');
                    String day = composite.substring(0, sep);
                    String station = composite.substring(sep + 1);
                    return KeyValue.pair(day, station + "\t" + cnt);
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> "{}",
                        (day, stationTabCnt, json) -> mergeStationMapJson(json, stationTabCnt),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("lab4-top3-map")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()))
                .toStream()
                .map((day, jsonMap) -> {
                    try {
                        String json = formatTop3Json(day, jsonMap);
                        return KeyValue.pair(day, json);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .to(top3Topic, Produced.with(Serdes.String(), Serdes.String()));

        return source;
    }

    private static String mergeSumCount(String agg, double duration) {
        String[] p = agg.split("\\|");
        double sum = Double.parseDouble(p[0]) + duration;
        long cnt = Long.parseLong(p[1]) + 1;
        return sum + "|" + cnt;
    }

    private static String betterStationCount(String a, String b) {
        String[] pa = a.split("\t", 2);
        String[] pb = b.split("\t", 2);
        long ca = Long.parseLong(pa[1]);
        long cb = Long.parseLong(pb[1]);
        if (ca > cb) {
            return a;
        }
        if (cb > ca) {
            return b;
        }
        return pa[0].compareTo(pb[0]) <= 0 ? a : b;
    }

    private static String mergeStationMapJson(String json, String stationTabCount) {
        try {
            Map<String, Long> m = MAPPER.readValue(json, new TypeReference<Map<String, Long>>() {
            });
            String[] p = stationTabCount.split("\t", 2);
            m.put(p[0], Long.parseLong(p[1]));
            return MAPPER.writeValueAsString(m);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String formatTop3Json(String day, String jsonMap) throws Exception {
        Map<String, Long> m = MAPPER.readValue(jsonMap, new TypeReference<Map<String, Long>>() {
        });
        Comparator<Map.Entry<String, Long>> cmp = Comparator
                .<Map.Entry<String, Long>>comparingLong(e -> -e.getValue())
                .thenComparing(Map.Entry::getKey);
        List<Map<String, Object>> top = m.entrySet().stream()
                .sorted(cmp)
                .limit(3)
                .map(e -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("station", e.getKey());
                    row.put("count", e.getValue());
                    return row;
                })
                .collect(Collectors.toList());
        return MAPPER.writeValueAsString(Map.of(
                "day", day,
                "topStations", top));
    }
}
