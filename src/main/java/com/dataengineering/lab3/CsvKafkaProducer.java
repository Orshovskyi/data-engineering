package com.dataengineering.lab3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Reads trip rows from a CSV file and publishes each row as a JSON message to Topic1 and Topic2.
 */
public final class CsvKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(CsvKafkaProducer.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093");
        String topic1 = env("KAFKA_TOPIC_1", "Topic1");
        String topic2 = env("KAFKA_TOPIC_2", "Topic2");
        Path csvPath = Path.of(env("CSV_FILE", "Divvy_Trips_2019_Q4.csv"));
        int maxRecords = Integer.parseInt(env("MAX_RECORDS", "500"));

        if (args.length >= 1) {
            csvPath = Path.of(args[0]);
        }

        if (!Files.isRegularFile(csvPath)) {
            System.err.println("CSV file not found: " + csvPath.toAbsolutePath());
            System.exit(1);
        }

        ObjectMapper json = new ObjectMapper();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "lab3-csv-producer");

        int sent = 0;
        try (Reader reader = Files.newBufferedReader(csvPath);
             CSVReader csv = new CSVReaderBuilder(reader).build();
             KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            String[] header = csv.readNext();
            if (header == null) {
                log.warn("CSV is empty");
                return;
            }
            validateHeader(header);

            String[] line;
            while ((line = csv.readNext()) != null) {
                if (maxRecords > 0 && sent >= maxRecords) {
                    break;
                }
                TripRecord record = toRecord(header, line);
                String key = record.trip_id() != null && !record.trip_id().isBlank()
                        ? record.trip_id()
                        : "row-" + (sent + 1);
                String value = json.writeValueAsString(record);

                producer.send(new ProducerRecord<>(topic1, key, value));
                producer.send(new ProducerRecord<>(topic2, key, value));
                sent++;

                if (sent % 100 == 0) {
                    log.info("Published {} records to {} and {}", sent, topic1, topic2);
                }
            }
            producer.flush();
        } catch (IOException e) {
            log.error("Failed to read CSV or produce", e);
            System.exit(1);
        }

        log.info("Done. Total records sent to each topic: {}", sent);
    }

    private static void validateHeader(String[] header) {
        if (header.length < 12) {
            throw new IllegalArgumentException(
                    "Expected at least 12 columns (Divvy trips schema), got " + header.length);
        }
    }

    private static TripRecord toRecord(String[] header, String[] line) {
        return new TripRecord(
                col(header, line, "trip_id"),
                col(header, line, "start_time"),
                col(header, line, "end_time"),
                col(header, line, "bikeid"),
                col(header, line, "tripduration"),
                col(header, line, "from_station_id"),
                col(header, line, "from_station_name"),
                col(header, line, "to_station_id"),
                col(header, line, "to_station_name"),
                col(header, line, "usertype"),
                col(header, line, "gender"),
                col(header, line, "birthyear")
        );
    }

    private static String col(String[] header, String[] line, String name) {
        for (int i = 0; i < header.length; i++) {
            if (name.equalsIgnoreCase(header[i].trim())) {
                return i < line.length ? nullToEmpty(line[i]) : "";
            }
        }
        return "";
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? defaultValue : v;
    }
}
