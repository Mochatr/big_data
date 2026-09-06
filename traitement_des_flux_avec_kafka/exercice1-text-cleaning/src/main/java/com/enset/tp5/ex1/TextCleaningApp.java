package com.enset.tp5.ex1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * Exercice 1 : nettoyage et validation de messages texte.
 *
 * Lit les messages du topic "text-input", les nettoie (trim, espaces multiples,
 * majuscules), les valide, puis route vers "text-clean" ou "text-dead-letter".
 */
public class TextCleaningApp {

    private static final List<String> FORBIDDEN_WORDS = Arrays.asList("HACK", "SPAM", "XXX");
    private static final int MAX_LENGTH = 100;

    public static String clean(String raw) {
        if (raw == null) {
            return "";
        }
        return raw.trim().replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
    }

    public static boolean isValid(String cleaned) {
        if (cleaned == null || cleaned.isEmpty()) {
            return false;
        }
        if (cleaned.length() > MAX_LENGTH) {
            return false;
        }
        for (String word : FORBIDDEN_WORDS) {
            if (cleaned.contains(word)) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-cleaning-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream("text-input");

        KStream<String, String> cleaned = input.mapValues(TextCleaningApp::clean);

        cleaned.split()
                .branch((key, value) -> isValid(value), Branched.withConsumer(s -> s.to("text-clean")))
                .defaultBranch(Branched.withConsumer(s -> s.to("text-dead-letter")));

        Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Arret de l'application de nettoyage de texte...");
            streams.close();
        }));

        streams.start();
    }
}
