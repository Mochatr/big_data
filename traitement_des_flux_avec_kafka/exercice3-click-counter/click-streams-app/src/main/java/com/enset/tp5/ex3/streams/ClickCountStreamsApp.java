package com.enset.tp5.ex3.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * Exercice 3 - Partie 2 : compte les clics recus sur le topic "clicks" et
 * publie le resultat dans "click-counts".
 *
 * Variante retenue ici : comptage global (cle constante "total"), qui
 * correspond a l'exemple de resultat attendu du guide (totalClicks qui
 * augmente progressivement). Le comptage par utilisateur (variante 2) est
 * une simple variation : grouper par cle (userId) au lieu de selectKey vers
 * une cle constante.
 */
public class ClickCountStreamsApp {

    static final String GLOBAL_KEY = "total";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "click-count-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> clicks = builder.stream("clicks", Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> totalCount = clicks
                .selectKey((userId, value) -> GLOBAL_KEY)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count();

        totalCount.toStream()
                .mapValues(String::valueOf)
                .to("click-counts", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Arret propre du comptage de clics...");
            streams.close();
        }));
        streams.start();
    }
}
