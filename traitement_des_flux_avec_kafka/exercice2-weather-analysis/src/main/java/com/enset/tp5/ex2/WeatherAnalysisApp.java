package com.enset.tp5.ex2;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Locale;
import java.util.Properties;

/**
 * Exercice 2 : analyse de donnees meteorologiques.
 *
 * Lit "station,temperature,humidity" depuis weather-data, ne garde que les
 * releves > 30C, convertit en Fahrenheit, regroupe par station et publie les
 * moyennes (temperature et humidite) dans station-averages.
 */
public class WeatherAnalysisApp {

    /** Un releve valide, deja converti en Fahrenheit. */
    record Reading(String station, double temperatureF, double humidity) {
    }

    /** Accumulateur d'agregation par station (compte + sommes). */
    static class StationStats {
        long count;
        double sumTemperature;
        double sumHumidity;

        StationStats add(Reading r) {
            count++;
            sumTemperature += r.temperatureF();
            sumHumidity += r.humidity();
            return this;
        }

        double avgTemperature() {
            return count == 0 ? 0 : sumTemperature / count;
        }

        double avgHumidity() {
            return count == 0 ? 0 : sumHumidity / count;
        }

        String encode() {
            return count + ":" + sumTemperature + ":" + sumHumidity;
        }

        static StationStats decode(String s) {
            StationStats stats = new StationStats();
            if (s == null || s.isEmpty()) {
                return stats;
            }
            String[] parts = s.split(":");
            stats.count = Long.parseLong(parts[0]);
            stats.sumTemperature = Double.parseDouble(parts[1]);
            stats.sumHumidity = Double.parseDouble(parts[2]);
            return stats;
        }
    }

    /** Serde simple (encode/decode texte) pour eviter une dependance JSON. */
    static Serde<StationStats> stationStatsSerde() {
        return Serdes.serdeFrom(
                (topic, data) -> data == null ? null : data.encode().getBytes(),
                (topic, bytes) -> bytes == null ? new StationStats() : StationStats.decode(new String(bytes)));
    }

    /** Serde simple (encode/decode texte) pour la valeur Reading, utilise lors du regroupement. */
    static Serde<Reading> readingSerde() {
        return Serdes.serdeFrom(
                (topic, data) -> data == null ? null
                        : (data.station() + ":" + data.temperatureF() + ":" + data.humidity()).getBytes(),
                (topic, bytes) -> {
                    if (bytes == null) {
                        return null;
                    }
                    String[] parts = new String(bytes).split(":");
                    return new Reading(parts[0], Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
                });
    }

    static double celsiusToFahrenheit(double celsius) {
        return celsius * 9.0 / 5.0 + 32.0;
    }

    /**
     * Parse une ligne "station,temperature,humidity". Retourne null si la
     * ligne est mal formee (evite d'arreter le pipeline sur une erreur).
     */
    static Reading parse(String line) {
        try {
            String[] parts = line.split(",");
            String station = parts[0].trim();
            double temperature = Double.parseDouble(parts[1].trim());
            double humidity = Double.parseDouble(parts[2].trim());
            return new Reading(station, celsiusToFahrenheit(temperature), humidity);
        } catch (Exception e) {
            System.err.println("Ligne meteo ignoree (mal formee) : " + line + " -> " + e.getMessage());
            return null;
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-analysis-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> raw = builder.stream("weather-data", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, Reading> parsed = raw
                .mapValues(WeatherAnalysisApp::parse)
                .filter((key, reading) -> reading != null)
                // Ne conserver que les releves dont la temperature d'origine depasse 30C,
                // donc > 86F une fois converti.
                .filter((key, reading) -> reading.temperatureF() > celsiusToFahrenheit(30.0) - 0.0001);

        KStream<String, Reading> byStation = parsed.selectKey((key, reading) -> reading.station());

        KTable<String, StationStats> stats = byStation
                .groupByKey(Grouped.with(Serdes.String(), readingSerde()))
                .aggregate(
                        StationStats::new,
                        (station, reading, aggregate) -> aggregate.add(reading),
                        org.apache.kafka.streams.kstream.Materialized.with(Serdes.String(), stationStatsSerde()));

        stats.toStream()
                .mapValues((station, s) -> String.format(Locale.ROOT,
                        "%s : Temperature moyenne = %.1f F, Humidite moyenne = %.1f %%",
                        station, s.avgTemperature(), s.avgHumidity()))
                .to("station-averages", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Arret propre de l'application meteo...");
            streams.close();
        }));

        streams.start();
    }
}
