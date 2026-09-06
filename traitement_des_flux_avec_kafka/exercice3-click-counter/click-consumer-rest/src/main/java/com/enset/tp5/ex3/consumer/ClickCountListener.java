package com.enset.tp5.ex3.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Ecoute le topic "click-counts" et garde en memoire la derniere valeur
 * connue du compteur global de clics.
 */
@Component
public class ClickCountListener {

    private final AtomicLong totalClicks = new AtomicLong(0);

    @KafkaListener(topics = "click-counts", groupId = "click-consumer-rest")
    public void onMessage(ConsumerRecord<String, String> record) {
        try {
            totalClicks.set(Long.parseLong(record.value()));
        } catch (NumberFormatException ignored) {
            // valeur inattendue, on l'ignore plutot que de faire planter le consommateur
        }
    }

    public long getTotalClicks() {
        return totalClicks.get();
    }
}
