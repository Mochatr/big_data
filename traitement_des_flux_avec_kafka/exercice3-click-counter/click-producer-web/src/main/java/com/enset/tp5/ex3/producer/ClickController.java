package com.enset.tp5.ex3.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exercice 3 - Partie 1 : chaque appel POST /click envoie un evenement
 * "key = userId, value = click" dans le topic Kafka "clicks".
 */
@RestController
public class ClickController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ClickController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/click")
    public String click(@RequestParam(defaultValue = "user1") String userId) {
        kafkaTemplate.send("clicks", userId, "click");
        return "Clic envoye pour " + userId;
    }
}
