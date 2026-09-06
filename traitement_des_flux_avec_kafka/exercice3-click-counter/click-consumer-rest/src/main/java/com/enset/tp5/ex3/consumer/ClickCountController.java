package com.enset.tp5.ex3.consumer;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Exercice 3 - Partie 3 : API REST exposant le nombre de clics.
 */
@RestController
public class ClickCountController {

    private final ClickCountListener listener;

    public ClickCountController(ClickCountListener listener) {
        this.listener = listener;
    }

    @GetMapping("/clicks/count")
    public Map<String, Long> count() {
        return Map.of("totalClicks", listener.getTotalClicks());
    }
}
