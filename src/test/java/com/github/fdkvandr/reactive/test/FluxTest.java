package com.github.fdkvandr.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@Slf4j
public class FluxTest {

    @Test
    public void fluxSubscriber() {
        Flux<String> flux = Flux.just("Andrey", "Fedyakov", "Alexandrovich")
                .log();

        StepVerifier.create(flux)
                .expectNext("Andrey", "Fedyakov", "Alexandrovich")
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbers() {
        Flux<Integer> flux = Flux.range(1, 5).log();

        flux.subscribe(it -> log.info("Number: {}", it));
        log.info("------------------------------");

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }
}
