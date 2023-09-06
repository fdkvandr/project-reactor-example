package com.github.fdkvandr.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j
public class MonoTest {

    @Test
    public void monoSubscriber() {
        String name = "Andrey Fedyakov";
        Mono<String> mono = Mono.just(name)
                .log();
        mono.subscribe();
        log.info("------------------------------");
        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumer() {
        String name = "Andrey Fedyakov";
        Mono<String> mono = Mono.just(name)
                .log();
        mono.subscribe(string -> log.info("Value: {}", string ));
        log.info("------------------------------");
        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerError() {
        String name = "Andrey Fedyakov";
        Mono<String> mono = Mono.just(name)
                .map(it -> {throw new RuntimeException("Testing Mono with Error");});
        mono.subscribe(
                string -> log.info("Value: {}", string ),
                error -> log.error("Error: {}", error.getMessage()));
        mono.subscribe(
                string -> log.info("Value: {}", string ),
                Throwable::printStackTrace);
        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    public void monoSubscriberConsumerComplete() {
        String name = "Andrey Fedyakov";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(
                string -> log.info("Value: {}", string ),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"));

        log.info("------------------------------");
        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerSubscription() {
        String name = "Andrey Fedyakov";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(
                string -> log.info("Value: {}", string ),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"),
                subscription -> subscription.request(5));

        log.info("------------------------------");
        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

    @Test
    public void monoDoOnMethods() {
        String name = "Andrey Fedyakov";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("SUBSCRIBED!"))
                .doOnRequest(longNumber -> log.info("Request received, starting doing smth..."))
                .doOnNext(it -> log.info("Value is here. Executing doOnNext({})", it))
                .doOnSuccess(it -> log.info("doOnSuccess executed, value={}", it));

        mono.subscribe(
                string -> log.info("Value: {}", string ),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"));
    }
}
