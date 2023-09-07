package com.github.fdkvandr.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class OperatorsTest {

    @Test
    public void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4 )
                .map(it -> {
                    log.info("Map 1 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                }).subscribeOn(Schedulers.boundedElastic())
                .map(it -> {
                    log.info("Map 2 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                });

        flux.subscribe();
    }

    @Test
    public void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4 )
                .map(it -> {
                    log.info("Map 1 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                }).publishOn(Schedulers.boundedElastic())
                .map(it -> {
                    log.info("Map 2 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                });

        flux.subscribe();
        flux.subscribe();
    }

    @Test
    public void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4 )
                .subscribeOn(Schedulers.single())
                .map(it -> {
                    log.info("Map 1 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                }).subscribeOn(Schedulers.boundedElastic())
                .map(it -> {
                    log.info("Map 2 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                });

        flux.subscribe();
    }

    @Test
    public void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4 )
                .publishOn(Schedulers.single())
                .map(it -> {
                    log.info("Map 1 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                }).publishOn(Schedulers.boundedElastic())
                .map(it -> {
                    log.info("Map 2 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                });

        flux.subscribe();
    }

    @Test
    public void publishOnAndSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4 )
                .publishOn(Schedulers.single())
                .map(it -> {
                    log.info("Map 1 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                }).subscribeOn(Schedulers.boundedElastic())
                .map(it -> {
                    log.info("Map 2 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                });

        flux.subscribe();
    }

    @Test
    public void subscribeOnAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4 )
                .subscribeOn(Schedulers.single())
                .map(it -> {
                    log.info("Map 1 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                }).publishOn(Schedulers.boundedElastic())
                .map(it -> {
                    log.info("Map 2 - Number: {} on Thread {}", it, Thread.currentThread().getName());
                    return it;
                });

        flux.subscribe();
    }
}
