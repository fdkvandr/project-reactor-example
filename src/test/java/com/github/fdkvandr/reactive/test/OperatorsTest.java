package com.github.fdkvandr.reactive.test;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

@Slf4j
public class OperatorsTest {

    @BeforeAll
    public static  void setUp() {
        BlockHound.install(builder ->
                builder.allowBlockingCallsInside("org.slf4j.impl.SimpleLogger", "write"));
    }

    @Test
    public void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
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
        Flux<Integer> flux = Flux.range(1, 4)
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
        Flux<Integer> flux = Flux.range(1, 4).subscribeOn(Schedulers.single())
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
        Flux<Integer> flux = Flux.range(1, 4).publishOn(Schedulers.single())
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
        Flux<Integer> flux = Flux.range(1, 4).publishOn(Schedulers.single())
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
        Flux<Integer> flux = Flux.range(1, 4).subscribeOn(Schedulers.single())
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
    public void subscribeOnIO() throws InterruptedException {
        Mono<List<String>> mono = Mono.fromCallable(() -> Files.readAllLines(Path.of("test.txt")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        mono.subscribe(it -> log.info("{}", it));
        Thread.sleep(2000L);

        log.info("------------------------------");
        StepVerifier.create(mono).expectSubscription().thenConsumeWhile(it -> {
            Assertions.assertFalse(it.isEmpty());
            log.info("Size {}", it.size());
            return true;
        }).verifyComplete();
    }

    @Test
    public void switchIfEmptyOperator() {
        Flux<Object> flux = emptyFlux().switchIfEmpty(Flux.just(1, 2, 3))
                .log();
        flux.subscribe();
    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }

    @Test
    public void deferOperator() throws InterruptedException {
        Mono<Long> mono = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        mono.subscribe(it -> log.info("time: {}", it));
        Thread.sleep(100L);
        mono.subscribe(it -> log.info("time: {}", it));
        Thread.sleep(100L);
        mono.subscribe(it -> log.info("time: {}", it));
        Thread.sleep(100L);
        mono.subscribe(it -> log.info("time: {}", it));
    }

    @Test
    public void concatOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concat(flux1, flux2)
                .log();

        concatFlux.subscribe(it -> log.info("Value: {}", it));
    }

    @Test
    public void concatWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatWithFlux = flux1.concatWith(flux2)
                .log();

        concatWithFlux.subscribe(it -> log.info("Value: {}", it));
    }

    @Test
    public void combineLatestOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a1", "a2", "a3", "a4" )
                .delayElements(Duration.ofMillis(1000));
        Flux<String> flux2 = Flux.just("b1", "b2", "b3", "b4")
                .delayElements(Duration.ofMillis(500));

        Flux<String> combineLatestFlux = Flux.combineLatest(flux1, flux2, (el1, el2) -> el1 + el2).log();

        combineLatestFlux.subscribe(it -> log.info("Value: {}", it));

        Thread.sleep(5000L);
    }

    @Test
    public void mergeOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a1", "a2", "a3", "a4" )
                .delayElements(Duration.ofMillis(1000));
        Flux<String> flux2 = Flux.just("b1", "b2", "b3", "b4")
                .delayElements(Duration.ofMillis(500));

        Flux<String> mergeFlux = Flux.merge(flux1, flux2).log();

        mergeFlux.subscribe(it -> log.info("Value: {}", it));

        Thread.sleep(5000L);
    }

    @Test
    public void mergeWithOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a1", "a2", "a3", "a4" )
                .delayElements(Duration.ofMillis(1000));
        Flux<String> flux2 = Flux.just("b1", "b2", "b3", "b4")
                .delayElements(Duration.ofMillis(500));

        Flux<String> mergeFlux = flux1.mergeWith(flux2).log();

        mergeFlux.subscribe(it -> log.info("Value: {}", it));

        Thread.sleep(5000L);
    }

    @Test
    public void mergeSequentialOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a1", "a2", "a3", "a4" )
                .delayElements(Duration.ofMillis(1000));
        Flux<String> flux2 = Flux.just("b1", "b2", "b3", "b4")
                .delayElements(Duration.ofMillis(500));

        Flux<String> mergeFlux = Flux.concat(flux1, flux2, flux1).log();

        mergeFlux.subscribe(it -> log.info("Value: {}", it));

        Thread.sleep(11000L);
    }

    @Test
    public void concatDelayErrorOperator() {
        Flux<String> flux1 = Flux.just("a", "b", "e")
                .map(it -> {
                    if (it.equals("b")) {
                        throw new IllegalArgumentException("error flux1");
                    }
                    return it;
                });
        Flux<String> flux2 = Flux.just("c", "d", "f")
                .map(it -> {
                    if (it.equals("d")) {
                        throw new IllegalArgumentException("error flux2");
                    }
                    return it;
                });

        Flux<String> concatFlux = Flux.concatDelayError(flux1, flux2)
                .log();

        concatFlux.subscribe(it -> log.info("Value: {}", it));
    }

    @Test
    public void mergeDelayErrorOperator() throws InterruptedException {
        Flux<String> flux1 = Flux.just("a", "b", "e")
                .delayElements(Duration.ofMillis(1000))
                .map(it -> {
                    if (it.equals("b")) {
                        throw new IllegalArgumentException("error flux1");
                    }
                    return it;
                });
        Flux<String> flux2 = Flux.just("c", "d", "f")
                .delayElements(Duration.ofMillis(500))
                .map(it -> {
                    if (it.equals("d")) {
                        throw new IllegalArgumentException("error flux2");
                    }
                    return it;
                });

        Flux<String> concatFlux = Flux.mergeSequentialDelayError(1, flux1, flux2)
                .log();

        concatFlux.subscribe(it -> log.info("Value: {}", it));

        Thread.sleep(11000L);
    }

    public Flux<String> findByName(String name) {
        return name.equals("A")
                ? Flux.just("nameA1", "nameA2").delayElements(Duration.ofMillis(300))
                : Flux.just("nameB1", "nameB2").delayElements(Duration.ofMillis(100));
    }

    @Test
    public void flatMapOperator() throws InterruptedException {
        Flux<String> flux = Flux.just("a", "b").delayElements(Duration.ofMillis(100));

        Flux<String> flatMapFlux = flux.map(String::toUpperCase)
                .flatMapSequential(this::findByName).log();

        flatMapFlux.subscribe(log::info);

        Thread.sleep(1000L);
    }

    @AllArgsConstructor
    @Getter
    @ToString
    @EqualsAndHashCode
    static class Book {
        private String title;
        private String author;
        private int head;
    }

    @Test
    public void zipOperator() {
        Flux<String> titles = Flux.just("Clean Code", "Programming on Java", "Learning Java");
        Flux<String> authors = Flux.just("Dad Bob Martin", "Vasiliev");
        Flux<Integer> heads = Flux.just(12, 24);

        Flux<Book> bookFlux = Flux.zip(titles, authors, heads)
                .map(tuple -> new Book(tuple.getT1(), tuple.getT2(), tuple.getT3()));

        bookFlux.subscribe(it -> log.info(String.valueOf(it)));
    }

    @Test
    public void zipWithOperator() {
        Flux<String> titles = Flux.just("Clean Code", "Programming on Java", "Learning Java");
        Flux<Integer> heads = Flux.just(12, 24);

        Flux<Book> bookFlux = titles.zipWith(heads)
                .map(tuple -> new Book(tuple.getT1(), null, tuple.getT2()));

        bookFlux.subscribe(it -> log.info(String.valueOf(it)));
    }
}
