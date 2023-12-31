package com.github.fdkvandr.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
public class FluxTest {

    @BeforeAll
    public static  void setUp() {
        BlockHound.install();
    }

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

    @Test
    public void fluxSubscriberFromList() {
        Flux<Integer> flux = Flux.fromIterable(List.of(1, 2, 3, 4 ,5)).log();

        flux.subscribe(it -> log.info("Number: {}", it));
        log.info("------------------------------");

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersError() {
        Flux<Integer> flux = Flux.range(1, 5)
                .map(it -> {
                    if (it == 4) throw new IndexOutOfBoundsException("Index error");
                    return it;
                })
                .log();

        flux.subscribe(
                it -> log.info("Number: {}", it),
                Throwable::printStackTrace,
                () -> log.info("DONE!"),
                subscription -> subscription.request(3));
    }

    @Test
    public void fluxSubscriberNumbersUglyBackpressure() {

        Flux<Integer> flux = Flux.range(1, 10)
                .log();

        flux.subscribe(new Subscriber<>() {

            private int count = 0;
            private Subscription subscription;
            private final int requestCount = 2;


            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(requestCount);
            }

            @Override
            public void onNext(Integer item) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    subscription.request(requestCount);
                }
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
            }
        });
    }

    @Test
    public void fluxSubscriberNumbersNotSoUglyBackpressure() {

        Flux<Integer> flux = Flux.range(1, 10)
                .log();

        flux.subscribe(new BaseSubscriber<>() {

            private int count = 0;
            private final int requestCount = 2;


            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });
    }

    @Test
    public void fluxSubscriberIntervalOne() throws InterruptedException {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
                .take(5)
                .log();

        interval.subscribe(it -> log.info("Number: {}", it));

        Thread.sleep(1000);
    }

    @Test
    public void fluxSubscriberIntervalTwo() {
        StepVerifier.withVirtualTime(this::createInterval)
                .expectSubscription()
                .expectNoEvent(Duration.ofDays(1))
                // .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenCancel()
                .verify();
    }

    private Flux<Long> createInterval() {
        return Flux.interval(Duration.ofDays(1))
                .log();
    }

    @Test
    public void fluxSubscriberPrettyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10)
                .log()
                .limitRate(3);


        flux.subscribe(it -> log.info("Number: {}", it));
        log.info("------------------------------");

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    public void connectableFlux() throws InterruptedException {
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
                .delayElements(Duration.ofMillis(100))
                .log()
                .limitRate(1)
                .publish();

        connectableFlux.connect();

        log.info("Thread sleeping for 300ms");
        Thread.sleep(300L);
        connectableFlux.subscribe(it -> log.info("Subscriber1 number: {}", it));
        log.info("Thread sleeping for 200ms");
        Thread.sleep(200L);
        connectableFlux.subscribe(it -> log.info("Subscriber2 number: {}", it));

        Thread.sleep(3000L);

        log.info("------------------------------");
        StepVerifier
                .create(connectableFlux)
                .then(connectableFlux::connect)
                .thenConsumeWhile(it -> it <= 5)
                .expectNext(6, 7, 8, 9, 10)
                .expectComplete()
                .verify();
    }

    @Test
    public void connectableFluxAutoConnect() throws InterruptedException {
        Flux<Integer> flux = Flux.range(1, 5)
                .delayElements(Duration.ofMillis(100))
                .log()
                .limitRate(1)
                .publish()
                .autoConnect(2);


        log.info("Thread sleeping for 300ms");
        Thread.sleep(300L);
        flux.subscribe(it -> log.info("Subscriber1 number: {}", it));
        log.info("Thread sleeping for 200ms");
        Thread.sleep(200L);
        flux.subscribe(it -> log.info("Subscriber2 number: {}", it));

        Thread.sleep(3000L);

        // log.info("------------------------------");
        // StepVerifier
        //         .create(flux)
        //         .then(flux::subscribe)
        //         .expectNext(1, 2, 3, 4, 5)
        //         .expectComplete()
        //         .verify();
    }
}
