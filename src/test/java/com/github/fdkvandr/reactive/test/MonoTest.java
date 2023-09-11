package com.github.fdkvandr.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MonoTest {

    @BeforeAll
    public static  void setUp() {
        BlockHound.install();
    }

    @Test
    public void BlockHoundWorks() {
        try {
            FutureTask<?> task = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });
            Schedulers.parallel().schedule(task);

            task.get(10, TimeUnit.SECONDS);
            Assertions.fail("should fail");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof BlockingOperationError);
        }
    }

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

    @Test
    public void monoDoOnError() {
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument"))
                .doOnError(e -> log.info("Error message: {}", e.getMessage()))
                .log();

        StepVerifier.create(error)
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    public void monoOnErrorResume() {
        String name = "Andrey Fedyakov";

        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument"))
                .onErrorResume(e -> {
                    log.info("Inside On Error Resume");
                    return Mono.just(name);
                })
                .doOnError(e -> log.info("Error message: {}", e.getMessage()))
                .log();

        StepVerifier.create(error)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    public void monoOnErrorReturn() {
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument"))
                .onErrorReturn("EMPTY")
                .doOnError(e -> log.info("Error message: {}", e.getMessage()))
                .log();

        StepVerifier.create(error)
                .expectNext("EMPTY")
                .verifyComplete();
    }
}
