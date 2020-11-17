package com.tiket.poc.bulkhead.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.server.HandlerFunction;
import org.springframework.web.reactive.function.server.RequestPredicate;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * @author zakyalvan
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
public class PingEndpointConfiguration {
  @Bean
  RouterFunction<ServerResponse> pingEndpoint(PingClient pingClient, ConcurrencySettings concurrencySettings) {
    RequestPredicate predicate = RequestPredicates.GET("/ping")
        .and(RequestPredicates.accept(MediaType.APPLICATION_JSON));

    Bulkhead bulkhead = Bulkhead.of("ping-bulkhead", BulkheadConfig.custom()
        .maxConcurrentCalls(concurrencySettings.getMaxConcurrentCalls())
        .maxWaitDuration(concurrencySettings.getMaxWaitDuration())
        .writableStackTraceEnabled(true)
        .build());

    /**
     * Disable bulkhead when query parameter `bulkhead=disabled` sent.
     */
    Predicate<ServerRequest> disabled = request -> request.queryParam("bulkhead")
        .filter(StringUtils::hasText)
        .map(value -> value.trim().toLowerCase().equals("disabled"))
        .orElse(false);

    HandlerFunction<ServerResponse> handler = request -> pingClient.ping()
        .transformDeferred(disabled.test(request) ? Function.identity() : BulkheadOperator.of(bulkhead))
        .flatMap(pong -> ServerResponse.ok().bodyValue(pong))
        .onErrorResume(BulkheadFullException.class, error -> ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE).build())
        .subscribeOn(Schedulers.newSingle("single-scheduler"))
        .doOnSubscribe(subscription -> log.info("Handle ping request with correlation : '{}'", request.queryParam("correlation").orElse("unknown")));

    return RouterFunctions.route(predicate, handler);
  }

  @Bean
  PingClient pingClient(WebClient.Builder webClients) {
    WebClient webClient = webClients.clone().baseUrl("http://localhost:2345/remote/ping").build();
    return () -> webClient.get().accept(MediaType.APPLICATION_JSON).retrieve().bodyToMono(Pong.class);
  }

  @Bean
  @ConfigurationProperties(prefix = "endpoint.ping.concurrency")
  ConcurrencySettings concurrencySettings() {
    return new ConcurrencySettings();
  }

  @Data
  static class ConcurrencySettings {
    private Integer maxConcurrentCalls = 3;
    private Duration maxWaitDuration = Duration.ofSeconds(1);
  }

  interface PingClient {
    Mono<Pong> ping();
  }

  @Getter
  static class Pong {
    private final LocalDateTime timestamp;

    @JsonCreator
    public Pong(LocalDateTime timestamp) {
      if(timestamp == null) {
        this.timestamp = LocalDateTime.now();
      }
      else {
        this.timestamp = timestamp;
      }
    }
  }
}
