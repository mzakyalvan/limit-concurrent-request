package com.tiket.poc.bulkhead.rest;

import static org.assertj.core.api.Assertions.assertThat;

import com.tiket.poc.bulkhead.SampleApplication;
import com.tiket.poc.bulkhead.rest.PingEndpointConfiguration.Pong;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.jupiter.MockServerSettings;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * @author zakyalvan
 */
@SpringBootTest(classes = SampleApplication.class, webEnvironment = WebEnvironment.DEFINED_PORT)
@MockServerSettings(ports = 2345)
class PingEndpointTests {
  @Autowired
  private WebClient.Builder webClients;

  @Test
  void whenConcurrentCallsBellowLimit_thenProcessAllRequests(MockServerClient mockServer) {
    mockServer.when(REMOTE_PING_REQUEST)
        .respond(REMOTE_PING_RESPONSE.withDelay(TimeUnit.MILLISECONDS, 500));

    WebClient webClient = webClients.clone().baseUrl("http://localhost:8080/ping").build();

    Flux<Pong> testStream = Flux.mergeDelayError(10,
        webClient.get().uri(builder -> builder.queryParam("correlation", "first").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "second").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "third").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "fourth").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "fifth").build()).retrieve().bodyToMono(Pong.class)
    );

    StepVerifier.create(testStream)
        .expectSubscription().thenAwait()
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    mockServer.verify(REMOTE_PING_REQUEST, VerificationTimes.exactly(5));
  }

  @Test
  void whenConcurrentCallsExceedsLimit_thenOnlyProcessAllowed(MockServerClient mockServer) {
    HttpRequest mockRequest = HttpRequest.request("/remote/ping")
        .withMethod("GET")
        .withHeader("Accept-Encoding", "gzip")
        .withHeader("Accept", "application/json")
        .withKeepAlive(true);

    HttpResponse mockResponse = HttpResponse.response()
        .withHeader("Content-Type", "application/json")
        .withBody("{\"timestamp\" : \""+ LocalDateTime.now().toString() +"\"}");

    mockServer.when(mockRequest).respond(mockResponse.withDelay(TimeUnit.SECONDS, 2));

    WebClient webClient = webClients.clone().baseUrl("http://localhost:8080/ping").build();

    Flux<Pong> testStream = Flux.mergeDelayError(10,
        webClient.get().uri(builder -> builder.queryParam("correlation", "first").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "second").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "third").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "fourth").build()).retrieve().bodyToMono(Pong.class)
    );

    StepVerifier.create(testStream)
        .expectSubscription().thenAwait()
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WebClientResponseException.ServiceUnavailable.class))
        .verify(Duration.ofSeconds(5));

    mockServer.verify(mockRequest, VerificationTimes.exactly(3));
  }

  @Test
  void whenConcurrentCallsButBulkheadDisabled_thenProcessAllRequests(MockServerClient mockServer) {
    mockServer.when(REMOTE_PING_REQUEST)
        .respond(REMOTE_PING_RESPONSE.withDelay(TimeUnit.SECONDS, 2));

    WebClient webClient = webClients.clone().baseUrl("http://localhost:8080/ping?bulkhead=disabled").build();

    Flux<Pong> testStream = Flux.mergeDelayError(10,
        webClient.get().uri(builder -> builder.queryParam("correlation", "first").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "second").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "third").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "fourth").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "fifth").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "sixth").build()).retrieve().bodyToMono(Pong.class),
        webClient.get().uri(builder -> builder.queryParam("correlation", "seventh").build()).retrieve().bodyToMono(Pong.class)
    );

    StepVerifier.create(testStream)
        .expectSubscription().thenAwait()
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .assertNext(pong -> assertThat(pong.getTimestamp()).isNotNull())
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    mockServer.verify(REMOTE_PING_REQUEST, VerificationTimes.exactly(7));
  }

  @BeforeEach
  void setUp(MockServerClient mockServer) {
    mockServer.reset();
  }

  private static final HttpRequest REMOTE_PING_REQUEST = HttpRequest.request("/remote/ping")
      .withMethod("GET")
      .withHeader("Accept-Encoding", "gzip")
      .withHeader("Accept", "application/json")
      .withKeepAlive(true);

  private static final HttpResponse REMOTE_PING_RESPONSE = HttpResponse.response()
      .withHeader("Content-Type", "application/json")
      .withBody("{\"timestamp\" : \""+ LocalDateTime.now().toString() +"\"}");
}