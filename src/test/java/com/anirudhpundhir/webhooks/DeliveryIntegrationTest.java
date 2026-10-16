package com.anirudhpundhir.webhooks;

import static org.assertj.core.api.Assertions.assertThat;

import com.anirudhpundhir.webhooks.delivery.DeliveryRepository.AttemptResult;
import com.anirudhpundhir.webhooks.delivery.DeliveryRepository.Claimed;
import com.anirudhpundhir.webhooks.delivery.DeliveryRepository.Completion;
import com.anirudhpundhir.webhooks.delivery.Dispatcher;
import com.anirudhpundhir.webhooks.event.EventPublisher;
import com.anirudhpundhir.webhooks.signing.WebhookSignature;
import com.anirudhpundhir.webhooks.support.IntegrationTest;
import com.anirudhpundhir.webhooks.support.TestReceiver.Received;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

class DeliveryIntegrationTest extends IntegrationTest {
    @Autowired ObjectMapper json;
    @Autowired EventPublisher publisher;
    @Autowired TransactionTemplate tx;
    RestClient api;

    @BeforeEach
    void client() {
        api = RestClient.builder().baseUrl("http://127.0.0.1:" + port).build();
    }

    record Ep(UUID id, String secret) {}

    Ep createEndpoint(String path, int maxConcurrency, List<String> types) {
        JsonNode r = api.post().uri("/v1/endpoints").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("url", receiver.url(path).toString(), "maxConcurrency", maxConcurrency, "eventTypes", types))
                .retrieve().body(JsonNode.class);
        return new Ep(UUID.fromString(r.get("id").asText()), r.get("secret").asText());
    }

    JsonNode publish(String type, Map<String, Object> data) {
        return api.post().uri("/v1/events").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("type", type, "data", data)).retrieve().body(JsonNode.class);
    }

    String status(UUID deliveryId) {
        return jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?", String.class, deliveryId);
    }

    UUID onlyDelivery(UUID endpoint) {
        return jdbc.queryForObject("SELECT id FROM deliveries WHERE endpoint_id = ? AND replay_of IS NULL", UUID.class, endpoint);
    }

    @Test
    void deliversSignedPayloadThatVerifies() throws Exception {
        Ep ep = createEndpoint("/a", 5, List.of());
        JsonNode pub = publish("payment.succeeded", Map.of("amount", 1999, "currency", "usd"));
        assertThat(pub.get("deliveries").asInt()).isEqualTo(1);
        startDispatcher(fastProps(5, Duration.ofSeconds(10)));
        await(Duration.ofSeconds(10), () -> receiver.requests("/a") == 1 && "succeeded".equals(status(onlyDelivery(ep.id()))));

        Received r = receiver.receivedOn("/a").get(0);
        WebhookSignature.verify(r.header("Webhook-Signature"), r.body(), List.of(ep.secret()),
                WebhookSignature.DEFAULT_TOLERANCE, Clock.systemUTC()); // throws if invalid
        JsonNode body = json.readTree(r.body());
        assertThat(body.get("type").asText()).isEqualTo("payment.succeeded");
        assertThat(body.get("data").get("amount").asInt()).isEqualTo(1999);
        assertThat(r.header("Webhook-Id")).isEqualTo(pub.get("eventId").asText());
        assertThat(r.header("Webhook-Attempt")).isEqualTo("1");

        JsonNode d = api.get().uri("/v1/deliveries/{id}", onlyDelivery(ep.id())).retrieve().body(JsonNode.class);
        assertThat(d.get("attempts").asInt()).isEqualTo(1);
        assertThat(d.get("attemptLog").get(0).get("statusCode").asInt()).isEqualTo(200);
    }

    @Test
    void routesByEventTypeAndSkipsDisabledEndpoints() {
        Ep payments = createEndpoint("/payments", 5, List.of("payment.succeeded"));
        Ep all = createEndpoint("/all", 5, List.of());
        Ep off = createEndpoint("/off", 5, List.of());
        api.patch().uri("/v1/endpoints/{id}", off.id()).contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("enabled", false)).retrieve().toBodilessEntity();
        assertThat(publish("customer.created", Map.of("x", 1)).get("deliveries").asInt()).isEqualTo(1);
        assertThat(publish("payment.succeeded", Map.of("x", 2)).get("deliveries").asInt()).isEqualTo(2);
        assertThat(count("SELECT count(*) FROM deliveries WHERE endpoint_id = ?", payments.id())).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM deliveries WHERE endpoint_id = ?", all.id())).isEqualTo(2);
    }

    @Test
    void outboxRowsRollBackWithTheBusinessTransaction() {
        createEndpoint("/a", 5, List.of());
        try {
            tx.executeWithoutResult(s -> {
                publisher.publish("order.created", json.createObjectNode().put("order", 1));
                throw new IllegalStateException("business logic failed after publishing");
            });
        } catch (IllegalStateException expected) {
            // rolled back
        }
        assertThat(count("SELECT count(*) FROM events")).isZero();
        assertThat(count("SELECT count(*) FROM deliveries")).isZero();
    }

    @Test
    void retriesWithBackoffUntilSuccess() throws Exception {
        Ep ep = createEndpoint("/flaky", 5, List.of());
        receiver.respond("/flaky", n -> n <= 2 ? 503 : 200);
        publish("x.y", Map.of("k", "v"));
        startDispatcher(fastProps(5, Duration.ofSeconds(10)));
        UUID id = onlyDelivery(ep.id());
        await(Duration.ofSeconds(10), () -> "succeeded".equals(status(id)));

        List<Received> got = receiver.receivedOn("/flaky");
        assertThat(got).hasSize(3);
        assertThat(got.stream().map(r -> r.header("Webhook-Attempt")).toList()).containsExactly("1", "2", "3");
        assertThat(got.stream().map(r -> new String(r.body())).distinct()).hasSize(1); // identical bytes each attempt
        // Gaps respect equal-jitter backoff: retry n waits >= ceiling(n)/2 = 25ms, 50ms.
        assertThat(Duration.ofNanos(got.get(1).atNanos() - got.get(0).atNanos())).isGreaterThanOrEqualTo(Duration.ofMillis(25));
        assertThat(Duration.ofNanos(got.get(2).atNanos() - got.get(1).atNanos())).isGreaterThanOrEqualTo(Duration.ofMillis(50));
        assertThat(count("SELECT count(*) FROM delivery_attempts WHERE delivery_id = ?", id)).isEqualTo(3);
    }

    @Test
    void deadLettersAfterMaxAttemptsThenReplayDelivers() throws Exception {
        Ep ep = createEndpoint("/down", 5, List.of());
        receiver.respond("/down", n -> 500);
        publish("x.y", Map.of("k", "v"));
        startDispatcher(fastProps(3, Duration.ofSeconds(10)));
        UUID id = onlyDelivery(ep.id());
        await(Duration.ofSeconds(10), () -> "dead".equals(status(id)));
        assertThat(receiver.requests("/down")).isEqualTo(3);

        JsonNode dead = api.get().uri("/v1/deliveries?status=dead&endpointId={e}", ep.id()).retrieve().body(JsonNode.class);
        assertThat(dead).hasSize(1);
        assertThat(dead.get(0).get("lastStatusCode").asInt()).isEqualTo(500);

        // Endpoint recovers; operator replays.
        receiver.respond("/down", n -> 200);
        JsonNode rep = api.post().uri("/v1/deliveries/{id}/replay", id).retrieve().body(JsonNode.class);
        UUID replay = UUID.fromString(rep.get("delivery_id").asText());
        await(Duration.ofSeconds(10), () -> "succeeded".equals(status(replay)));
        assertThat(receiver.requests("/down")).isEqualTo(4);
        assertThat(status(id)).isEqualTo("dead"); // original kept for audit

        // Replaying again is idempotent: same replay id, no new delivery.
        JsonNode again = api.post().uri("/v1/deliveries/{id}/replay", id).retrieve().body(JsonNode.class);
        assertThat(again.get("delivery_id").asText()).isEqualTo(replay.toString());
        assertThat(again.get("already_replayed").asBoolean()).isTrue();
    }

    @Test
    void bulkReplayOfDeadLetters() throws Exception {
        Ep ep = createEndpoint("/bulk", 10, List.of());
        receiver.respond("/bulk", n -> n <= 10 ? 500 : 200);
        for (int i = 0; i < 5; i++) {
            publish("x.y", Map.of("i", i));
        }
        startDispatcher(fastProps(2, Duration.ofSeconds(10)));
        await(Duration.ofSeconds(10), () -> count("SELECT count(*) FROM deliveries WHERE status = 'dead'") == 5);
        JsonNode r = api.post().uri("/v1/endpoints/{id}/replay-dead", ep.id()).retrieve().body(JsonNode.class);
        assertThat(r.get("replayed").asInt()).isEqualTo(5);
        assertThat(api.post().uri("/v1/endpoints/{id}/replay-dead", ep.id()).retrieve().body(JsonNode.class)
                .get("replayed").asInt()).isZero();
        await(Duration.ofSeconds(10), () -> count("SELECT count(*) FROM deliveries WHERE status = 'succeeded'") == 5);
    }

    @Test
    void replayOfUnfinishedDeliveryIsRejected() {
        Ep ep = createEndpoint("/a", 5, List.of());
        publish("x.y", Map.of());
        try {
            api.post().uri("/v1/deliveries/{id}/replay", onlyDelivery(ep.id())).retrieve().toBodilessEntity();
            throw new AssertionError("expected 409");
        } catch (HttpClientErrorException e) {
            assertThat(e.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        }
    }

    /**
     * Two dispatcher instances, one slow endpoint with max_concurrency=3 and a
     * fast neighbour. The limit must hold globally, and the slow endpoint must
     * not starve the other one.
     */
    @Test
    void perEndpointConcurrencyLimitHoldsAcrossDispatchers() throws Exception {
        Ep slow = createEndpoint("/slow", 3, List.of("slow"));
        Ep fast = createEndpoint("/fast", 50, List.of("fast"));
        receiver.latency("/slow", 150);
        for (int i = 0; i < 30; i++) {
            publish("slow", Map.of("i", i));
        }
        for (int i = 0; i < 30; i++) {
            publish("fast", Map.of("i", i));
        }
        startDispatcher(fastProps(5, Duration.ofSeconds(10)));
        startDispatcher(fastProps(5, Duration.ofSeconds(10)));
        long t0 = System.nanoTime();
        await(Duration.ofSeconds(20), () -> receiver.requests("/fast") == 30);
        Duration fastDone = Duration.ofNanos(System.nanoTime() - t0);
        await(Duration.ofSeconds(30), () -> count("SELECT count(*) FROM deliveries WHERE status = 'succeeded'") == 60);

        assertThat(receiver.maxConcurrent("/slow")).isLessThanOrEqualTo(3).isGreaterThanOrEqualTo(2);
        assertThat(receiver.requests("/slow")).isEqualTo(30);
        // 30 slow requests at <=3 concurrent x 150ms need >= 1.5s; fast ones must not wait behind them.
        assertThat(fastDone).isLessThan(Duration.ofMillis(1500));
        assertThat(count("SELECT count(*) FROM deliveries WHERE endpoint_id = ?", slow.id())).isEqualTo(30);
        assertThat(fast).isNotNull();
    }

    /** A dispatcher that dies mid-delivery: the lease expires and another instance redelivers. */
    @Test
    void expiredLeaseIsRedeliveredAndStaleCompletionIsDiscarded() throws Exception {
        Ep ep = createEndpoint("/a", 5, List.of());
        publish("x.y", Map.of());
        // "Crashed" worker: claims with a 200ms lease and never completes (yet).
        List<Claimed> claimed = deliveries.claim(10, Duration.ofMillis(200));
        assertThat(claimed).hasSize(1);
        assertThat(deliveries.claim(10, Duration.ofSeconds(10))).as("leased delivery is not re-claimable").isEmpty();

        Thread.sleep(300);
        startDispatcher(fastProps(5, Duration.ofSeconds(10)));
        UUID id = onlyDelivery(ep.id());
        await(Duration.ofSeconds(10), () -> "succeeded".equals(status(id)));

        // The zombie finally reports: its lease token is stale, so nothing changes.
        Completion c = deliveries.complete(claimed.get(0), new AttemptResult(Instant.now(), 1, 500, "HTTP 500"), 5, Duration.ZERO);
        assertThat(c).isEqualTo(Completion.LEASE_LOST);
        assertThat(status(id)).isEqualTo("succeeded");
        assertThat(count("SELECT count(*) FROM delivery_attempts WHERE delivery_id = ?", id)).isEqualTo(1);
    }

    /** Many concurrent claimers on the same rows: every delivery is claimed by exactly one of them. */
    @Test
    void concurrentClaimsNeverDoubleClaim() throws Exception {
        createEndpoint("/a", 1000, List.of());
        for (int i = 0; i < 200; i++) {
            publish("x.y", Map.of("i", i));
        }
        List<Thread> threads = new java.util.ArrayList<>();
        Set<UUID> seen = java.util.concurrent.ConcurrentHashMap.newKeySet();
        java.util.concurrent.atomic.AtomicInteger dup = new java.util.concurrent.atomic.AtomicInteger();
        for (int t = 0; t < 8; t++) {
            threads.add(Thread.ofVirtual().start(() -> {
                List<Claimed> got;
                do {
                    got = deliveries.claim(7, Duration.ofSeconds(30));
                    for (Claimed c : got) {
                        if (!seen.add(c.id())) {
                            dup.incrementAndGet();
                        }
                    }
                } while (!got.isEmpty() || seen.size() < 200 && count("SELECT count(*) FROM deliveries WHERE status='pending'") > 0);
            }));
        }
        for (Thread t : threads) {
            t.join();
        }
        assertThat(dup.get()).isZero();
        assertThat(seen).hasSize(200);
        assertThat(count("SELECT count(*) FROM deliveries WHERE status = 'in_flight'")).isEqualTo(200);
    }

    @Test
    void validationErrors() {
        for (Map<String, Object> bad : List.of(
                Map.<String, Object>of("url", "not a url"),
                Map.<String, Object>of("url", "ftp://x/y"),
                Map.<String, Object>of("url", "http://x/y", "maxConcurrency", 0))) {
            try {
                api.post().uri("/v1/endpoints").contentType(MediaType.APPLICATION_JSON).body(bad).retrieve().toBodilessEntity();
                throw new AssertionError("accepted " + bad);
            } catch (HttpClientErrorException e) {
                assertThat(e.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
            }
        }
        try {
            api.post().uri("/v1/events").contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("type", "x.y", "data", List.of(1))).retrieve().toBodilessEntity();
            throw new AssertionError("accepted array data");
        } catch (HttpClientErrorException e) {
            assertThat(e.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        }
        assertThat(Set.of("a").stream().collect(Collectors.toSet())).hasSize(1);
    }

    @Test
    void gracefulCloseWaitsForInFlight() throws Exception {
        Ep ep = createEndpoint("/slow", 5, List.of());
        receiver.latency("/slow", 300);
        publish("x.y", Map.of());
        Dispatcher d = new Dispatcher(deliveries, new com.anirudhpundhir.webhooks.delivery.HttpSender(
                fastProps(5, Duration.ofSeconds(10)))::send, fastProps(5, Duration.ofSeconds(10))).start();
        await(Duration.ofSeconds(5), () -> receiver.requests("/slow") == 1);
        d.close();
        assertThat(status(onlyDelivery(ep.id()))).isEqualTo("succeeded");
    }
}
