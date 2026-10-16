package com.anirudhpundhir.webhooks;

import com.anirudhpundhir.webhooks.event.EventPublisher;
import com.anirudhpundhir.webhooks.signing.WebhookSignature;
import com.anirudhpundhir.webhooks.support.IntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * End-to-end dispatch throughput: outbox rows → claim → signed HTTP POST to a
 * local receiver → completion. Excluded from `mvn test`; run with `make bench`.
 * Prints measured numbers only; nothing is asserted about speed.
 */
@Tag("benchmark")
class DispatchBenchmark extends IntegrationTest {
    @Autowired EventPublisher publisher;
    @Autowired TransactionTemplate tx;
    @Autowired ObjectMapper json;

    @ParameterizedTest(name = "{0} deliveries, {1} endpoints, {2} dispatcher(s), receiver latency {3}ms")
    @CsvSource({"20000, 10, 1, 0", "20000, 10, 2, 0", "20000, 10, 1, 20", "20000, 10, 2, 20"})
    void throughput(int total, int endpoints, int dispatchers, int latencyMs) throws Exception {
        for (int i = 0; i < endpoints; i++) {
            jdbc.update("INSERT INTO endpoints (id, url, secret, max_concurrency) VALUES (gen_random_uuid(), ?, ?, 64)",
                    receiver.url("/e" + i).toString(), WebhookSignature.newSecret());
            receiver.latency("/e" + i, latencyMs);
        }
        int events = total / endpoints;
        for (int b = 0; b < events; b += 500) {
            int from = b;
            tx.executeWithoutResult(s -> {
                for (int i = from; i < Math.min(from + 500, events); i++) {
                    publisher.publish("bench.event", json.createObjectNode().put("i", i).put("pad", "x".repeat(200)));
                }
            });
        }
        long t0 = System.nanoTime();
        for (int d = 0; d < dispatchers; d++) {
            startDispatcher(new com.anirudhpundhir.webhooks.config.WebhookProperties(false, Duration.ofMillis(20), 100, 256,
                    Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofSeconds(1), 5, Duration.ofMillis(50),
                    Duration.ofSeconds(1)));
        }
        await(Duration.ofMinutes(5), () -> count("SELECT count(*) FROM deliveries WHERE status = 'succeeded'") == total);
        double secs = (System.nanoTime() - t0) / 1e9;
        System.out.printf("BENCH deliveries=%d endpoints=%d dispatchers=%d latency_ms=%d elapsed=%.2fs throughput=%.0f/s%n",
                total, endpoints, dispatchers, latencyMs, secs, total / secs);
        List<Integer> maxConc = java.util.stream.IntStream.range(0, endpoints).mapToObj(i -> receiver.maxConcurrent("/e" + i)).toList();
        System.out.println("BENCH max concurrent per endpoint (limit 64): " + maxConc);
    }
}
