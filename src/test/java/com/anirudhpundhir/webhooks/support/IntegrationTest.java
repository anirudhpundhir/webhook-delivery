package com.anirudhpundhir.webhooks.support;

import com.anirudhpundhir.webhooks.config.WebhookProperties;
import com.anirudhpundhir.webhooks.delivery.DeliveryRepository;
import com.anirudhpundhir.webhooks.delivery.Dispatcher;
import com.anirudhpundhir.webhooks.delivery.HttpSender;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Runs against a real Postgres: {@code WEBHOOK_TEST_DATABASE_URL} (JDBC URL),
 * default {@code jdbc:postgresql://localhost:5432/webhooks_test}. The Spring
 * dispatcher bean is disabled; tests start dispatchers explicitly so they can
 * run several "instances" with fast timings.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "webhooks.dispatcher-enabled=false")
public abstract class IntegrationTest {
    @DynamicPropertySource
    static void db(DynamicPropertyRegistry r) {
        String url = System.getenv().getOrDefault("WEBHOOK_TEST_DATABASE_URL", "jdbc:postgresql://localhost:5432/webhooks_test");
        r.add("spring.datasource.url", () -> url);
    }

    @Autowired protected JdbcTemplate jdbc;
    @Autowired protected DeliveryRepository deliveries;
    @LocalServerPort protected int port;

    protected TestReceiver receiver;
    private final List<Dispatcher> dispatchers = new ArrayList<>();

    @BeforeEach
    void resetState() throws Exception {
        jdbc.execute("TRUNCATE delivery_attempts, deliveries, events, endpoints");
        receiver = new TestReceiver();
    }

    @AfterEach
    void stopAll() {
        dispatchers.forEach(Dispatcher::close);
        dispatchers.clear();
        receiver.close();
    }

    /** Fast timings: 20ms polls, 50ms base backoff, 5 attempts. */
    protected static WebhookProperties fastProps(int maxAttempts, Duration lease) {
        return new WebhookProperties(false, Duration.ofMillis(20), 50, 256, lease, Duration.ofSeconds(2),
                Duration.ofSeconds(1), maxAttempts, Duration.ofMillis(50), Duration.ofMillis(400));
    }

    protected Dispatcher startDispatcher(WebhookProperties p) {
        HttpSender sender = new HttpSender(p);
        Dispatcher d = new Dispatcher(deliveries, sender::send, p).start();
        dispatchers.add(d);
        return d;
    }

    protected static void await(Duration timeout, BooleanSupplier cond) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!cond.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(10);
        }
    }

    protected int count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Integer.class, args);
    }
}
