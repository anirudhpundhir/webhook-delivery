package com.anirudhpundhir.webhooks.delivery;

import com.anirudhpundhir.webhooks.config.WebhookProperties;
import com.anirudhpundhir.webhooks.delivery.DeliveryRepository.AttemptResult;
import com.anirudhpundhir.webhooks.delivery.DeliveryRepository.Claimed;
import com.anirudhpundhir.webhooks.signing.WebhookSignature;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.Executors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Performs one signed HTTP POST attempt. Never throws; failures become an {@link AttemptResult}. */
@Component
public class HttpSender {
    private final HttpClient client;
    private final WebhookProperties props;
    private final Clock clock;

    @Autowired
    public HttpSender(WebhookProperties props) {
        this(props, Clock.systemUTC());
    }

    HttpSender(WebhookProperties props, Clock clock) {
        this.props = props;
        this.clock = clock;
        this.client = HttpClient.newBuilder()
                .connectTimeout(props.connectTimeout())
                .followRedirects(HttpClient.Redirect.NEVER) // a redirect is a failed delivery, not a hop to an unvetted URL
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }

    public AttemptResult send(Claimed c) {
        byte[] body = c.body().getBytes(StandardCharsets.UTF_8);
        Instant start = clock.instant();
        // Signed at send time: a retry three days later still passes a 5-minute tolerance check.
        String signature = WebhookSignature.sign(c.secret(), start.getEpochSecond(), body);
        HttpRequest req = HttpRequest.newBuilder(c.url())
                .timeout(props.requestTimeout())
                .header("Content-Type", "application/json")
                .header("User-Agent", "webhook-delivery/0.1")
                .header("Webhook-Id", c.eventId().toString()) // stable across retries and replays: receiver dedup key
                .header("Webhook-Delivery-Id", c.id().toString())
                .header("Webhook-Event-Type", c.eventType())
                .header("Webhook-Attempt", Integer.toString(c.attempts() + 1))
                .header(WebhookSignature.HEADER, signature)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        try {
            HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
            return new AttemptResult(start, ms(start), resp.statusCode(),
                    resp.statusCode() / 100 == 2 ? null : "HTTP " + resp.statusCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new AttemptResult(start, ms(start), null, "interrupted");
        } catch (Exception e) {
            return new AttemptResult(start, ms(start), null, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private long ms(Instant start) {
        return Math.max(0, clock.millis() - start.toEpochMilli());
    }
}
