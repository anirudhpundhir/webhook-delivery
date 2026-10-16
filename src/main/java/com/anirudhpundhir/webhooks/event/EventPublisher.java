package com.anirudhpundhir.webhooks.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * The write side of the transactional outbox.
 *
 * <p>{@link #publish} must run inside the caller's transaction
 * ({@code MANDATORY}): the event and one {@code deliveries} row per subscribed
 * endpoint commit or roll back together with the caller's business change.
 * No message broker, no dual write.
 */
@Service
public class EventPublisher {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;

    public EventPublisher(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    public record Published(UUID eventId, int deliveries) {}

    @Transactional(propagation = Propagation.MANDATORY)
    public Published publish(String type, JsonNode data) {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        ObjectNode envelope = json.createObjectNode();
        envelope.put("id", id.toString());
        envelope.put("type", type);
        envelope.put("created_at", now.toString());
        envelope.set("data", data);
        String body;
        try {
            body = json.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalArgumentException("unserializable payload", e);
        }
        jdbc.update("INSERT INTO events (id, type, body, created_at) VALUES (?, ?, ?, ?)", id, type, body,
                java.sql.Timestamp.from(now));
        int n = jdbc.update(
                """
                INSERT INTO deliveries (id, event_id, endpoint_id, status)
                SELECT gen_random_uuid(), ?, e.id, 'pending'
                FROM endpoints e
                WHERE e.enabled AND (cardinality(e.event_types) = 0 OR ? = ANY (e.event_types))
                """,
                id,
                type);
        return new Published(id, n);
    }
}
