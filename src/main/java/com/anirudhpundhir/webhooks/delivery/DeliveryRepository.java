package com.anirudhpundhir.webhooks.delivery;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * All dispatcher state transitions. Every timestamp comparison uses the
 * database clock ({@code now()}) so dispatcher hosts never need synchronised
 * clocks.
 *
 * <pre>
 * pending ──claim──▶ in_flight ──2xx──▶ succeeded
 *    ▲                  │  │
 *    └──fail, retry─────┘  └──fail, attempts exhausted──▶ dead
 *    └──lease expired (worker crashed) is re-claimable as if pending
 * </pre>
 */
@Repository
public class DeliveryRepository {
    private final JdbcTemplate jdbc;

    public DeliveryRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** A delivery claimed by a dispatcher, with everything needed to send it. */
    public record Claimed(
            UUID id, UUID leaseToken, int attempts, UUID eventId, String eventType, String body,
            UUID endpointId, URI url, String secret) {}

    private static final String DUE =
            "((d.status = 'pending' AND d.next_attempt_at <= now()) OR (d.status = 'in_flight' AND d.locked_until <= now()))";

    /**
     * Claims up to {@code limit} due deliveries without exceeding any endpoint's
     * {@code max_concurrency}, across all dispatcher instances.
     *
     * <p>Each endpoint row with due work is locked {@code FOR UPDATE SKIP LOCKED};
     * while holding it we count that endpoint's live leases and claim only the
     * free slots. Concurrent claimers therefore serialise per endpoint (and skip
     * endpoints another instance is busy claiming) so the limit is exact.
     */
    @Transactional
    public List<Claimed> claim(int limit, Duration lease) {
        List<Claimed> out = new ArrayList<>();
        if (limit <= 0) {
            return out;
        }
        List<Object[]> eps = jdbc.query(
                """
                SELECT e.id, e.max_concurrency FROM endpoints e
                WHERE e.enabled AND EXISTS (SELECT 1 FROM deliveries d WHERE d.endpoint_id = e.id AND %s)
                ORDER BY random()
                LIMIT 64
                FOR UPDATE OF e SKIP LOCKED
                """.formatted(DUE),
                (rs, i) -> new Object[] {rs.getObject(1, UUID.class), rs.getInt(2)});
        for (Object[] ep : eps) {
            if (out.size() >= limit) {
                break;
            }
            UUID endpointId = (UUID) ep[0];
            int max = (int) ep[1];
            Integer live = jdbc.queryForObject(
                    "SELECT count(*) FROM deliveries WHERE endpoint_id = ? AND status = 'in_flight' AND locked_until > now()",
                    Integer.class,
                    endpointId);
            int slots = Math.min(max - live, limit - out.size());
            if (slots <= 0) {
                continue;
            }
            out.addAll(jdbc.query(
                    """
                    WITH c AS (
                        SELECT d.id FROM deliveries d
                        WHERE d.endpoint_id = ? AND %s
                        ORDER BY d.next_attempt_at
                        LIMIT ?
                        FOR UPDATE SKIP LOCKED
                    ), u AS (
                        UPDATE deliveries d
                        SET status = 'in_flight', lease_token = gen_random_uuid(),
                            locked_until = now() + ? * interval '1 millisecond', updated_at = now()
                        FROM c WHERE d.id = c.id
                        RETURNING d.id, d.lease_token, d.attempts, d.event_id, d.endpoint_id
                    )
                    SELECT u.*, ev.type, ev.body, e.url, e.secret
                    FROM u JOIN events ev ON ev.id = u.event_id JOIN endpoints e ON e.id = u.endpoint_id
                    """.formatted(DUE),
                    (rs, i) -> new Claimed(
                            rs.getObject("id", UUID.class),
                            rs.getObject("lease_token", UUID.class),
                            rs.getInt("attempts"),
                            rs.getObject("event_id", UUID.class),
                            rs.getString("type"),
                            rs.getString("body"),
                            rs.getObject("endpoint_id", UUID.class),
                            URI.create(rs.getString("url")),
                            rs.getString("secret")),
                    endpointId,
                    slots,
                    lease.toMillis()));
        }
        return out;
    }

    /** Outcome of one HTTP attempt. */
    public record AttemptResult(Instant startedAt, long durationMs, Integer statusCode, String error) {
        public boolean success() {
            return statusCode != null && statusCode >= 200 && statusCode < 300;
        }
    }

    public enum Completion {
        SUCCEEDED, RETRY_SCHEDULED, DEAD, LEASE_LOST
    }

    /**
     * Records an attempt and moves the delivery on, but only if we still hold
     * the lease. If the lease expired and another dispatcher re-claimed the
     * delivery, this attempt's outcome is discarded (the receiver may have seen
     * it twice: at-least-once).
     */
    @Transactional
    public Completion complete(Claimed c, AttemptResult r, int maxAttempts, Duration retryDelay) {
        int attempt = c.attempts() + 1;
        String status;
        Completion completion;
        if (r.success()) {
            status = "succeeded";
            completion = Completion.SUCCEEDED;
        } else if (attempt >= maxAttempts) {
            status = "dead";
            completion = Completion.DEAD;
        } else {
            status = "pending";
            completion = Completion.RETRY_SCHEDULED;
        }
        int n = jdbc.update(
                """
                UPDATE deliveries
                SET status = ?, attempts = ?, lease_token = NULL, locked_until = NULL,
                    last_status_code = ?, last_error = ?, updated_at = now(),
                    next_attempt_at = CASE WHEN ? = 'pending' THEN now() + ? * interval '1 millisecond' ELSE next_attempt_at END,
                    completed_at = CASE WHEN ? IN ('succeeded', 'dead') THEN now() END
                WHERE id = ? AND lease_token = ? AND status = 'in_flight'
                """,
                status, attempt, r.statusCode(), truncate(r.error()), status, retryDelay.toMillis(), status,
                c.id(), c.leaseToken());
        if (n == 0) {
            return Completion.LEASE_LOST;
        }
        jdbc.update(
                "INSERT INTO delivery_attempts (delivery_id, attempt, started_at, duration_ms, status_code, error) VALUES (?, ?, ?, ?, ?, ?)",
                c.id(), attempt, Timestamp.from(r.startedAt()), (int) Math.min(r.durationMs(), Integer.MAX_VALUE),
                r.statusCode(), truncate(r.error()));
        return completion;
    }

    private static String truncate(String s) {
        return s == null || s.length() <= 1000 ? s : s.substring(0, 1000);
    }

    // ---------- read & replay ----------

    public record Attempt(int attempt, Instant startedAt, int durationMs, Integer statusCode, String error) {}

    public record Delivery(
            UUID id, UUID eventId, UUID endpointId, String status, int attempts, Instant nextAttemptAt,
            Integer lastStatusCode, String lastError, UUID replayOf, Instant createdAt, Instant completedAt,
            List<Attempt> attemptLog) {}

    public Optional<Delivery> find(UUID id) {
        List<Attempt> attempts = jdbc.query(
                "SELECT * FROM delivery_attempts WHERE delivery_id = ? ORDER BY attempt",
                (rs, i) -> new Attempt(rs.getInt("attempt"), rs.getTimestamp("started_at").toInstant(),
                        rs.getInt("duration_ms"), (Integer) rs.getObject("status_code"), rs.getString("error")),
                id);
        return jdbc.query("SELECT * FROM deliveries WHERE id = ?", (rs, i) -> map(rs, attempts), id).stream().findFirst();
    }

    public List<Delivery> list(UUID endpointId, String status, int limit) {
        return jdbc.query(
                """
                SELECT * FROM deliveries
                WHERE (?::uuid IS NULL OR endpoint_id = ?) AND (?::text IS NULL OR status = ?)
                ORDER BY created_at DESC LIMIT ?
                """,
                (rs, i) -> map(rs, List.of()),
                endpointId, endpointId, status, status, limit);
    }

    private static Delivery map(ResultSet rs, List<Attempt> attempts) throws SQLException {
        Timestamp done = rs.getTimestamp("completed_at");
        return new Delivery(
                rs.getObject("id", UUID.class), rs.getObject("event_id", UUID.class),
                rs.getObject("endpoint_id", UUID.class), rs.getString("status"), rs.getInt("attempts"),
                rs.getTimestamp("next_attempt_at").toInstant(), (Integer) rs.getObject("last_status_code"),
                rs.getString("last_error"), rs.getObject("replay_of", UUID.class),
                rs.getTimestamp("created_at").toInstant(), done == null ? null : done.toInstant(), attempts);
    }

    public enum ReplayOutcome { CREATED, NOT_FOUND, NOT_FINISHED, ALREADY_REPLAYED }

    public record Replay(ReplayOutcome outcome, UUID newDeliveryId) {}

    /**
     * Replays a finished (succeeded or dead) delivery by creating a new pending
     * delivery of the same event to the same endpoint. The original row and its
     * attempt log are kept; a delivery can be replayed at most once (replay the
     * replay to send again), which makes the call safe to retry.
     */
    @Transactional
    public Replay replay(UUID id) {
        List<String> st = jdbc.queryForList("SELECT status FROM deliveries WHERE id = ? FOR UPDATE", String.class, id);
        if (st.isEmpty()) {
            return new Replay(ReplayOutcome.NOT_FOUND, null);
        }
        if (!st.get(0).equals("succeeded") && !st.get(0).equals("dead")) {
            return new Replay(ReplayOutcome.NOT_FINISHED, null);
        }
        List<UUID> existing = jdbc.queryForList("SELECT id FROM deliveries WHERE replay_of = ?", UUID.class, id);
        if (!existing.isEmpty()) {
            return new Replay(ReplayOutcome.ALREADY_REPLAYED, existing.get(0));
        }
        UUID newId = UUID.randomUUID();
        jdbc.update(
                """
                INSERT INTO deliveries (id, event_id, endpoint_id, status, replay_of)
                SELECT ?, event_id, endpoint_id, 'pending', id FROM deliveries WHERE id = ?
                """,
                newId, id);
        return new Replay(ReplayOutcome.CREATED, newId);
    }

    /** Replays every dead delivery of an endpoint that has not been replayed yet. */
    @Transactional
    public int replayDead(UUID endpointId, Instant since) {
        return jdbc.update(
                """
                INSERT INTO deliveries (id, event_id, endpoint_id, status, replay_of)
                SELECT gen_random_uuid(), d.event_id, d.endpoint_id, 'pending', d.id
                FROM deliveries d
                WHERE d.endpoint_id = ? AND d.status = 'dead' AND d.completed_at >= ?
                  AND NOT EXISTS (SELECT 1 FROM deliveries r WHERE r.replay_of = d.id)
                ON CONFLICT DO NOTHING
                """,
                endpointId, Timestamp.from(since));
    }
}
