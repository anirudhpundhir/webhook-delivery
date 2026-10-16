package com.anirudhpundhir.webhooks.delivery;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongUnaryOperator;

/**
 * Exponential backoff with "equal jitter": the n-th retry waits
 * {@code d/2 + rand[0, d/2]} where {@code d = min(cap, base * 2^(n-1))}.
 *
 * <p>Jitter de-synchronises retries from many deliveries that failed together
 * (an endpoint outage), so recovery isn't met with a thundering herd. Equal
 * jitter, unlike full jitter, keeps a guaranteed minimum of {@code d/2}, so a
 * struggling endpoint always gets breathing room.
 */
public final class Backoff {
    private final Duration base;
    private final Duration cap;
    private final LongUnaryOperator random;

    public Backoff(Duration base, Duration cap) {
        this(base, cap, bound -> ThreadLocalRandom.current().nextLong(bound + 1));
    }

    /** {@code random.applyAsLong(bound)} must return a value in {@code [0, bound]}. */
    Backoff(Duration base, Duration cap, LongUnaryOperator random) {
        if (base.isNegative() || base.isZero() || cap.compareTo(base) < 0) {
            throw new IllegalArgumentException("need 0 < base <= cap");
        }
        this.base = base;
        this.cap = cap;
        this.random = random;
    }

    /** The un-jittered ceiling for retry number {@code attempt} (1-based). */
    public Duration ceiling(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        int shift = Math.min(attempt - 1, 62);
        long baseMs = base.toMillis();
        // Shifting a set bit into the sign bit would overflow.
        long ms = shift >= Long.numberOfLeadingZeros(baseMs) ? Long.MAX_VALUE : baseMs << shift;
        return Duration.ofMillis(Math.min(ms, cap.toMillis()));
    }

    /** Delay before retry number {@code attempt} (1-based). */
    public Duration delay(int attempt) {
        long d = ceiling(attempt).toMillis();
        long half = d / 2;
        return Duration.ofMillis(d - half + random.applyAsLong(half));
    }
}
