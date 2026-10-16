package com.anirudhpundhir.webhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffTest {
    final Duration base = Duration.ofSeconds(30);
    final Duration cap = Duration.ofHours(6);

    @Test
    void ceilingDoublesThenCaps() {
        Backoff b = new Backoff(base, cap);
        assertThat(b.ceiling(1)).isEqualTo(Duration.ofSeconds(30));
        assertThat(b.ceiling(2)).isEqualTo(Duration.ofSeconds(60));
        assertThat(b.ceiling(5)).isEqualTo(Duration.ofSeconds(480));
        assertThat(b.ceiling(10)).isEqualTo(Duration.ofSeconds(30 * 512)); // 4h16m
        assertThat(b.ceiling(11)).isEqualTo(cap); // 8h32m capped to 6h
        assertThat(b.ceiling(64)).isEqualTo(cap);
        assertThat(b.ceiling(Integer.MAX_VALUE)).isEqualTo(cap); // no overflow
    }

    @Test
    void jitterStaysInUpperHalf() {
        Backoff lo = new Backoff(base, cap, bound -> 0);
        Backoff hi = new Backoff(base, cap, bound -> bound);
        for (int n = 1; n <= 20; n++) {
            long c = new Backoff(base, cap).ceiling(n).toMillis();
            assertThat(lo.delay(n).toMillis()).isEqualTo(c - c / 2);
            assertThat(hi.delay(n).toMillis()).isEqualTo(c);
        }
        Backoff real = new Backoff(base, cap);
        for (int i = 0; i < 10_000; i++) {
            long d = real.delay(3).toMillis();
            assertThat(d).isBetween(60_000L, 120_000L);
        }
    }

    @Test
    void jitterActuallySpreads() {
        Backoff real = new Backoff(base, cap);
        long distinct = java.util.stream.IntStream.range(0, 1000).mapToLong(i -> real.delay(4).toMillis()).distinct().count();
        assertThat(distinct).isGreaterThan(900);
    }

    @Test
    void validates() {
        assertThatThrownBy(() -> new Backoff(Duration.ZERO, cap)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(cap, base)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(base, cap).delay(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
