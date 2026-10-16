package com.anirudhpundhir.webhooks.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Dispatcher tuning. With the defaults (16 attempts, 30s base, 6h cap) a
 * failing delivery is retried for 19-38 hours (depending on jitter) before
 * it is dead-lettered.
 */
@Validated
@ConfigurationProperties(prefix = "webhooks")
public record WebhookProperties(
        @DefaultValue("true") boolean dispatcherEnabled,
        @DefaultValue("200ms") Duration pollInterval,
        @DefaultValue("100") @Min(1) int claimBatchSize,
        @DefaultValue("256") @Min(1) int maxInFlight,
        @DefaultValue("30s") Duration lease,
        @DefaultValue("10s") Duration requestTimeout,
        @DefaultValue("3s") Duration connectTimeout,
        @DefaultValue("16") @Min(1) int maxAttempts,
        @DefaultValue("30s") Duration backoffBase,
        @DefaultValue("6h") Duration backoffCap) {

    /** A lease must outlive any single HTTP attempt, or a slow attempt could be re-claimed while still running. */
    @AssertTrue(message = "webhooks.lease must exceed webhooks.request-timeout + webhooks.connect-timeout")
    public boolean isLeaseLongerThanAttempt() {
        return lease.compareTo(requestTimeout.plus(connectTimeout)) > 0;
    }
}
