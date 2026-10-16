package com.anirudhpundhir.webhooks.endpoint;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record Endpoint(
        UUID id, URI url, String secret, List<String> eventTypes, int maxConcurrency, boolean enabled, Instant createdAt) {}
