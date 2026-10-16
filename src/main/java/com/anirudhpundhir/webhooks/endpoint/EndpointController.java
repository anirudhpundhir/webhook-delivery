package com.anirudhpundhir.webhooks.endpoint;

import com.anirudhpundhir.webhooks.signing.WebhookSignature;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/v1/endpoints")
public class EndpointController {
    private final EndpointRepository endpoints;

    public EndpointController(EndpointRepository endpoints) {
        this.endpoints = endpoints;
    }

    public record CreateEndpoint(
            @NotBlank String url, List<String> eventTypes, @Min(1) @Max(1000) Integer maxConcurrency) {}

    public record UpdateEndpoint(@Min(1) @Max(1000) Integer maxConcurrency, Boolean enabled) {}

    /** The secret is returned in full only on creation. */
    public record EndpointView(
            UUID id, String url, String secret, List<String> eventTypes, int maxConcurrency, boolean enabled, Instant createdAt) {
        static EndpointView of(Endpoint e, boolean revealSecret) {
            String s = revealSecret ? e.secret() : e.secret().substring(0, 10) + "…";
            return new EndpointView(e.id(), e.url().toString(), s, e.eventTypes(), e.maxConcurrency(), e.enabled(), e.createdAt());
        }
    }

    @PostMapping
    public ResponseEntity<EndpointView> create(@Valid @RequestBody CreateEndpoint req) {
        URI url;
        try {
            url = URI.create(req.url());
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "invalid url");
        }
        if (!("https".equals(url.getScheme()) || "http".equals(url.getScheme())) || url.getHost() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "url must be absolute http(s)");
        }
        Endpoint e = endpoints.insert(
                url,
                WebhookSignature.newSecret(),
                req.eventTypes() == null ? List.of() : req.eventTypes(),
                req.maxConcurrency() == null ? 5 : req.maxConcurrency());
        return ResponseEntity.status(HttpStatus.CREATED).body(EndpointView.of(e, true));
    }

    @GetMapping("/{id}")
    public EndpointView get(@PathVariable UUID id) {
        return endpoints.find(id).map(e -> EndpointView.of(e, false))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    @PatchMapping("/{id}")
    public EndpointView update(@PathVariable UUID id, @Valid @RequestBody UpdateEndpoint req) {
        if (!endpoints.update(id, req.maxConcurrency(), req.enabled())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        return get(id);
    }
}
