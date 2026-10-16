package com.anirudhpundhir.webhooks.delivery;

import com.anirudhpundhir.webhooks.delivery.DeliveryRepository.Delivery;
import com.anirudhpundhir.webhooks.delivery.DeliveryRepository.Replay;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/v1")
public class DeliveryController {
    private static final Set<String> STATUSES = Set.of("pending", "in_flight", "succeeded", "dead");
    private final DeliveryRepository repo;

    public DeliveryController(DeliveryRepository repo) {
        this.repo = repo;
    }

    @GetMapping("/deliveries/{id}")
    public Delivery get(@PathVariable UUID id) {
        return repo.find(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    @GetMapping("/deliveries")
    public List<Delivery> list(
            @RequestParam(required = false) UUID endpointId,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "50") int limit) {
        if (status != null && !STATUSES.contains(status)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "unknown status");
        }
        return repo.list(endpointId, status, Math.clamp(limit, 1, 500));
    }

    /** Replays one finished delivery. 202 with the new delivery id; idempotent. */
    @PostMapping("/deliveries/{id}/replay")
    public ResponseEntity<Map<String, Object>> replay(@PathVariable UUID id) {
        Replay r = repo.replay(id);
        return switch (r.outcome()) {
            case CREATED -> ResponseEntity.accepted().body(Map.of("delivery_id", r.newDeliveryId()));
            case ALREADY_REPLAYED -> ResponseEntity.ok(Map.of("delivery_id", r.newDeliveryId(), "already_replayed", true));
            case NOT_FOUND -> throw new ResponseStatusException(HttpStatus.NOT_FOUND);
            case NOT_FINISHED -> throw new ResponseStatusException(HttpStatus.CONFLICT, "delivery is still pending or in flight");
        };
    }

    /** Replays all dead-lettered deliveries for an endpoint (e.g. after fixing an outage). */
    @PostMapping("/endpoints/{id}/replay-dead")
    public ResponseEntity<Map<String, Object>> replayDead(
            @PathVariable UUID id, @RequestParam(required = false) Instant since) {
        int n = repo.replayDead(id, since == null ? Instant.EPOCH : since);
        return ResponseEntity.accepted().body(Map.of("replayed", n));
    }
}
