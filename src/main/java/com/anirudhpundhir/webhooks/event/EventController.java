package com.anirudhpundhir.webhooks.event;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/v1/events")
public class EventController {
    private final EventPublisher publisher;

    public EventController(EventPublisher publisher) {
        this.publisher = publisher;
    }

    public record CreateEvent(@NotBlank @Pattern(regexp = "[a-z0-9_.]{1,100}") String type, @NotNull JsonNode data) {}

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    @Transactional
    public EventPublisher.Published create(@Valid @RequestBody CreateEvent req) {
        if (!req.data().isObject()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "data must be a JSON object");
        }
        return publisher.publish(req.type(), req.data());
    }
}
