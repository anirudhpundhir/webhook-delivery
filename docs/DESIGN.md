# Design notes (ADR log)

Short Architecture Decision Records. Each has context, the decision, and its consequences.

---

## ADR-001: Transactional outbox on Postgres, with no broker

**Context.** Webhooks are emitted *because* of a business change, such as a payment succeeding. Writing the change to the database and then publishing to a queue is a dual write: a crash between the two steps loses the event or invents one.

**Decision.** `EventPublisher.publish` runs with `Propagation.MANDATORY` inside the caller's transaction. It inserts the event and fans it out into one `deliveries` row per subscribed, enabled endpoint (filtered by `event_types`). Everything commits or rolls back together (`outboxRowsRollBackWithTheBusinessTransaction`). Dispatchers poll `deliveries`. The body is serialised **once** at publish time and stored as text, so every attempt and replay sends identical bytes.

**Consequences.** Postgres is the queue. That's operationally simple and transactional, and it scales to thousands of deliveries per second (see README). It isn't the right tool for 10⁵/s; see the follow-ups (partitioning, or relaying the outbox to Kafka with CDC).

---

## ADR-002: Claiming with leases, per-endpoint limits enforced in the database

**Context.** Several dispatcher instances must (a) never send one delivery concurrently from two instances under normal operation, (b) recover deliveries from crashed instances, and (c) respect `max_concurrency` per endpoint *globally*. Otherwise a slow merchant endpoint gets hammered.

**Decision.** One claim transaction:
1. `SELECT … FROM endpoints WHERE <has due work> FOR UPDATE OF e SKIP LOCKED`. Claimers serialise per endpoint and skip endpoints another instance is already claiming for.
2. While holding the endpoint lock, count live leases: `status='in_flight' AND locked_until > now()`. Free slots = `max_concurrency − live`.
3. Claim at most that many due rows (`pending` and due, **or** `in_flight` with an expired lease) with `FOR UPDATE SKIP LOCKED`. Each one gets a fresh `lease_token` and `locked_until = now() + lease`.

All time comparisons use the database's `now()`, so dispatcher hosts don't need synchronised clocks. Config validation requires `lease > request-timeout + connect-timeout`, so a lease can't expire while a well-behaved attempt is still running.

**Consequences.** Verified by `perEndpointConcurrencyLimitHoldsAcrossDispatchers`: two instances, limit 3, a 150ms receiver. The observed max concurrency is ≤ 3, and a fast neighbour endpoint finishes before the slow one. Removing the slot count makes that test fail. `concurrentClaimsNeverDoubleClaim` runs 8 concurrent claimers over 200 rows with no duplicate claims. Limits are approximate only if an attempt outlives its lease, which the lease/timeout validation prevents for well-behaved attempts. Endpoints with work are visited in `random()` order, 64 per claim. That's fair on average, but it isn't weighted fair queuing.

---

## ADR-003: At-least-once with fenced completions

**Context.** HTTP has no exactly-once. A receiver can get the request, process it, and have the response lost.

**Decision.**
- Delivery is **at-least-once**. Receivers deduplicate on `Webhook-Id`, the event id. It's stable across retries and replays, while `Webhook-Delivery-Id` differs per replay.
- A completion is `UPDATE … WHERE id = ? AND lease_token = ? AND status = 'in_flight'`. If the lease expired and another instance re-claimed the delivery, the zombie's outcome is discarded (`LEASE_LOST`) and no attempt row is written (`expiredLeaseIsRedeliveredAndStaleCompletionIsDiscarded`). Without the fence, that test fails on a duplicate attempt.
- If recording the outcome fails (database blip), the dispatcher does nothing. The lease expires and the delivery is retried.

---

## ADR-004: Retry schedule: exponential backoff with equal jitter, then dead-letter

**Decision.** Every non-2xx response, timeout or connection error is retried. Redirects aren't followed, and a 3xx counts as a failure. The n-th retry waits `d/2 + U[0, d/2]` with `d = min(cap, base·2ⁿ⁻¹)`. With the defaults (30s base, 6h cap, 16 attempts), a delivery is retried for 19–38h and then marked `dead`, with `last_status_code` and `last_error` kept and every attempt in `delivery_attempts`.

Equal jitter rather than full jitter: full jitter (`U[0, d]`) can retry almost immediately even late in the schedule. Equal jitter guarantees a struggling endpoint at least `d/2` of quiet, while still de-synchronising a herd of deliveries that failed together.

**Consequences.** Unit tests pin the ceilings, overflow safety and jitter bounds. The integration test asserts the observed gaps between attempts respect the minimums.

---

## ADR-005: Signatures: Stripe-style `t=…,v1=…`

**Decision.** `Webhook-Signature: t=<unix>,v1=hex(HMAC-SHA256(secret, "<t>." + body))`. The signature is computed **at send time**, so a retry three days later still passes a receiver's 5-minute tolerance. The verifier, shipped in the same package for receivers and tests, checks the tolerance in both directions, compares in constant time (`MessageDigest.isEqual`), accepts multiple `v1` values, and accepts multiple secrets (rotation). Secrets are 32 random bytes (`whsec_…`). The full secret is returned only when the endpoint is created.

**Consequences.** The timestamp is inside the MAC, so re-stamping an old signature fails (tested). The known-answer test vector was produced with `openssl dgst`, not with the code under test.

---

## ADR-006: Replay creates new deliveries; originals are immutable

**Decision.** `POST /v1/deliveries/{id}/replay` is allowed for `succeeded` or `dead` deliveries. It inserts a new `pending` delivery with `replay_of = id` and keeps the original row and its attempt log. A unique index on `replay_of` makes replay idempotent: a repeated call returns the same replay id. `POST /v1/endpoints/{id}/replay-dead?since=` bulk-replays all not-yet-replayed dead letters. A delivery that's still pending or in flight gets `409`.

---

## Follow-ups / known scope cuts

- **SSRF protection**: endpoint URLs aren't checked against private or link-local ranges. That's required before accepting URLs from untrusted tenants. It would be a DNS-resolve-then-pin check in `HttpSender`.
- **Secret rotation API** (the verifier already supports multiple secrets, but the sender only signs with one).
- **Per-endpoint circuit breaking / auto-disable** after sustained failure (Stripe disables after days of failures and emails the owner).
- **Ordering**: there's no per-endpoint ordering guarantee, as with Stripe. Concurrency > 1 and retries reorder events.
- **Retention**: no TTL or partitioning for `deliveries`, `events` or `delivery_attempts`.
- **AuthN/Z** on the management API, plus multi-tenancy.
- Metrics (Micrometer counters exist only as in-process `Dispatcher.stats()`).
- No Dockerfile or Testcontainers. Docker was unavailable while building this, so tests use a real Postgres via `WEBHOOK_TEST_DATABASE_URL`, and CI provides it as a service container.
