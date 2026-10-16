package com.anirudhpundhir.webhooks.delivery;

import com.anirudhpundhir.webhooks.config.WebhookProperties;
import com.anirudhpundhir.webhooks.delivery.DeliveryRepository.AttemptResult;
import com.anirudhpundhir.webhooks.delivery.DeliveryRepository.Claimed;
import com.anirudhpundhir.webhooks.delivery.DeliveryRepository.Completion;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The read side of the outbox: polls for due deliveries, sends them on virtual
 * threads, and records outcomes. Any number of dispatchers (threads, pods) can
 * run against the same database; coordination happens entirely in Postgres.
 *
 * <p>A global semaphore bounds this instance's in-flight requests; per-endpoint
 * limits are enforced by {@link DeliveryRepository#claim}.
 */
public class Dispatcher implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    private final DeliveryRepository repo;
    private final Function<Claimed, AttemptResult> sender;
    private final WebhookProperties props;
    private final Backoff backoff;
    private final Semaphore permits;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore wake = new Semaphore(0);
    private final Thread poller;
    private volatile boolean running = true;

    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong retried = new AtomicLong();
    private final AtomicLong dead = new AtomicLong();
    private final AtomicLong leaseLost = new AtomicLong();

    public Dispatcher(DeliveryRepository repo, Function<Claimed, AttemptResult> sender, WebhookProperties props) {
        this.repo = repo;
        this.sender = sender;
        this.props = props;
        this.backoff = new Backoff(props.backoffBase(), props.backoffCap());
        this.permits = new Semaphore(props.maxInFlight());
        this.poller = Thread.ofPlatform().name("webhook-dispatcher").daemon().unstarted(this::pollLoop);
    }

    public Dispatcher start() {
        poller.start();
        return this;
    }

    private void pollLoop() {
        while (running) {
            int claimed = 0;
            try {
                claimed = pollOnce();
            } catch (Exception e) {
                log.warn("claim failed", e);
            }
            try {
                // Claim again immediately while there is work and capacity;
                // otherwise sleep until the interval passes or a slot frees up.
                if (claimed == 0 || permits.availablePermits() == 0) {
                    wake.tryAcquire(props.pollInterval().toMillis(), TimeUnit.MILLISECONDS);
                    wake.drainPermits();
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /** Claims and launches one batch. Returns the number claimed. Visible for tests. */
    int pollOnce() {
        int free = permits.availablePermits();
        int want = Math.min(free, props.claimBatchSize());
        if (want == 0) {
            return 0;
        }
        List<Claimed> batch = repo.claim(want, props.lease());
        for (Claimed c : batch) {
            permits.acquireUninterruptibly();
            workers.execute(() -> {
                try {
                    deliver(c);
                } finally {
                    permits.release();
                    wake.release();
                }
            });
        }
        return batch.size();
    }

    private void deliver(Claimed c) {
        AttemptResult r = sender.apply(c);
        int attempt = c.attempts() + 1;
        Completion outcome;
        try {
            outcome = repo.complete(c, r, props.maxAttempts(), backoff.delay(attempt));
        } catch (Exception e) {
            // The lease will expire and the delivery will be retried: at-least-once.
            log.warn("could not record outcome for delivery {}", c.id(), e);
            return;
        }
        switch (outcome) {
            case SUCCEEDED -> succeeded.incrementAndGet();
            case RETRY_SCHEDULED -> retried.incrementAndGet();
            case DEAD -> {
                dead.incrementAndGet();
                log.info("delivery {} dead-lettered after {} attempts: {}", c.id(), attempt, r.error());
            }
            case LEASE_LOST -> {
                leaseLost.incrementAndGet();
                log.warn("lease lost for delivery {}; outcome discarded", c.id());
            }
        }
    }

    public record Stats(long succeeded, long retried, long dead, long leaseLost) {}

    public Stats stats() {
        return new Stats(succeeded.get(), retried.get(), dead.get(), leaseLost.get());
    }

    /** Stops claiming and waits up to {@code request-timeout} for in-flight attempts to finish. */
    @Override
    public void close() {
        running = false;
        poller.interrupt();
        workers.shutdown();
        try {
            if (!workers.awaitTermination(props.requestTimeout().toMillis() + 1000, TimeUnit.MILLISECONDS)) {
                workers.shutdownNow(); // remaining leases expire and are retried elsewhere
            }
            poller.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
