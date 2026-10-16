package com.anirudhpundhir.webhooks.support;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

/**
 * An in-process webhook receiver. Each path is an independent "endpoint" with
 * a programmable response (by per-path request number), optional latency, and
 * concurrency tracking.
 */
public final class TestReceiver implements AutoCloseable {
    public record Received(String path, Map<String, List<String>> headers, byte[] body, long atNanos) {
        public String header(String name) {
            return headers.entrySet().stream()
                    .filter(e -> e.getKey().equalsIgnoreCase(name))
                    .map(e -> e.getValue().get(0))
                    .findFirst()
                    .orElse(null);
        }
    }

    private static final class PathState {
        volatile IntUnaryOperator status = n -> 200;
        volatile long latencyMs;
        final AtomicInteger requests = new AtomicInteger();
        final AtomicInteger current = new AtomicInteger();
        final AtomicInteger maxConcurrent = new AtomicInteger();
    }

    private final HttpServer server;
    private final Map<String, PathState> paths = new ConcurrentHashMap<>();
    public final List<Received> received = new CopyOnWriteArrayList<>();

    public TestReceiver() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 512);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/", ex -> {
            PathState st = state(ex.getRequestURI().getPath());
            int now = st.current.incrementAndGet();
            st.maxConcurrent.accumulateAndGet(now, Math::max);
            try {
                byte[] body = ex.getRequestBody().readAllBytes();
                int n = st.requests.incrementAndGet();
                received.add(new Received(ex.getRequestURI().getPath(), Map.copyOf(ex.getRequestHeaders()), body, System.nanoTime()));
                if (st.latencyMs > 0) {
                    Thread.sleep(st.latencyMs);
                }
                ex.sendResponseHeaders(st.status.applyAsInt(n), -1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                st.current.decrementAndGet();
                ex.close();
            }
        });
        server.start();
    }

    private PathState state(String path) {
        return paths.computeIfAbsent(path, p -> new PathState());
    }

    public URI url(String path) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
    }

    /** Status to return for the n-th request (1-based) to {@code path}. */
    public TestReceiver respond(String path, IntUnaryOperator statusByRequestNumber) {
        state(path).status = statusByRequestNumber;
        return this;
    }

    public TestReceiver latency(String path, long ms) {
        state(path).latencyMs = ms;
        return this;
    }

    public int requests(String path) {
        return state(path).requests.get();
    }

    public int maxConcurrent(String path) {
        return state(path).maxConcurrent.get();
    }

    public List<Received> receivedOn(String path) {
        return received.stream().filter(r -> r.path().equals(path)).toList();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
