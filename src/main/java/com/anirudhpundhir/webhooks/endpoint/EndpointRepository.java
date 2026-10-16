package com.anirudhpundhir.webhooks.endpoint;

import java.net.URI;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class EndpointRepository {
    private final JdbcTemplate jdbc;

    public EndpointRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Endpoint insert(URI url, String secret, List<String> eventTypes, int maxConcurrency) {
        UUID id = UUID.randomUUID();
        return jdbc.queryForObject(
                """
                INSERT INTO endpoints (id, url, secret, event_types, max_concurrency)
                VALUES (?, ?, ?, ?, ?)
                RETURNING *
                """,
                EndpointRepository::map,
                id,
                url.toString(),
                secret,
                eventTypes.toArray(String[]::new),
                maxConcurrency);
    }

    public Optional<Endpoint> find(UUID id) {
        return jdbc.query("SELECT * FROM endpoints WHERE id = ?", EndpointRepository::map, id).stream().findFirst();
    }

    public boolean update(UUID id, Integer maxConcurrency, Boolean enabled) {
        return jdbc.update(
                        """
                        UPDATE endpoints SET max_concurrency = COALESCE(?, max_concurrency),
                                             enabled = COALESCE(?, enabled)
                        WHERE id = ?
                        """,
                        maxConcurrency,
                        enabled,
                        id)
                == 1;
    }

    static Endpoint map(ResultSet rs, int row) throws SQLException {
        Array types = rs.getArray("event_types");
        return new Endpoint(
                rs.getObject("id", UUID.class),
                URI.create(rs.getString("url")),
                rs.getString("secret"),
                Arrays.asList((String[]) types.getArray()),
                rs.getInt("max_concurrency"),
                rs.getBoolean("enabled"),
                rs.getTimestamp("created_at").toInstant());
    }
}
