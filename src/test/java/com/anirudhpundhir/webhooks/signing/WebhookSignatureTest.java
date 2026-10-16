package com.anirudhpundhir.webhooks.signing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.anirudhpundhir.webhooks.signing.WebhookSignature.VerificationException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class WebhookSignatureTest {
    static final byte[] BODY = "{\"id\":\"evt_1\",\"data\":{\"amount\":100}}".getBytes(StandardCharsets.UTF_8);
    static final long T = 1_700_000_000L;
    static final Clock AT_T = Clock.fixed(Instant.ofEpochSecond(T), ZoneOffset.UTC);
    static final Duration TOL = WebhookSignature.DEFAULT_TOLERANCE;

    @Test
    void knownAnswerVector() {
        // Independently computed: printf '1700000000.{"id":"evt_1","data":{"amount":100}}' | openssl dgst -sha256 -hmac whsec_test
        assertThat(WebhookSignature.sign("whsec_test", T, BODY))
                .isEqualTo("t=1700000000,v1=" + KnownAnswer.HEX);
    }

    @Test
    void roundTrip() {
        String h = WebhookSignature.sign("whsec_a", T, BODY);
        assertThatNoException().isThrownBy(() -> WebhookSignature.verify(h, BODY, List.of("whsec_a"), TOL, AT_T));
    }

    @Test
    void rejectsTamperedBodyWrongSecretAndTimestampSwap() {
        String h = WebhookSignature.sign("whsec_a", T, BODY);
        byte[] tampered = "{\"id\":\"evt_1\",\"data\":{\"amount\":999}}".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> WebhookSignature.verify(h, tampered, List.of("whsec_a"), TOL, AT_T))
                .isInstanceOf(VerificationException.class).hasMessageContaining("no matching");
        assertThatThrownBy(() -> WebhookSignature.verify(h, BODY, List.of("whsec_b"), TOL, AT_T))
                .isInstanceOf(VerificationException.class);
        // Attacker re-stamps an old signature with a fresh timestamp: MAC covers t.
        String swapped = h.replace("t=" + T, "t=" + (T + 1));
        assertThatThrownBy(() -> WebhookSignature.verify(swapped, BODY, List.of("whsec_a"), TOL,
                        Clock.offset(AT_T, Duration.ofSeconds(1))))
                .isInstanceOf(VerificationException.class);
    }

    @Test
    void enforcesToleranceBothDirections() {
        String h = WebhookSignature.sign("whsec_a", T, BODY);
        assertThatThrownBy(() -> WebhookSignature.verify(h, BODY, List.of("whsec_a"), TOL,
                        Clock.offset(AT_T, Duration.ofMinutes(6))))
                .hasMessageContaining("tolerance");
        assertThatThrownBy(() -> WebhookSignature.verify(h, BODY, List.of("whsec_a"), TOL,
                        Clock.offset(AT_T, Duration.ofMinutes(-6))))
                .hasMessageContaining("tolerance");
        assertThatNoException().isThrownBy(() -> WebhookSignature.verify(h, BODY, List.of("whsec_a"), TOL,
                Clock.offset(AT_T, Duration.ofMinutes(5))));
    }

    @Test
    void supportsSecretRotationAndMultipleSignatures() {
        String oldSig = WebhookSignature.sign("whsec_old", T, BODY);
        String newSig = WebhookSignature.sign("whsec_new", T, BODY);
        String both = oldSig + ",v1=" + newSig.substring(newSig.indexOf("v1=") + 3);
        assertThatNoException().isThrownBy(() -> WebhookSignature.verify(both, BODY, List.of("whsec_new"), TOL, AT_T));
        assertThatNoException().isThrownBy(() -> WebhookSignature.verify(oldSig, BODY, List.of("whsec_new", "whsec_old"), TOL, AT_T));
    }

    @Test
    void rejectsMalformedHeaders() {
        for (String h : new String[] {null, "", "garbage", "t=abc,v1=00", "v1=00", "t=1700000000", "t=1700000000,v0=00"}) {
            assertThatThrownBy(() -> WebhookSignature.verify(h, BODY, List.of("s"), TOL, AT_T))
                    .as(String.valueOf(h)).isInstanceOf(VerificationException.class);
        }
    }

    @Test
    void secretsAreRandomAndPrefixed() {
        String a = WebhookSignature.newSecret();
        assertThat(a).startsWith("whsec_").hasSize(6 + 43).isNotEqualTo(WebhookSignature.newSecret());
    }
}
