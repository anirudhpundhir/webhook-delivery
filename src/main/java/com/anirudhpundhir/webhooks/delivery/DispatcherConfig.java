package com.anirudhpundhir.webhooks.delivery;

import com.anirudhpundhir.webhooks.config.WebhookProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class DispatcherConfig {
    /** Disabled with {@code webhooks.dispatcher-enabled=false} (API-only nodes, tests). */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnProperty(name = "webhooks.dispatcher-enabled", havingValue = "true", matchIfMissing = true)
    Dispatcher dispatcher(DeliveryRepository repo, HttpSender sender, WebhookProperties props) {
        return new Dispatcher(repo, sender::send, props);
    }
}
