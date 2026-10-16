package com.anirudhpundhir.webhooks;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class WebhookDeliveryApplication {
    public static void main(String[] args) {
        SpringApplication.run(WebhookDeliveryApplication.class, args);
    }
}
