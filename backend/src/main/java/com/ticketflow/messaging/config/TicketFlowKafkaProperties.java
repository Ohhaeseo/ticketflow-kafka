package com.ticketflow.messaging.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Kafka 토픽 설정 (PRD §10.1). 값은 application.yml의 {@code ticketflow.kafka.*}.
 */
@ConfigurationProperties(prefix = "ticketflow.kafka")
public record TicketFlowKafkaProperties(
        String reservationRequestedTopic,
        String reservationRequestedDltTopic,
        int partitions,
        int replicationFactor
) {
}
