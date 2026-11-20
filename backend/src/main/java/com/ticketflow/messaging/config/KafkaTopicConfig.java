package com.ticketflow.messaging.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * 애플리케이션 기동 시 KafkaAdmin이 아래 토픽을 만든다 (이미 있으면 그대로 둔다).
 * 브로커는 auto.create.topics.enable=false로 운영하므로 토픽은 여기서만 정의한다.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicConfig {

    @Bean
    public NewTopic reservationRequestedTopic(TicketFlowKafkaProperties properties) {
        return TopicBuilder.name(properties.reservationRequestedTopic())
                .partitions(properties.partitions())
                .replicas(properties.replicationFactor())
                .build();
    }

    @Bean
    public NewTopic reservationRequestedDltTopic(TicketFlowKafkaProperties properties) {
        return TopicBuilder.name(properties.reservationRequestedDltTopic())
                .partitions(properties.partitions())
                .replicas(properties.replicationFactor())
                .build();
    }
}
