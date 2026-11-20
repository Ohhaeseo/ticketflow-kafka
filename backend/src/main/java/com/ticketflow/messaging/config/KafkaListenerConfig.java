package com.ticketflow.messaging.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

/**
 * 예약 Consumer 컨테이너 설정.
 * <p>
 * {@code ticketflow.consumer.batch-enabled=true}(기본): 배치 리스너. poll 묶음을 한 트랜잭션으로 처리하고 오프셋을 배치마다 커밋한다.
 * {@code false}: Spring Boot 기본 팩토리로 레코드 리스너를 쓴다 ({@code spring.kafka.listener.ack-mode}를 따른다).
 */
@Configuration(proxyBeanMethods = false)
public class KafkaListenerConfig {

    @Bean
    @ConditionalOnBooleanProperty(name = "ticketflow.consumer.batch-enabled", matchIfMissing = true)
    public ConcurrentKafkaListenerContainerFactory<String, String> reservationBatchListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler reservationErrorHandler,
            @Value("${spring.kafka.listener.concurrency:3}") int concurrency) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        // 배치 처리(=DB 커밋)가 끝난 뒤 배치의 마지막 오프셋을 한 번 커밋한다
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setCommonErrorHandler(reservationErrorHandler);
        return factory;
    }
}
