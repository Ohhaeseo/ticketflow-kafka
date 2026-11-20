package com.ticketflow.messaging.config;

import com.ticketflow.messaging.consumer.NonRetryableMessageException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

/**
 * Consumer 오류 처리 (PRD FR-CSM-02·03).
 * <ul>
 *   <li>일시 오류(DB 연결, 잠금 타임아웃 등): 같은 파티션 안에서 200ms → 400ms → 800ms 간격으로 3번 재시도 후 DLT.
 *       재시도하는 동안 뒤 메시지를 기다리게 하므로(blocking) 파티션 안의 순서가 유지된다.
 *       ({@code @RetryableTopic}의 non-blocking 재시도는 순서를 보존하지 않아 쓰지 않는다. PRD §10.3)</li>
 *   <li>영구 오류(JSON 파싱 실패, 스키마 버전 불일치, 대상 예약 없음): 재시도 없이 바로 DLT.</li>
 *   <li>SOLD_OUT은 오류가 아니므로 이 경로를 타지 않는다.</li>
 * </ul>
 * Spring Boot는 {@code CommonErrorHandler} 빈이 하나 있으면 기본 리스너 컨테이너 팩토리에 연결한다.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaErrorHandlingConfig {

    public static final int MAX_RETRIES = 3;

    @Bean
    public DefaultErrorHandler reservationErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                                       TicketFlowKafkaProperties properties) {
        // 원본과 같은 파티션 번호의 DLT 파티션으로 보낸다 (DLT도 파티션 3개)
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(properties.reservationRequestedDltTopic(), record.partition()));

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(MAX_RETRIES);
        backOff.setInitialInterval(200);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(2_000);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(NonRetryableMessageException.class, JacksonException.class);
        // 배치 리스너는 재시도·복구 때마다 "Seek to current after exception"을 던져 위치를 되돌린다.
        // 정상 동작이므로 ERROR가 아닌 WARN으로 남긴다 (실패 원인은 DLT 감사 이력에 남는다).
        handler.setLogLevel(KafkaException.Level.WARN);
        return handler;
    }

    /**
     * DLT 감사 리스너 전용 팩토리. 감사 기록에 실패해도 DLT로 다시 보내지 않고(무한 루프 방지) 로그만 남긴다.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> dltAuditListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1_000L, 3L)));
        return factory;
    }
}
