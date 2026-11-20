package com.ticketflow.monitoring.service;

import com.ticketflow.messaging.config.TicketFlowKafkaProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 주기적으로 계산해 Micrometer gauge로 노출하는 운영 지표 (PRD FR-OBX-04, M6, M7).
 * Prometheus가 긁을 때마다 DB·Kafka를 조회하지 않도록 값을 미리 계산해 둔다.
 * <ul>
 *   <li>{@code ticketflow.outbox.unpublished}: 미발행 Outbox 건수 (Kafka 연결 장애 탐지)</li>
 *   <li>{@code ticketflow.outbox.oldest.unpublished.age}: 가장 오래 기다린 미발행 이벤트의 대기 시간(초)</li>
 *   <li>{@code ticketflow.consumer.lag}: 예약 Consumer group의 lag 합계 (계산 실패 시 -1)</li>
 *   <li>{@code ticketflow.dead.letters}: DLT 감사 이력 누적 건수</li>
 * </ul>
 */
@Component
public class OperationalMetrics {

    private static final Logger log = LoggerFactory.getLogger(OperationalMetrics.class);

    private final JdbcClient jdbcClient;
    private final ConsumerLagService consumerLagService;
    private final TicketFlowKafkaProperties kafkaProperties;
    private final String consumerGroupId;

    private final AtomicLong outboxUnpublished = new AtomicLong();
    private final AtomicLong outboxOldestAgeMillis = new AtomicLong();
    private final AtomicLong consumerLag = new AtomicLong(-1);
    private final AtomicLong deadLetters = new AtomicLong();

    public OperationalMetrics(JdbcTemplate jdbcTemplate, ConsumerLagService consumerLagService,
                              TicketFlowKafkaProperties kafkaProperties, MeterRegistry registry,
                              org.springframework.core.env.Environment environment) {
        this.jdbcClient = JdbcClient.create(jdbcTemplate);
        this.consumerLagService = consumerLagService;
        this.kafkaProperties = kafkaProperties;
        this.consumerGroupId = environment.getRequiredProperty("spring.kafka.consumer.group-id");

        Gauge.builder("ticketflow.outbox.unpublished", outboxUnpublished, AtomicLong::get)
                .description("미발행 Outbox 이벤트 수").register(registry);
        Gauge.builder("ticketflow.outbox.oldest.unpublished.age", outboxOldestAgeMillis, v -> v.get() / 1000.0)
                .baseUnit("seconds").description("가장 오래 기다린 미발행 이벤트의 대기 시간").register(registry);
        Gauge.builder("ticketflow.consumer.lag", consumerLag, AtomicLong::get)
                .description("예약 Consumer group lag 합계 (-1: 계산 실패)").register(registry);
        Gauge.builder("ticketflow.dead.letters", deadLetters, AtomicLong::get)
                .description("DLT 감사 이력 누적 건수").register(registry);
    }

    @Scheduled(fixedDelayString = "${ticketflow.monitoring.refresh-ms}", initialDelay = 1000)
    public void refresh() {
        try {
            jdbcClient.sql("""
                            SELECT count(*) AS unpublished,
                                   coalesce(extract(epoch FROM now() - min(created_at)) * 1000, 0)::bigint AS oldest_ms
                              FROM outbox_events WHERE published_at IS NULL
                            """)
                    .query((rs, rowNum) -> {
                        outboxUnpublished.set(rs.getLong("unpublished"));
                        outboxOldestAgeMillis.set(rs.getLong("oldest_ms"));
                        return null;
                    })
                    .list();
            deadLetters.set(jdbcClient.sql("SELECT count(*) FROM dead_letter_messages").query(Long.class).single());
        } catch (RuntimeException e) {
            log.warn("Failed to refresh DB metrics: {}", e.toString());
        }
        consumerLag.set(currentLag());
    }

    public long currentLag() {
        try {
            return consumerLagService.lagByPartition(consumerGroupId, kafkaProperties.reservationRequestedTopic())
                    .values().stream().mapToLong(Long::longValue).sum();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        } catch (Exception e) {
            log.debug("Failed to compute consumer lag: {}", e.toString());
            return -1;
        }
    }
}
