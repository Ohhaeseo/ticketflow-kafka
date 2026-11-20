package com.ticketflow.outbox.relay;

import com.ticketflow.messaging.config.TicketFlowKafkaProperties;
import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.outbox.entity.OutboxEvent;
import com.ticketflow.outbox.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 미발행 Outbox 행을 Kafka로 발행하고, 브로커 ack를 받은 행만 {@code published_at}을 채운다 (PRD FR-OBX-02).
 * <p>
 * 발행과 {@code published_at} 갱신 사이에 프로세스가 죽으면 다음 실행에서 같은 {@code message_id}로 다시 발행한다.
 * 이것은 정상 시나리오이며 Consumer가 멱등하게 처리한다 (FR-OBX-03, MSG-03).
 * MVP는 Relay 인스턴스 1개를 가정한다. 여러 개를 띄우면 {@code FOR UPDATE SKIP LOCKED}가 필요하다.
 */
@Component
@ConditionalOnBooleanProperty(name = "ticketflow.outbox.relay.enabled", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TicketFlowKafkaProperties kafkaProperties;
    private final OutboxRelayProperties properties;
    private final Clock clock;

    public OutboxRelay(OutboxEventRepository outboxEventRepository,
                       KafkaTemplate<String, String> kafkaTemplate,
                       TicketFlowKafkaProperties kafkaProperties,
                       OutboxRelayProperties properties,
                       Clock clock) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${ticketflow.outbox.relay.fixed-delay-ms}")
    public void relayScheduled() {
        try {
            relayOnce();
        } catch (RuntimeException e) {
            log.warn("Outbox relay run failed; will retry on next run", e);
        }
    }

    /**
     * 미발행 행을 최대 batchSize건 발행한다.
     *
     * @return 이번 실행에서 published_at을 채운 건수
     */
    public int relayOnce() {
        List<OutboxEvent> batch = outboxEventRepository.findUnpublished(properties.batchSize());
        if (batch.isEmpty()) {
            return 0;
        }

        List<CompletableFuture<SendResult<String, String>>> sends = new ArrayList<>(batch.size());
        for (OutboxEvent event : batch) {
            sends.add(kafkaTemplate.send(topicFor(event), event.messageKey(), event.payload()));
        }

        List<Long> publishedIds = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            OutboxEvent event = batch.get(i);
            try {
                sends.get(i).get(properties.sendTimeoutMs(), TimeUnit.MILLISECONDS);
                publishedIds.add(event.id());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException | TimeoutException e) {
                // published_at을 채우지 않으므로 다음 실행에서 다시 발행한다
                log.warn("Failed to publish outbox event id={} messageId={}", event.id(), event.messageId(), e);
            }
        }

        int marked = outboxEventRepository.markPublished(publishedIds, clock.instant().truncatedTo(ChronoUnit.MICROS));
        log.debug("Outbox relay published {}/{} events", marked, batch.size());
        return marked;
    }

    private String topicFor(OutboxEvent event) {
        if (ReservationRequestedMessage.EVENT_TYPE.equals(event.eventType())) {
            return kafkaProperties.reservationRequestedTopic();
        }
        throw new IllegalStateException("Unknown outbox event type: " + event.eventType());
    }
}
