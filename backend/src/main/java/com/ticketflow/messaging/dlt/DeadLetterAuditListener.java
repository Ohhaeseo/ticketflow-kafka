package com.ticketflow.messaging.dlt;

import com.ticketflow.messaging.dlt.DeadLetterMessageRepository.DeadLetter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.temporal.ChronoUnit;

/**
 * DLT에 들어온 메시지를 dead_letter_messages에 기록한다 (PRD MSG-04 "감사 이력 유지").
 * DeadLetterPublishingRecoverer가 붙인 헤더(원본 토픽·파티션·오프셋, 예외 클래스·메시지)를 그대로 옮긴다.
 */
@Component
public class DeadLetterAuditListener {

    public static final String LISTENER_ID = "reservation-requested-dlt-audit";

    private static final Logger log = LoggerFactory.getLogger(DeadLetterAuditListener.class);

    private final DeadLetterMessageRepository repository;
    private final Clock clock;

    public DeadLetterAuditListener(DeadLetterMessageRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    @KafkaListener(id = LISTENER_ID,
            topics = "${ticketflow.kafka.reservation-requested-dlt-topic}",
            groupId = "ticketflow-dlt-audit",
            containerFactory = "dltAuditListenerContainerFactory")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        DeadLetter deadLetter = new DeadLetter(
                text(record, KafkaHeaders.DLT_ORIGINAL_TOPIC, record.topic()),
                intHeader(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                longHeader(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                record.key(),
                record.value(),
                text(record, KafkaHeaders.DLT_EXCEPTION_FQCN, null),
                text(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN, null),
                text(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE, null),
                record.partition(),
                record.offset());
        repository.record(deadLetter, clock.instant().truncatedTo(ChronoUnit.MICROS));
        log.warn("Dead letter recorded: originalTopic={} partition={} offset={} cause={} message={}",
                deadLetter.originalTopic(), deadLetter.originalPartition(), deadLetter.originalOffset(),
                deadLetter.exceptionCauseClass(), deadLetter.exceptionMessage());
    }

    private static String text(ConsumerRecord<?, ?> record, String name, String fallback) {
        Header header = record.headers().lastHeader(name);
        return header == null ? fallback : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static int intHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? -1 : ByteBuffer.wrap(header.value()).getInt();
    }

    private static long longHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? -1L : ByteBuffer.wrap(header.value()).getLong();
    }
}
