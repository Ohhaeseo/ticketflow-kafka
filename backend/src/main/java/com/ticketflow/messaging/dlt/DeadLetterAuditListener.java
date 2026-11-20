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
        // 배치 리스너에서는 예외가 ListenerExecutionFailedException → BatchListenerFailedException → 실제 원인으로 감싸진다.
        // 헤더의 cause는 바로 아래 한 단계뿐이라, 스택 트레이스의 가장 깊은 "Caused by:"로 근본 원인을 기록한다.
        String[] root = rootCause(text(record, KafkaHeaders.DLT_EXCEPTION_STACKTRACE, null));
        DeadLetter deadLetter = new DeadLetter(
                text(record, KafkaHeaders.DLT_ORIGINAL_TOPIC, record.topic()),
                intHeader(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                longHeader(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                record.key(),
                record.value(),
                text(record, KafkaHeaders.DLT_EXCEPTION_FQCN, null),
                root != null ? root[0] : text(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN, null),
                root != null && root[1] != null ? root[1] : text(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE, null),
                record.partition(),
                record.offset());
        repository.record(deadLetter, clock.instant().truncatedTo(ChronoUnit.MICROS));
        log.warn("Dead letter recorded: originalTopic={} partition={} offset={} cause={} message={}",
                deadLetter.originalTopic(), deadLetter.originalPartition(), deadLetter.originalOffset(),
                deadLetter.exceptionCauseClass(), deadLetter.exceptionMessage());
    }

    /**
     * 스택 트레이스 문자열에서 가장 깊은 {@code Caused by: <클래스>: <메시지>}를 찾는다.
     *
     * @return {클래스, 메시지 첫 줄} 또는 Caused by가 없으면 null
     */
    static String[] rootCause(String stackTrace) {
        if (stackTrace == null) {
            return null;
        }
        String deepest = null;
        for (String line : stackTrace.split("\\R")) {
            String trimmed = line.strip();
            if (trimmed.startsWith("Caused by: ")) {
                deepest = trimmed.substring("Caused by: ".length());
            }
        }
        if (deepest == null) {
            return null;
        }
        int separator = deepest.indexOf(": ");
        return separator < 0
                ? new String[] {deepest, null}
                : new String[] {deepest.substring(0, separator), deepest.substring(separator + 2)};
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
