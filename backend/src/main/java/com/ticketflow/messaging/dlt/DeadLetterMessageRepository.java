package com.ticketflow.messaging.dlt;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Repository
public class DeadLetterMessageRepository {

    private final JdbcClient jdbcClient;

    public DeadLetterMessageRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcClient = JdbcClient.create(jdbcTemplate);
    }

    /** 같은 원본 레코드는 한 번만 기록한다. */
    public int record(DeadLetter deadLetter, Instant receivedAt) {
        return jdbcClient.sql("""
                        INSERT INTO dead_letter_messages
                            (original_topic, original_partition, original_offset, message_key, payload,
                             exception_class, exception_cause_class, exception_message,
                             dlt_partition, dlt_offset, received_at)
                        VALUES (:originalTopic, :originalPartition, :originalOffset, :messageKey, :payload,
                                :exceptionClass, :exceptionCauseClass, :exceptionMessage,
                                :dltPartition, :dltOffset, :receivedAt)
                        ON CONFLICT (original_topic, original_partition, original_offset) DO NOTHING
                        """)
                .param("originalTopic", deadLetter.originalTopic())
                .param("originalPartition", deadLetter.originalPartition())
                .param("originalOffset", deadLetter.originalOffset())
                .param("messageKey", deadLetter.messageKey())
                .param("payload", deadLetter.payload())
                .param("exceptionClass", deadLetter.exceptionClass())
                .param("exceptionCauseClass", deadLetter.exceptionCauseClass())
                .param("exceptionMessage", deadLetter.exceptionMessage())
                .param("dltPartition", deadLetter.dltPartition())
                .param("dltOffset", deadLetter.dltOffset())
                .param("receivedAt", OffsetDateTime.ofInstant(receivedAt, ZoneOffset.UTC))
                .update();
    }

    public record DeadLetter(
            String originalTopic,
            int originalPartition,
            long originalOffset,
            String messageKey,
            String payload,
            String exceptionClass,
            String exceptionCauseClass,
            String exceptionMessage,
            int dltPartition,
            long dltOffset
    ) {
    }
}
