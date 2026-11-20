package com.ticketflow.messaging.dlt;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeadLetterAuditListenerTest {

    @Test
    @DisplayName("감싼 예외 여러 겹 중 가장 깊은 원인의 클래스와 메시지 첫 줄을 찾는다")
    void findsDeepestCause() {
        String stackTrace = """
                org.springframework.kafka.listener.ListenerExecutionFailedException: Listener method threw exception
                \tat org.springframework.kafka.listener.KafkaMessageListenerContainer.invoke(KafkaMessageListenerContainer.java:1)
                Caused by: org.springframework.kafka.listener.BatchListenerFailedException: Invalid message at index 1 @-1
                \tat com.ticketflow.messaging.consumer.ReservationRequestedBatchListener.onBatch(ReservationRequestedBatchListener.java:1)
                Caused by: com.ticketflow.messaging.consumer.NonRetryableMessageException: Unsupported schemaVersion 99
                \tat com.ticketflow.messaging.consumer.ReservationMessageParser.parse(ReservationMessageParser.java:1)
                \t... 12 more
                """;

        assertThat(DeadLetterAuditListener.rootCause(stackTrace)).containsExactly(
                "com.ticketflow.messaging.consumer.NonRetryableMessageException", "Unsupported schemaVersion 99");
    }

    @Test
    @DisplayName("Caused by가 없거나 스택 트레이스가 없으면 null")
    void noCause() {
        assertThat(DeadLetterAuditListener.rootCause("java.lang.IllegalStateException: boom\n\tat x.y(Z.java:1)")).isNull();
        assertThat(DeadLetterAuditListener.rootCause(null)).isNull();
    }
}
