package com.ticketflow.messaging;

import com.ticketflow.event.service.EventService;
import com.ticketflow.messaging.consumer.ReservationRequestedListener;
import com.ticketflow.support.Await;
import com.ticketflow.support.IntegrationTest;
import com.ticketflow.support.StockInvariant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.Duration;
import java.util.UUID;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static com.ticketflow.support.ReservationRequests.reserve;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * MSG-02 Consumer 중단 (PRD §12.1): Consumer가 멈춘 동안 접수된 예약은 Kafka에 남아 있다가
 * 재시작 후 유실·중복 없이 처리된다. 프로세스 강제 종료 실험은 experiments/consumer-crash.md 참고.
 */
@IntegrationTest
class ConsumerRestartIntegrationTest {

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Autowired
    WebApplicationContext context;

    @Autowired
    EventService eventService;

    @Autowired
    JdbcTemplate jdbcTemplate;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    @DisplayName("MSG-02 Consumer를 멈춘 사이 접수된 8건이 재시작 후 처리된다 (티켓 5장 → 확정 5, 매진 3)")
    void reservationsAcceptedWhileConsumerIsDownAreProcessedAfterRestart() throws Exception {
        long eventId = openEvent(eventService, 5);
        MessageListenerContainer container = registry.getListenerContainer(ReservationRequestedListener.LISTENER_ID);
        assertThat(container).isNotNull();

        container.stop();
        try {
            for (long userId = 951; userId <= 958; userId++) {
                mockMvc.perform(reserve(userId, "restart-" + UUID.randomUUID(), eventId))
                        .andExpect(status().isAccepted());
            }
            Await.until("outbox published while consumer is down", Duration.ofSeconds(15), () ->
                    unpublished(eventId) == 0);
            Thread.sleep(1_000);

            assertThat(StockInvariant.snapshot(jdbcTemplate, eventId).queued())
                    .as("nothing processed while the consumer is stopped").isEqualTo(8);
        } finally {
            container.start();
        }

        Await.until("all processed after restart", Duration.ofSeconds(30), () ->
                StockInvariant.snapshot(jdbcTemplate, eventId).queued() == 0);
        StockInvariant.Snapshot snapshot = StockInvariant.assertHolds(jdbcTemplate, eventId);
        assertThat(snapshot.confirmed()).isEqualTo(5);
        assertThat(snapshot.soldOut()).isEqualTo(3);
        assertThat(jdbcTemplate.queryForObject("""
                SELECT count(*) FROM processed_messages p JOIN reservations r ON r.id = p.reservation_id
                 WHERE r.event_id = ?
                """, Integer.class, eventId)).isEqualTo(8);
    }

    private long unpublished(long eventId) {
        return jdbcTemplate.queryForObject("""
                SELECT count(*) FROM outbox_events o JOIN reservations r ON r.id = o.reservation_id
                 WHERE r.event_id = ? AND o.published_at IS NULL
                """, Long.class, eventId);
    }
}
