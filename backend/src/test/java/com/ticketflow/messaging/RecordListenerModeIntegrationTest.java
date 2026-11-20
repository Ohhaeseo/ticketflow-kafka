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
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.test.context.TestPropertySource;
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
 * 비교용 레코드 리스너 모드({@code ticketflow.consumer.batch-enabled=false})가 계속 동작하는지 확인한다.
 * 별도 Spring 컨텍스트(와 컨테이너)로 실행된다.
 */
@IntegrationTest
@TestPropertySource(properties = "ticketflow.consumer.batch-enabled=false")
class RecordListenerModeIntegrationTest {

    @Autowired
    WebApplicationContext context;

    @Autowired
    KafkaListenerEndpointRegistry registry;

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
    @DisplayName("레코드 모드: ack-mode record로 동작하고, 티켓 2장·3명 → 확정 2, 매진 1")
    void recordModeProcessesReservations() throws Exception {
        assertThat(registry.getListenerContainer(ReservationRequestedListener.LISTENER_ID)
                .getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.RECORD);

        long eventId = openEvent(eventService, 2);
        for (long userId = 931; userId <= 933; userId++) {
            mockMvc.perform(reserve(userId, "record-mode-" + UUID.randomUUID(), eventId)).andExpect(status().isAccepted());
        }

        Await.until("processed in record mode", Duration.ofSeconds(30), () ->
                StockInvariant.snapshot(jdbcTemplate, eventId).queued() == 0);
        StockInvariant.Snapshot snapshot = StockInvariant.assertHolds(jdbcTemplate, eventId);
        assertThat(snapshot.confirmed()).isEqualTo(2);
        assertThat(snapshot.soldOut()).isEqualTo(1);
    }
}
