package com.ticketflow.reservation;

import com.jayway.jsonpath.JsonPath;
import com.ticketflow.event.service.EventService;
import com.ticketflow.support.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static com.ticketflow.support.ReservationRequests.ROLE_HEADER;
import static com.ticketflow.support.ReservationRequests.USER_HEADER;
import static com.ticketflow.support.ReservationRequests.createEvent;
import static com.ticketflow.support.ReservationRequests.getReservation;
import static com.ticketflow.support.ReservationRequests.openEvent;
import static com.ticketflow.support.ReservationRequests.reserve;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@IntegrationTest
class ReservationApiIntegrationTest {

    @Autowired
    WebApplicationContext context;

    @Autowired
    EventService eventService;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    @DisplayName("최초 접수는 202 QUEUED와 상태 조회 주소를 반환한다")
    void firstRequestIsAccepted() throws Exception {
        long eventId = openEvent(eventService, 100);

        String body = mockMvc.perform(reserve(42, key(), eventId))
                .andExpect(status().isAccepted())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.eventId").value(eventId))
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andReturn().getResponse().getContentAsString();

        Integer reservationId = JsonPath.read(body, "$.reservationId");
        String createdAt = JsonPath.read(body, "$.createdAt");
        // Consumer가 백그라운드에서 돌기 때문에 조회 시점에 이미 확정됐을 수 있다
        mockMvc.perform(getReservation(42, reservationId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", anyOf(is("QUEUED"), is("CONFIRMED"))))
                .andExpect(jsonPath("$.statusUrl").value("/api/v1/reservations/" + reservationId))
                // 202 응답과 DB에서 다시 읽은 값의 시각 정밀도가 같아야 한다
                .andExpect(jsonPath("$.createdAt").value(createdAt));
    }

    @Test
    @DisplayName("같은 키·같은 본문으로 다시 보내면 200과 같은 예약을 반환한다 (Q1)")
    void replayReturnsExistingReservation() throws Exception {
        long eventId = openEvent(eventService, 100);
        String key = key();

        String first = mockMvc.perform(reserve(43, key, eventId))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();
        Integer reservationId = JsonPath.read(first, "$.reservationId");

        mockMvc.perform(reserve(43, key, eventId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.reservationId").value(reservationId))
                .andExpect(jsonPath("$.status", anyOf(is("QUEUED"), is("CONFIRMED"))));
    }

    @Test
    @DisplayName("같은 키로 다른 공연을 요청하면 409 IDEMPOTENCY_KEY_REUSED")
    void sameKeyDifferentBodyIsConflict() throws Exception {
        long firstEvent = openEvent(eventService, 100);
        long secondEvent = openEvent(eventService, 100);
        String key = key();

        mockMvc.perform(reserve(44, key, firstEvent)).andExpect(status().isAccepted());

        mockMvc.perform(reserve(44, key, secondEvent))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("IDEMPOTENCY_KEY_REUSED"));
    }

    @Test
    @DisplayName("같은 공연에 다른 키로 다시 요청하면 409와 기존 reservationId (Q2)")
    void sameEventDifferentKeyIsConflict() throws Exception {
        long eventId = openEvent(eventService, 100);

        String first = mockMvc.perform(reserve(45, key(), eventId))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();
        Integer reservationId = JsonPath.read(first, "$.reservationId");

        mockMvc.perform(reserve(45, key(), eventId))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("RESERVATION_ALREADY_EXISTS"))
                .andExpect(jsonPath("$.reservationId").value(reservationId));
    }

    @Test
    @DisplayName("Idempotency-Key가 없으면 400, 형식이 틀리면 400 INVALID_IDEMPOTENCY_KEY")
    void idempotencyKeyIsRequired() throws Exception {
        long eventId = openEvent(eventService, 100);

        mockMvc.perform(post("/api/v1/reservations")
                        .header(USER_HEADER, 46)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventId\":" + eventId + "}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        mockMvc.perform(reserve(46, "has space", eventId))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_IDEMPOTENCY_KEY"));

        mockMvc.perform(reserve(46, "k".repeat(101), eventId))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_IDEMPOTENCY_KEY"));
    }

    @Test
    @DisplayName("eventId가 없으면 400과 필드 오류")
    void eventIdIsRequired() throws Exception {
        mockMvc.perform(post("/api/v1/reservations")
                        .header(USER_HEADER, 47)
                        .header("Idempotency-Key", key())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("eventId"));
    }

    @Test
    @DisplayName("사용자 헤더가 없거나 시드에 없는 사용자면 401 UNAUTHENTICATED")
    void unauthenticated() throws Exception {
        long eventId = openEvent(eventService, 100);

        mockMvc.perform(post("/api/v1/reservations")
                        .header("Idempotency-Key", key())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventId\":" + eventId + "}"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));

        mockMvc.perform(reserve(5_000, key(), eventId))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));

        mockMvc.perform(post("/api/v1/reservations")
                        .header(USER_HEADER, "abc")
                        .header("Idempotency-Key", key())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventId\":" + eventId + "}"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("UNAUTHENTICATED"));
    }

    @Test
    @DisplayName("없는 공연이면 404 EVENT_NOT_FOUND")
    void eventNotFound() throws Exception {
        mockMvc.perform(reserve(48, key(), 999_999))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("EVENT_NOT_FOUND"));
    }

    @Test
    @DisplayName("판매 기간 밖이면 422 SALES_NOT_STARTED / SALES_ENDED")
    void outsideSalesPeriod() throws Exception {
        Instant now = Instant.now();
        long upcoming = createEvent(eventService, 100, now.plus(Duration.ofDays(1)), now.plus(Duration.ofDays(2)));
        long ended = createEvent(eventService, 100, now.minus(Duration.ofDays(2)), now.minus(Duration.ofDays(1)));

        mockMvc.perform(reserve(49, key(), upcoming))
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.code").value("SALES_NOT_STARTED"));

        mockMvc.perform(reserve(49, key(), ended))
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.code").value("SALES_ENDED"));
    }

    @Test
    @DisplayName("다른 사용자의 예약은 404, 관리자는 조회할 수 있다")
    void reservationVisibility() throws Exception {
        long eventId = openEvent(eventService, 100);
        String body = mockMvc.perform(reserve(50, key(), eventId))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();
        Integer reservationId = JsonPath.read(body, "$.reservationId");

        mockMvc.perform(getReservation(51, reservationId))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("RESERVATION_NOT_FOUND"));

        mockMvc.perform(getReservation(51, reservationId).header(ROLE_HEADER, "ADMIN"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.reservationId").value(reservationId));
    }

    @Test
    @DisplayName("내 예약 목록에는 본인 예약만 최근 순으로 나온다")
    void myReservations() throws Exception {
        long firstEvent = openEvent(eventService, 100);
        long secondEvent = openEvent(eventService, 100);
        Integer first = JsonPath.read(mockMvc.perform(reserve(777, key(), firstEvent))
                .andReturn().getResponse().getContentAsString(), "$.reservationId");
        Integer second = JsonPath.read(mockMvc.perform(reserve(777, key(), secondEvent))
                .andReturn().getResponse().getContentAsString(), "$.reservationId");
        mockMvc.perform(reserve(778, key(), firstEvent)).andExpect(status().isAccepted());

        mockMvc.perform(get("/api/v1/users/me/reservations").header(USER_HEADER, 777))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].reservationId", containsInAnyOrder(second, first)))
                .andExpect(jsonPath("$[0].reservationId").value(second));
    }

    private static String key() {
        return "test-" + UUID.randomUUID();
    }
}
