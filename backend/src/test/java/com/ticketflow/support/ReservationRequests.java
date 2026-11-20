package com.ticketflow.support;

import com.ticketflow.event.dto.CreateEventRequest;
import com.ticketflow.event.service.EventService;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.time.Duration;
import java.time.Instant;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * 예약 테스트용 요청·데이터 헬퍼. 시드 사용자는 id 1~1000.
 */
public final class ReservationRequests {

    public static final String USER_HEADER = "X-Test-User-Id";
    public static final String ROLE_HEADER = "X-Test-User-Role";
    public static final String IDEMPOTENCY_HEADER = "Idempotency-Key";

    private ReservationRequests() {
    }

    public static MockHttpServletRequestBuilder reserve(long userId, String idempotencyKey, long eventId) {
        return post("/api/v1/reservations")
                .header(USER_HEADER, userId)
                .header(IDEMPOTENCY_HEADER, idempotencyKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventId\":" + eventId + "}");
    }

    public static MockHttpServletRequestBuilder getReservation(long userId, Object reservationId) {
        return get("/api/v1/reservations/{id}", reservationId).header(USER_HEADER, userId);
    }

    /** 지금 판매 중인 공연을 만든다. */
    public static long openEvent(EventService eventService, int totalTickets) {
        Instant now = Instant.now();
        return createEvent(eventService, totalTickets, now.minus(Duration.ofHours(1)), now.plus(Duration.ofDays(1)));
    }

    public static long createEvent(EventService eventService, int totalTickets, Instant salesStartAt, Instant salesEndAt) {
        return eventService.create(new CreateEventRequest("예약 테스트 공연", totalTickets, salesStartAt, salesEndAt)).id();
    }
}
