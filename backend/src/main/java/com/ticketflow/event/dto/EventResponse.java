package com.ticketflow.event.dto;

import com.ticketflow.event.entity.Event;

import java.time.Instant;

public record EventResponse(
        Long id,
        String title,
        int totalTickets,
        int remainingTickets,
        Instant salesStartAt,
        Instant salesEndAt
) {

    public static EventResponse from(Event event) {
        return new EventResponse(
                event.getId(),
                event.getTitle(),
                event.getTotalTickets(),
                event.getRemainingTickets(),
                event.getSalesStartAt(),
                event.getSalesEndAt());
    }
}
