package com.ticketflow.event.controller;

import com.ticketflow.event.dto.EventResponse;
import com.ticketflow.event.service.EventService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Tag(name = "Events", description = "공연 조회")
@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final EventService eventService;

    public EventController(EventService eventService) {
        this.eventService = eventService;
    }

    @Operation(summary = "공연 목록 조회")
    @GetMapping
    public List<EventResponse> list() {
        return eventService.findAll();
    }

    @Operation(summary = "공연 상세 조회", description = "남은 재고(remainingTickets)를 포함한다.")
    @GetMapping("/{eventId}")
    public EventResponse get(@PathVariable Long eventId) {
        return eventService.findById(eventId);
    }
}
