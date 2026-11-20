package com.ticketflow.event.controller;

import com.ticketflow.event.dto.CreateEventRequest;
import com.ticketflow.event.dto.EventResponse;
import com.ticketflow.event.service.EventService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

/**
 * 테스트 관리자용 공연 등록 API. {@code ticketflow.admin.local-endpoints-enabled=true}(local 프로필)일 때만 등록된다 (PRD FR-EVT-01).
 */
@Tag(name = "Events (local admin)", description = "로컬 프로필 전용 공연 등록")
@RestController
@RequestMapping("/api/v1/events")
@ConditionalOnBooleanProperty("ticketflow.admin.local-endpoints-enabled")
public class EventAdminController {

    private final EventService eventService;

    public EventAdminController(EventService eventService) {
        this.eventService = eventService;
    }

    @Operation(summary = "공연 등록", description = "등록 시 remainingTickets = totalTickets")
    @PostMapping
    public ResponseEntity<EventResponse> create(@Valid @RequestBody CreateEventRequest request) {
        EventResponse created = eventService.create(request);
        return ResponseEntity.created(URI.create("/api/v1/events/" + created.id())).body(created);
    }
}
