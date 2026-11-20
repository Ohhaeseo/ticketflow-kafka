package com.ticketflow.event.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.time.Instant;

public record CreateEventRequest(
        @Schema(example = "인기 공연 2026")
        @NotBlank @Size(max = 200)
        String title,

        @Schema(example = "100")
        @NotNull @Positive @Max(100_000)
        Integer totalTickets,

        @Schema(example = "2026-11-23T10:00:00Z")
        @NotNull
        Instant salesStartAt,

        @Schema(example = "2026-12-31T23:59:59Z")
        @NotNull
        Instant salesEndAt
) {
}
