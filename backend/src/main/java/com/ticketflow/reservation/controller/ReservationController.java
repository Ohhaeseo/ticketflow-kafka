package com.ticketflow.reservation.controller;

import com.ticketflow.global.auth.CurrentUser;
import com.ticketflow.global.response.ErrorResponse;
import com.ticketflow.idempotency.service.IdempotencyKeys;
import com.ticketflow.reservation.dto.CreateReservationRequest;
import com.ticketflow.reservation.dto.ReservationResponse;
import com.ticketflow.reservation.service.ReservationResult;
import com.ticketflow.reservation.service.ReservationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;

@Tag(name = "Reservations", description = "예약 접수·조회. 202는 좌석 확정이 아니라 접수 완료를 뜻한다.")
@RestController
@RequestMapping("/api/v1")
public class ReservationController {

    private final ReservationService reservationService;

    public ReservationController(ReservationService reservationService) {
        this.reservationService = reservationService;
    }

    @Operation(summary = "예약 접수",
            description = "최초 접수는 202(QUEUED). 같은 Idempotency-Key·같은 본문으로 다시 보내면 200과 기존 예약. "
                    + "같은 키에 다른 본문이면 409 IDEMPOTENCY_KEY_REUSED, 같은 공연에 다른 키면 409 RESERVATION_ALREADY_EXISTS.")
    @Parameter(name = "X-Test-User-Id", in = ParameterIn.HEADER, required = true,
            description = "테스트용 사용자 ID (local 전용, 1~1000 시드)", schema = @Schema(type = "integer", example = "42"))
    @ApiResponse(responseCode = "202", description = "최초 접수 (QUEUED). 좌석 확정이 아님")
    @ApiResponse(responseCode = "201", description = "동기 비교 모드(processing-mode=sync)에서 최초 접수. 이미 CONFIRMED/SOLD_OUT")
    @ApiResponse(responseCode = "200", description = "같은 키·같은 본문 재요청. 기존 예약의 현재 상태")
    @ApiResponse(responseCode = "400", description = "INVALID_REQUEST, INVALID_IDEMPOTENCY_KEY",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @ApiResponse(responseCode = "401", description = "UNAUTHENTICATED",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @ApiResponse(responseCode = "404", description = "EVENT_NOT_FOUND",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @ApiResponse(responseCode = "409", description = "IDEMPOTENCY_KEY_REUSED, RESERVATION_ALREADY_EXISTS(reservationId 포함)",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @ApiResponse(responseCode = "422", description = "SALES_NOT_STARTED, SALES_ENDED",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @PostMapping("/reservations")
    public ResponseEntity<ReservationResponse> reserve(
            @Parameter(hidden = true) CurrentUser user,
            @Parameter(example = "demo-user42-concert1-attempt1")
            @RequestHeader(IdempotencyKeys.HEADER) String idempotencyKey,
            @Valid @RequestBody CreateReservationRequest request) {
        ReservationResult result = reservationService.reserve(user, idempotencyKey, request);
        ReservationResponse body = result.reservation();
        if (result.created() && result.processed()) {
            // 동기 비교 모드: 응답 시점에 최종 상태가 정해져 있다
            return ResponseEntity.created(URI.create(body.statusUrl())).body(body);
        }
        if (result.created()) {
            return ResponseEntity.accepted().location(URI.create(body.statusUrl())).body(body);
        }
        return ResponseEntity.ok(body);
    }

    @Operation(summary = "예약 상태 조회", description = "본인 또는 관리자만 조회할 수 있다. 다른 사용자의 예약은 404.")
    @Parameter(name = "X-Test-User-Id", in = ParameterIn.HEADER, required = true,
            description = "테스트용 사용자 ID (local 전용)", schema = @Schema(type = "integer", example = "42"))
    @Parameter(name = "X-Test-User-Role", in = ParameterIn.HEADER,
            description = "ADMIN이면 모든 예약 조회 가능 (local 전용)", schema = @Schema(type = "string", example = "ADMIN"))
    @GetMapping("/reservations/{reservationId}")
    public ReservationResponse get(@Parameter(hidden = true) CurrentUser user, @PathVariable long reservationId) {
        return reservationService.get(user, reservationId);
    }

    @Operation(summary = "내 예약 목록", description = "최근 접수 순")
    @Parameter(name = "X-Test-User-Id", in = ParameterIn.HEADER, required = true,
            description = "테스트용 사용자 ID (local 전용)", schema = @Schema(type = "integer", example = "42"))
    @GetMapping("/users/me/reservations")
    public List<ReservationResponse> mine(@Parameter(hidden = true) CurrentUser user) {
        return reservationService.findMine(user);
    }
}
