package com.ticketflow.monitoring.controller;

import com.ticketflow.global.auth.CurrentUser;
import com.ticketflow.global.exception.BusinessException;
import com.ticketflow.global.exception.ErrorCode;
import com.ticketflow.monitoring.dto.AdminMetricsResponse;
import com.ticketflow.monitoring.dto.DeadLetterResponse;
import com.ticketflow.monitoring.service.AdminMetricsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Tag(name = "Admin", description = "운영 지표와 DLT 확인 (관리자 전용)")
@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {

    private static final int MAX_LIMIT = 200;

    private final AdminMetricsService adminMetricsService;

    public AdminController(AdminMetricsService adminMetricsService) {
        this.adminMetricsService = adminMetricsService;
    }

    @Operation(summary = "운영 지표", description = "상태별 예약 수, Outbox 미발행, Consumer lag, DLT, 최근 공연별 재고 불변식")
    @Parameter(name = "X-Test-User-Id", in = ParameterIn.HEADER, required = true, schema = @Schema(type = "integer", example = "1"))
    @Parameter(name = "X-Test-User-Role", in = ParameterIn.HEADER, required = true, schema = @Schema(type = "string", example = "ADMIN"))
    @GetMapping("/metrics")
    public AdminMetricsResponse metrics(@Parameter(hidden = true) CurrentUser user) {
        requireAdmin(user);
        return adminMetricsService.metrics();
    }

    @Operation(summary = "DLT 메시지", description = "최근에 DLT로 이동한 메시지와 실패 원인")
    @Parameter(name = "X-Test-User-Id", in = ParameterIn.HEADER, required = true, schema = @Schema(type = "integer", example = "1"))
    @Parameter(name = "X-Test-User-Role", in = ParameterIn.HEADER, required = true, schema = @Schema(type = "string", example = "ADMIN"))
    @GetMapping("/dlt")
    public List<DeadLetterResponse> deadLetters(@Parameter(hidden = true) CurrentUser user,
                                                @RequestParam(defaultValue = "20") int limit) {
        requireAdmin(user);
        return adminMetricsService.recentDeadLetters(Math.clamp(limit, 1, MAX_LIMIT));
    }

    private static void requireAdmin(CurrentUser user) {
        if (!user.admin()) {
            throw new BusinessException(ErrorCode.FORBIDDEN);
        }
    }
}
