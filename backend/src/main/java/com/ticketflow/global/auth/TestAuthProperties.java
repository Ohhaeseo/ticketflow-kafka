package com.ticketflow.global.auth;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 테스트용 헤더 인증 설정 (PRD Q4). 기본값은 꺼짐이고 local 프로필에서만 켠다.
 */
@ConfigurationProperties(prefix = "ticketflow.test-auth")
public record TestAuthProperties(boolean enabled) {
}
