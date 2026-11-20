package com.ticketflow.global.auth;

import com.ticketflow.global.exception.BusinessException;
import com.ticketflow.global.exception.ErrorCode;
import org.springframework.core.MethodParameter;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

/**
 * <b>테스트 전용</b> 사용자 식별. {@code X-Test-User-Id} 헤더 값을 그대로 믿으므로 실제 인증이 아니다.
 * {@code ticketflow.test-auth.enabled=false}(기본값)이면 모든 요청을 401로 거부한다.
 * 공개 배포 전에 실제 인증·인가로 교체해야 한다 (PRD NFR-SEC-03).
 */
@Component
public class CurrentUserArgumentResolver implements HandlerMethodArgumentResolver {

    public static final String USER_ID_HEADER = "X-Test-User-Id";
    public static final String ROLE_HEADER = "X-Test-User-Role";
    public static final String ADMIN_ROLE = "ADMIN";

    private final TestAuthProperties properties;

    public CurrentUserArgumentResolver(TestAuthProperties properties) {
        this.properties = properties;
    }

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return CurrentUser.class.equals(parameter.getParameterType());
    }

    @Override
    public CurrentUser resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer,
                                       NativeWebRequest webRequest, WebDataBinderFactory binderFactory) {
        if (!properties.enabled()) {
            throw new BusinessException(ErrorCode.UNAUTHENTICATED,
                    "인증이 구성되지 않았습니다. 테스트용 헤더 인증은 local 프로필에서만 켜집니다.");
        }
        String rawUserId = webRequest.getHeader(USER_ID_HEADER);
        long userId = parseUserId(rawUserId);
        boolean admin = ADMIN_ROLE.equalsIgnoreCase(webRequest.getHeader(ROLE_HEADER));
        return new CurrentUser(userId, admin);
    }

    private static long parseUserId(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new BusinessException(ErrorCode.UNAUTHENTICATED, USER_ID_HEADER + " 헤더가 필요합니다.");
        }
        try {
            long userId = Long.parseLong(raw.trim());
            if (userId <= 0) {
                throw new NumberFormatException();
            }
            return userId;
        } catch (NumberFormatException e) {
            throw new BusinessException(ErrorCode.UNAUTHENTICATED, USER_ID_HEADER + " 헤더는 양의 정수여야 합니다.");
        }
    }
}
