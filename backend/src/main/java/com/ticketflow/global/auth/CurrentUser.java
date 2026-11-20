package com.ticketflow.global.auth;

/**
 * 요청을 보낸 사용자. 컨트롤러 메서드 파라미터로 받는다 ({@link CurrentUserArgumentResolver}).
 */
public record CurrentUser(long userId, boolean admin) {

    public boolean canAccess(long ownerUserId) {
        return admin || userId == ownerUserId;
    }
}
