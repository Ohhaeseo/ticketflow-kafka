package com.ticketflow.idempotency.service;

import com.ticketflow.global.exception.BusinessException;
import com.ticketflow.global.exception.ErrorCode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.regex.Pattern;

/**
 * 멱등성 키 검증과 요청 해시 계산 (PRD FR-RSV-02).
 */
public final class IdempotencyKeys {

    public static final String HEADER = "Idempotency-Key";
    public static final int MAX_LENGTH = 100;

    private static final Pattern ALLOWED = Pattern.compile("[A-Za-z0-9._:-]{1," + MAX_LENGTH + "}");

    private IdempotencyKeys() {
    }

    public static String validate(String key) {
        if (key == null || !ALLOWED.matcher(key).matches()) {
            throw new BusinessException(ErrorCode.INVALID_IDEMPOTENCY_KEY);
        }
        return key;
    }

    /**
     * 요청 본문을 정규화한 문자열의 SHA-256(hex 64자). 같은 키로 다른 본문이 오면 해시가 달라진다.
     */
    public static String requestHash(String canonicalRequest) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonicalRequest.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
