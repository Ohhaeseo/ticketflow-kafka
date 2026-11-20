package com.ticketflow.user.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * 테스트용 합성 사용자. 실제 개인정보는 저장하지 않는다.
 */
@Entity
@Table(name = "users")
public class User {

    @Id
    private Long id;

    @Column(nullable = false, length = 50)
    private String nickname;

    protected User() {
    }

    public Long getId() {
        return id;
    }

    public String getNickname() {
        return nickname;
    }
}
