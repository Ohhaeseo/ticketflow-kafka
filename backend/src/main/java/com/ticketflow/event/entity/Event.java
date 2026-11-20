package com.ticketflow.event.entity;

import com.ticketflow.global.exception.BusinessException;
import com.ticketflow.global.exception.ErrorCode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.Objects;

/**
 * 공연(행사). 잔여 수량은 엔티티 필드 변경이 아니라 DB 조건부 UPDATE로만 감소시킨다 (PRD FR-INV-01).
 */
@Entity
@Table(name = "events")
public class Event {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 200)
    private String title;

    @Column(name = "total_tickets", nullable = false, updatable = false)
    private int totalTickets;

    @Column(name = "remaining_tickets", nullable = false, updatable = false)
    private int remainingTickets;

    @Column(name = "sales_start_at", nullable = false)
    private Instant salesStartAt;

    @Column(name = "sales_end_at", nullable = false)
    private Instant salesEndAt;

    protected Event() {
    }

    private Event(String title, int totalTickets, Instant salesStartAt, Instant salesEndAt) {
        this.title = title;
        this.totalTickets = totalTickets;
        this.remainingTickets = totalTickets;
        this.salesStartAt = salesStartAt;
        this.salesEndAt = salesEndAt;
    }

    public static Event create(String title, int totalTickets, Instant salesStartAt, Instant salesEndAt) {
        Objects.requireNonNull(salesStartAt, "salesStartAt");
        Objects.requireNonNull(salesEndAt, "salesEndAt");
        if (totalTickets <= 0) {
            throw new IllegalArgumentException("totalTickets must be positive");
        }
        if (!salesStartAt.isBefore(salesEndAt)) {
            throw new BusinessException(ErrorCode.INVALID_SALES_PERIOD);
        }
        return new Event(title, totalTickets, salesStartAt, salesEndAt);
    }

    public Long getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public int getTotalTickets() {
        return totalTickets;
    }

    public int getRemainingTickets() {
        return remainingTickets;
    }

    public Instant getSalesStartAt() {
        return salesStartAt;
    }

    public Instant getSalesEndAt() {
        return salesEndAt;
    }
}
