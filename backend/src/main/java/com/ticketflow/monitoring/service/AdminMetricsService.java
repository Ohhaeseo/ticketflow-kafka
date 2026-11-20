package com.ticketflow.monitoring.service;

import com.ticketflow.messaging.config.TicketFlowKafkaProperties;
import com.ticketflow.monitoring.dto.AdminMetricsResponse;
import com.ticketflow.monitoring.dto.AdminMetricsResponse.ConsumerLag;
import com.ticketflow.monitoring.dto.AdminMetricsResponse.DeadLetters;
import com.ticketflow.monitoring.dto.AdminMetricsResponse.EventStock;
import com.ticketflow.monitoring.dto.AdminMetricsResponse.Outbox;
import com.ticketflow.monitoring.dto.DeadLetterResponse;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class AdminMetricsService {

    private static final int RECENT_EVENTS = 20;

    private final JdbcClient jdbcClient;
    private final ConsumerLagService consumerLagService;
    private final TicketFlowKafkaProperties kafkaProperties;
    private final String consumerGroupId;
    private final Clock clock;

    public AdminMetricsService(JdbcTemplate jdbcTemplate, ConsumerLagService consumerLagService,
                               TicketFlowKafkaProperties kafkaProperties, Environment environment, Clock clock) {
        this.jdbcClient = JdbcClient.create(jdbcTemplate);
        this.consumerLagService = consumerLagService;
        this.kafkaProperties = kafkaProperties;
        this.consumerGroupId = environment.getRequiredProperty("spring.kafka.consumer.group-id");
        this.clock = clock;
    }

    public AdminMetricsResponse metrics() {
        Map<String, Long> byStatus = new LinkedHashMap<>();
        for (String status : List.of("QUEUED", "CONFIRMED", "SOLD_OUT", "FAILED")) {
            byStatus.put(status, 0L);
        }
        jdbcClient.sql("SELECT status, count(*) AS cnt FROM reservations GROUP BY status")
                .query((rs, rowNum) -> byStatus.put(rs.getString("status"), rs.getLong("cnt")))
                .list();

        Outbox outbox = jdbcClient.sql("""
                        SELECT count(*) AS unpublished,
                               coalesce(extract(epoch FROM now() - min(created_at)) * 1000, 0)::bigint AS oldest_ms
                          FROM outbox_events WHERE published_at IS NULL
                        """)
                .query((rs, rowNum) -> new Outbox(rs.getLong("unpublished"), rs.getLong("oldest_ms")))
                .single();

        DeadLetters deadLetters = jdbcClient.sql("""
                        SELECT count(*) AS total,
                               count(*) FILTER (WHERE received_at > now() - INTERVAL '24 hours') AS last24h
                          FROM dead_letter_messages
                        """)
                .query((rs, rowNum) -> new DeadLetters(rs.getLong("total"), rs.getLong("last24h")))
                .single();

        List<EventStock> events = jdbcClient.sql("""
                        SELECT e.id, e.title, e.total_tickets, e.remaining_tickets,
                               count(r.id) FILTER (WHERE r.status = 'CONFIRMED') AS confirmed,
                               count(r.id) FILTER (WHERE r.status = 'SOLD_OUT')  AS sold_out,
                               count(r.id) FILTER (WHERE r.status = 'FAILED')    AS failed,
                               count(r.id) FILTER (WHERE r.status = 'QUEUED')    AS queued
                          FROM events e
                          LEFT JOIN reservations r ON r.event_id = e.id
                         GROUP BY e.id
                         ORDER BY e.id DESC
                         LIMIT :limit
                        """)
                .param("limit", RECENT_EVENTS)
                .query((rs, rowNum) -> {
                    int total = rs.getInt("total_tickets");
                    int remaining = rs.getInt("remaining_tickets");
                    long confirmed = rs.getLong("confirmed");
                    return new EventStock(rs.getLong("id"), rs.getString("title"), total, remaining, confirmed,
                            rs.getLong("sold_out"), rs.getLong("failed"), rs.getLong("queued"),
                            remaining >= 0 && confirmed <= total && confirmed + remaining == total);
                })
                .list();

        return new AdminMetricsResponse(clock.instant(), byStatus, outbox, consumerLag(), deadLetters, events);
    }

    public List<DeadLetterResponse> recentDeadLetters(int limit) {
        return jdbcClient.sql("""
                        SELECT id, original_topic, original_partition, original_offset, message_key, payload,
                               exception_cause_class, exception_message, received_at
                          FROM dead_letter_messages
                         ORDER BY received_at DESC, id DESC
                         LIMIT :limit
                        """)
                .param("limit", limit)
                .query((rs, rowNum) -> new DeadLetterResponse(
                        rs.getLong("id"),
                        rs.getString("original_topic"),
                        rs.getInt("original_partition"),
                        rs.getLong("original_offset"),
                        rs.getString("message_key"),
                        rs.getString("payload"),
                        rs.getString("exception_cause_class"),
                        rs.getString("exception_message"),
                        rs.getObject("received_at", OffsetDateTime.class).toInstant()))
                .list();
    }

    private ConsumerLag consumerLag() {
        String topic = kafkaProperties.reservationRequestedTopic();
        try {
            Map<Integer, Long> byPartition = consumerLagService.lagByPartition(consumerGroupId, topic);
            long total = byPartition.values().stream().mapToLong(Long::longValue).sum();
            return new ConsumerLag(consumerGroupId, topic, total, byPartition);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ConsumerLag(consumerGroupId, topic, -1, Map.of());
        } catch (Exception e) {
            return new ConsumerLag(consumerGroupId, topic, -1, Map.of());
        }
    }
}
