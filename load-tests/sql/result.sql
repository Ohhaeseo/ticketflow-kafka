-- 부하 테스트 결과 집계. 사용: psql -v title='LOAD-01 load01-async' < result.sql
-- k6가 만든 공연은 모두 같은 title을 가진다 (load-tests/lib/common.js).
\set ON_ERROR_STOP on
\pset footer off

\echo '--- reservations (M2 confirm latency = updated_at - created_at)'
SELECT count(*)                                         AS reservations,
       count(*) FILTER (WHERE r.status = 'CONFIRMED')   AS confirmed,
       count(*) FILTER (WHERE r.status = 'SOLD_OUT')    AS sold_out,
       count(*) FILTER (WHERE r.status = 'FAILED')      AS failed,
       count(*) FILTER (WHERE r.status = 'QUEUED')      AS queued,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM r.updated_at - r.created_at))::numeric * 1000, 1) AS confirm_p50_ms,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM r.updated_at - r.created_at))::numeric * 1000, 1) AS confirm_p95_ms,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM r.updated_at - r.created_at))::numeric * 1000, 1) AS confirm_p99_ms,
       round(max(extract(epoch FROM r.updated_at - r.created_at))::numeric * 1000, 1)                                          AS confirm_max_ms
  FROM reservations r
  JOIN events e ON e.id = r.event_id
 WHERE e.title = :'title';

\echo '--- stock invariant per event (PRD 12.3)'
SELECT e.id                                                  AS event_id,
       e.total_tickets,
       e.remaining_tickets,
       count(r.id) FILTER (WHERE r.status = 'CONFIRMED')     AS confirmed,
       (e.remaining_tickets >= 0
        AND count(r.id) FILTER (WHERE r.status = 'CONFIRMED') <= e.total_tickets
        AND count(r.id) FILTER (WHERE r.status = 'CONFIRMED') + e.remaining_tickets = e.total_tickets) AS invariant_ok
  FROM events e
  LEFT JOIN reservations r ON r.event_id = e.id
 WHERE e.title = :'title'
 GROUP BY e.id
 ORDER BY e.id;

\echo '--- outbox (M7, async only)'
SELECT count(o.id)                                     AS outbox_rows,
       count(o.id) FILTER (WHERE o.published_at IS NULL) AS unpublished,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM o.published_at - o.created_at))::numeric * 1000, 1) AS publish_p95_ms,
       round(max(extract(epoch FROM o.published_at - o.created_at))::numeric * 1000, 1)                                          AS publish_max_ms
  FROM outbox_events o
  JOIN reservations r ON r.id = o.reservation_id
  JOIN events e ON e.id = r.event_id
 WHERE e.title = :'title';

\echo '--- processing throughput (processed_messages)'
SELECT count(*)                                                                AS processed,
       round(extract(epoch FROM max(p.processed_at) - min(p.processed_at))::numeric, 2) AS span_s,
       round(count(*) / nullif(extract(epoch FROM max(p.processed_at) - min(p.processed_at)), 0)) AS per_second
  FROM processed_messages p
  JOIN reservations r ON r.id = p.reservation_id
  JOIN events e ON e.id = r.event_id
 WHERE e.title = :'title';
