package com.ticketflow.reservation.service;

import com.ticketflow.event.entity.Event;
import com.ticketflow.event.repository.EventRepository;
import com.ticketflow.global.auth.CurrentUser;
import com.ticketflow.global.exception.BusinessException;
import com.ticketflow.global.exception.DuplicateReservationException;
import com.ticketflow.global.exception.ErrorCode;
import com.ticketflow.idempotency.service.IdempotencyKeys;
import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.messaging.producer.ReservationEventOutbox;
import com.ticketflow.reservation.dto.CreateReservationRequest;
import com.ticketflow.reservation.dto.ReservationResponse;
import com.ticketflow.reservation.entity.Reservation;
import com.ticketflow.reservation.repository.ReservationCommandRepository;
import com.ticketflow.reservation.repository.ReservationRepository;
import com.ticketflow.user.repository.UserRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

/**
 * 예약 접수와 조회. 접수는 QUEUED 저장 + Outbox 기록까지만 하고 재고는 건드리지 않는다.
 * 재고 감소와 확정은 Consumer가 {@link ReservationProcessor}로 처리한다.
 */
@Service
@Transactional(readOnly = true)
public class ReservationService {

    private final ReservationRepository reservationRepository;
    private final ReservationCommandRepository reservationCommandRepository;
    private final EventRepository eventRepository;
    private final UserRepository userRepository;
    private final ReservationEventOutbox reservationEventOutbox;
    private final ReservationProcessor reservationProcessor;
    private final ReservationProperties reservationProperties;
    private final Clock clock;

    public ReservationService(ReservationRepository reservationRepository,
                              ReservationCommandRepository reservationCommandRepository,
                              EventRepository eventRepository,
                              UserRepository userRepository,
                              ReservationEventOutbox reservationEventOutbox,
                              ReservationProcessor reservationProcessor,
                              ReservationProperties reservationProperties,
                              Clock clock) {
        this.reservationRepository = reservationRepository;
        this.reservationCommandRepository = reservationCommandRepository;
        this.eventRepository = eventRepository;
        this.userRepository = userRepository;
        this.reservationEventOutbox = reservationEventOutbox;
        this.reservationProcessor = reservationProcessor;
        this.reservationProperties = reservationProperties;
        this.clock = clock;
    }

    /**
     * 예약을 접수한다.
     * <ol>
     *   <li>같은 사용자·같은 키의 예약이 있으면 본문을 비교해 기존 예약(200) 또는 409를 반환한다.</li>
     *   <li>사용자, 공연, 판매 기간을 검증한다.</li>
     *   <li>INSERT ... ON CONFLICT DO NOTHING으로 저장한다. 동시 요청이 경쟁하면 DB가 한 건만 통과시킨다.</li>
     *   <li>충돌로 저장되지 않았으면 이긴 쪽의 예약을 다시 읽어 1번과 같은 규칙으로 응답한다.</li>
     * </ol>
     */
    @Transactional
    public ReservationResult reserve(CurrentUser user, String idempotencyKey, CreateReservationRequest request) {
        IdempotencyKeys.validate(idempotencyKey);
        String requestHash = IdempotencyKeys.requestHash(request.canonical());

        Optional<ReservationResult> replay = findReplay(user.userId(), idempotencyKey, requestHash);
        if (replay.isPresent()) {
            return replay.get();
        }

        if (!userRepository.existsById(user.userId())) {
            throw new BusinessException(ErrorCode.UNAUTHENTICATED, "등록되지 않은 사용자입니다.");
        }
        Event event = eventRepository.findById(request.eventId())
                .orElseThrow(() -> new BusinessException(ErrorCode.EVENT_NOT_FOUND));
        // PostgreSQL timestamptz는 마이크로초까지 저장한다. 202 응답과 이후 조회 값이 같도록 미리 자른다.
        Instant now = clock.instant().truncatedTo(ChronoUnit.MICROS);
        validateSalesPeriod(event, now);

        Optional<Long> createdId = reservationCommandRepository.insertQueuedIfAbsent(
                event.getId(), user.userId(), idempotencyKey, requestHash, now);
        if (createdId.isPresent()) {
            long reservationId = createdId.get();
            if (reservationProperties.synchronous()) {
                return processSynchronously(reservationId, event.getId(), now);
            }
            // 같은 트랜잭션에서 Outbox 기록: 예약이 커밋되면 이벤트도 반드시 남는다 (FR-OBX-01)
            reservationEventOutbox.reservationRequested(reservationId, event.getId(), now);
            return ReservationResult.accepted(ReservationResponse.queued(reservationId, event.getId(), now));
        }

        return findReplay(user.userId(), idempotencyKey, requestHash)
                .orElseThrow(() -> reservationRepository.findByEventIdAndUserId(event.getId(), user.userId())
                        .map(existing -> new DuplicateReservationException(existing.getId()))
                        .orElseThrow(() -> new IllegalStateException(
                                "Reservation insert conflicted but no conflicting row was found")));
    }

    public ReservationResponse get(CurrentUser user, long reservationId) {
        return reservationRepository.findById(reservationId)
                // 다른 사람의 예약은 존재 여부도 드러내지 않도록 404로 응답한다
                .filter(reservation -> user.canAccess(reservation.getUserId()))
                .map(ReservationResponse::from)
                .orElseThrow(() -> new BusinessException(ErrorCode.RESERVATION_NOT_FOUND));
    }

    public List<ReservationResponse> findMine(CurrentUser user) {
        return reservationRepository.findAllByUserIdOrderByIdDesc(user.userId()).stream()
                .map(ReservationResponse::from)
                .toList();
    }

    private Optional<ReservationResult> findReplay(long userId, String idempotencyKey, String requestHash) {
        return reservationRepository.findByUserIdAndIdempotencyKey(userId, idempotencyKey)
                .map(existing -> {
                    if (!existing.hasSameRequest(requestHash)) {
                        throw new BusinessException(ErrorCode.IDEMPOTENCY_KEY_REUSED);
                    }
                    return ReservationResult.replay(ReservationResponse.from(existing));
                });
    }

    /**
     * 벤치마크 비교용 동기 처리 (FR-BMK-01). Consumer와 같은 처리 로직을 요청 트랜잭션 안에서 바로 실행하므로
     * DB 작업은 비동기 방식과 같고, Outbox·Kafka만 빠진다. 응답 시점에 이미 최종 상태다.
     */
    private ReservationResult processSynchronously(long reservationId, long eventId, Instant now) {
        reservationProcessor.process(ReservationRequestedMessage.create(reservationId, eventId, now));
        Reservation processed = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new IllegalStateException("Reservation disappeared: " + reservationId));
        return ReservationResult.processedNow(ReservationResponse.from(processed));
    }

    private static void validateSalesPeriod(Event event, Instant now) {
        if (now.isBefore(event.getSalesStartAt())) {
            throw new BusinessException(ErrorCode.SALES_NOT_STARTED);
        }
        if (!now.isBefore(event.getSalesEndAt())) {
            throw new BusinessException(ErrorCode.SALES_ENDED);
        }
    }
}
