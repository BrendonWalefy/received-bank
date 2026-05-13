package com.bank.recebimentos.boleto.adapter.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {
    @Query(value = """
        SELECT *
        FROM outbox_events
        WHERE status = 'PENDING'
          AND (next_attempt_at IS NULL OR next_attempt_at <= :now)
        ORDER BY created_at
        LIMIT :batchSize
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<OutboxEvent> lockNextPending(@Param("now") LocalDateTime now, @Param("batchSize") int batchSize);

    @Query(value = """
        SELECT *
        FROM outbox_events
        WHERE status = 'PROCESSING'
          AND processing_started_at <= :expiredBefore
        ORDER BY processing_started_at
        LIMIT :batchSize
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<OutboxEvent> lockExpiredProcessing(@Param("expiredBefore") LocalDateTime expiredBefore,
                                            @Param("batchSize") int batchSize);
}
