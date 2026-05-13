package com.bank.recebimentos.boleto.adapter.outbox;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class OutboxEventClaimer {
    private final OutboxEventRepository repository;

    public OutboxEventClaimer(OutboxEventRepository repository) {
        this.repository = repository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<OutboxEvent> claimPending(int batchSize, LocalDateTime now) {
        List<OutboxEvent> events = repository.lockNextPending(now, batchSize);
        events.forEach(event -> event.marcarProcessando(now));
        return events;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int releaseExpiredProcessing(int batchSize, LocalDateTime expiredBefore, LocalDateTime nextAttemptAt) {
        List<OutboxEvent> events = repository.lockExpiredProcessing(expiredBefore, batchSize);
        events.forEach(event -> event.liberarProcessamentoExpirado(nextAttemptAt));
        return events.size();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markPublished(String eventId, LocalDateTime now) {
        repository.findById(eventId).ifPresent(event -> event.marcarPublicado(now));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markFailed(String eventId, Exception exception, int maxAttempts, LocalDateTime nextAttemptAt) {
        repository.findById(eventId)
            .ifPresent(event -> event.registrarFalha(exception, maxAttempts, nextAttemptAt));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markPermanentlyFailed(String eventId, Exception exception) {
        repository.findById(eventId).ifPresent(event -> event.registrarFalhaDefinitiva(exception));
    }
}
