package com.bank.recebimentos.boleto.adapter.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {
    @Id
    private String id;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(nullable = false)
    private String topic;

    @Column(name = "message_key", nullable = false)
    private String messageKey;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxEventStatus status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "next_attempt_at")
    private LocalDateTime nextAttemptAt;

    @Column(name = "processing_started_at")
    private LocalDateTime processingStartedAt;

    @Column(name = "published_at")
    private LocalDateTime publishedAt;

    protected OutboxEvent() {
    }

    private OutboxEvent(String id,
                        String aggregateId,
                        String eventType,
                        String topic,
                        String messageKey,
                        String payload) {
        this.id = id;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.topic = topic;
        this.messageKey = messageKey;
        this.payload = payload;
        this.status = OutboxEventStatus.PENDING;
        this.attempts = 0;
        this.createdAt = LocalDateTime.now();
        this.nextAttemptAt = this.createdAt;
    }

    public static OutboxEvent pending(String id,
                                      String aggregateId,
                                      String eventType,
                                      String topic,
                                      String messageKey,
                                      String payload) {
        return new OutboxEvent(id, aggregateId, eventType, topic, messageKey, payload);
    }

    public String getId() {
        return id;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxEventStatus getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getLastError() {
        return lastError;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getNextAttemptAt() {
        return nextAttemptAt;
    }

    public LocalDateTime getProcessingStartedAt() {
        return processingStartedAt;
    }

    public LocalDateTime getPublishedAt() {
        return publishedAt;
    }

    public void marcarProcessando(LocalDateTime now) {
        this.status = OutboxEventStatus.PROCESSING;
        this.processingStartedAt = now;
        this.lastError = null;
    }

    public void marcarPublicado(LocalDateTime now) {
        this.status = OutboxEventStatus.PUBLISHED;
        this.publishedAt = now;
        this.processingStartedAt = null;
        this.nextAttemptAt = null;
        this.lastError = null;
    }

    public void registrarFalha(Exception exception, int maxAttempts, LocalDateTime nextAttemptAt) {
        this.attempts++;
        this.lastError = exception.getMessage();
        this.processingStartedAt = null;
        if (this.attempts >= maxAttempts) {
            this.status = OutboxEventStatus.FAILED;
            this.nextAttemptAt = null;
            return;
        }

        this.status = OutboxEventStatus.PENDING;
        this.nextAttemptAt = nextAttemptAt;
    }

    public void registrarFalhaDefinitiva(Exception exception) {
        this.attempts++;
        this.lastError = exception.getMessage();
        this.processingStartedAt = null;
        this.nextAttemptAt = null;
        this.status = OutboxEventStatus.FAILED;
    }

    public void liberarProcessamentoExpirado(LocalDateTime nextAttemptAt) {
        this.status = OutboxEventStatus.PENDING;
        this.processingStartedAt = null;
        this.nextAttemptAt = nextAttemptAt;
    }
}
