package com.bank.recebimentos.boleto.adapter.outbox;

import com.bank.recebimentos.domain.event.BoletoGeradoIntegrationEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;

@Component
public class OutboxKafkaPublisher {
    private static final String BOLETO_GERADO_EVENT_TYPE = "boleto.gerado";

    private final OutboxEventClaimer outboxEventClaimer;
    private final OutboxMessagePublisher outboxMessagePublisher;
    private final ObjectMapper objectMapper;
    private final int maxAttempts;
    private final int batchSize;
    private final Duration processingTimeout;

    public OutboxKafkaPublisher(OutboxEventClaimer outboxEventClaimer,
                                OutboxMessagePublisher outboxMessagePublisher,
                                ObjectMapper objectMapper,
                                @Value("${app.outbox.max-attempts:5}") int maxAttempts,
                                @Value("${app.outbox.batch-size:50}") int batchSize,
                                @Value("${app.outbox.processing-timeout-seconds:120}") long processingTimeoutSeconds) {
        this.outboxEventClaimer = outboxEventClaimer;
        this.outboxMessagePublisher = outboxMessagePublisher;
        this.objectMapper = objectMapper;
        this.maxAttempts = maxAttempts;
        this.batchSize = batchSize;
        this.processingTimeout = Duration.ofSeconds(processingTimeoutSeconds);
    }

    @Scheduled(fixedDelayString = "${app.outbox.publish-delay-ms:5000}")
    public void publicarPendentes() {
        LocalDateTime now = LocalDateTime.now();
        outboxEventClaimer.releaseExpiredProcessing(
            batchSize,
            now.minus(processingTimeout),
            now
        );
        outboxEventClaimer.claimPending(batchSize, now)
            .forEach(this::publicar);
    }

    private void publicar(OutboxEvent evento) {
        try {
            outboxMessagePublisher.publish(evento, toKafkaPayload(evento));
            outboxEventClaimer.markPublished(evento.getId(), LocalDateTime.now());
        } catch (InvalidOutboxPayloadException exception) {
            outboxEventClaimer.markPermanentlyFailed(evento.getId(), exception);
        } catch (Exception exception) {
            outboxEventClaimer.markFailed(
                evento.getId(),
                exception,
                maxAttempts,
                LocalDateTime.now().plus(backoffForNextAttempt(evento.getAttempts() + 1))
            );
        }
    }

    private Duration backoffForNextAttempt(int nextAttempt) {
        long seconds = Math.min(300, (long) Math.pow(2, Math.max(0, nextAttempt - 1)) * 5);
        return Duration.ofSeconds(seconds);
    }

    private Object toKafkaPayload(OutboxEvent evento) {
        if (BOLETO_GERADO_EVENT_TYPE.equals(evento.getEventType())) {
            return readPayload(evento, BoletoGeradoIntegrationEvent.class);
        }
        throw new InvalidOutboxPayloadException("Tipo de evento nao suportado para publicacao Kafka: " + evento.getEventType());
    }

    private <T> T readPayload(OutboxEvent evento, Class<T> payloadType) {
        try {
            return objectMapper.readValue(evento.getPayload(), payloadType);
        } catch (JsonProcessingException exception) {
            throw new InvalidOutboxPayloadException("Nao foi possivel desserializar payload da outbox: " + evento.getId(), exception);
        }
    }
}
