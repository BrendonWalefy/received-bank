package com.bank.recebimentos.boleto.adapter.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OutboxKafkaPublisherTest {
    @Test
    void devePublicarEventoClaimadoEMarcarComoPublicado() {
        FakeClaimer claimer = new FakeClaimer(List.of(boletoGeradoEvent()));
        FakeMessagePublisher messagePublisher = new FakeMessagePublisher(false);
        OutboxKafkaPublisher publisher = new OutboxKafkaPublisher(
            claimer,
            messagePublisher,
            objectMapper(),
            5,
            50,
            120
        );

        publisher.publicarPendentes();

        assertEquals(1, claimer.releaseExpiredCalls);
        assertEquals(1, messagePublisher.publishedEvents.size());
        assertEquals(List.of("evt-1"), claimer.publishedIds);
    }

    @Test
    void deveRegistrarFalhaQuandoKafkaNaoConfirmaPublicacao() {
        FakeClaimer claimer = new FakeClaimer(List.of(boletoGeradoEvent()));
        FakeMessagePublisher messagePublisher = new FakeMessagePublisher(true);
        OutboxKafkaPublisher publisher = new OutboxKafkaPublisher(
            claimer,
            messagePublisher,
            objectMapper(),
            5,
            50,
            120
        );

        publisher.publicarPendentes();

        assertEquals(List.of("evt-1"), claimer.failedIds);
        assertEquals(5, claimer.lastMaxAttempts);
    }

    @Test
    void deveMarcarFalhaDefinitivaQuandoPayloadNaoPodeSerConvertido() {
        FakeClaimer claimer = new FakeClaimer(List.of(OutboxEvent.pending(
            "evt-2",
            "boleto-2",
            "boleto.gerado",
            "boleto.gerado",
            "boleto-2",
            "{payload-invalido"
        )));
        FakeMessagePublisher messagePublisher = new FakeMessagePublisher(false);
        OutboxKafkaPublisher publisher = new OutboxKafkaPublisher(
            claimer,
            messagePublisher,
            objectMapper(),
            5,
            50,
            120
        );

        publisher.publicarPendentes();

        assertEquals(List.of("evt-2"), claimer.permanentlyFailedIds);
        assertEquals(0, messagePublisher.publishedEvents.size());
    }

    private OutboxEvent boletoGeradoEvent() {
        return OutboxEvent.pending(
            "evt-1",
            "boleto-1",
            "boleto.gerado",
            "boleto.gerado",
            "boleto-1",
            """
                {
                  "eventId": "evt-1",
                  "boletoId": "boleto-1",
                  "beneficiarioCpfCnpj": "12345678901",
                  "beneficiarioNome": "Empresa Beneficiaria",
                  "beneficiarioBanco": "341",
                  "pagadorCpfCnpj": "10987654321",
                  "pagadorNome": "Cliente Pagador",
                  "valor": 99.90,
                  "vencimento": "2026-06-10",
                  "codigoBarras": "12345678901234567890123456789012345678901234567",
                  "linhaDigitavel": "12345678901234567890123456789012345678901234567"
                }
                """
        );
    }

    private ObjectMapper objectMapper() {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private static class FakeClaimer extends OutboxEventClaimer {
        private final List<OutboxEvent> eventsToClaim;
        private final List<String> publishedIds = new ArrayList<>();
        private final List<String> failedIds = new ArrayList<>();
        private final List<String> permanentlyFailedIds = new ArrayList<>();
        private int releaseExpiredCalls;
        private int lastMaxAttempts;

        FakeClaimer(List<OutboxEvent> eventsToClaim) {
            super(null);
            this.eventsToClaim = eventsToClaim;
        }

        @Override
        public List<OutboxEvent> claimPending(int batchSize, LocalDateTime now) {
            return eventsToClaim;
        }

        @Override
        public int releaseExpiredProcessing(int batchSize, LocalDateTime expiredBefore, LocalDateTime nextAttemptAt) {
            releaseExpiredCalls++;
            return 0;
        }

        @Override
        public void markPublished(String eventId, LocalDateTime now) {
            publishedIds.add(eventId);
        }

        @Override
        public void markFailed(String eventId, Exception exception, int maxAttempts, LocalDateTime nextAttemptAt) {
            failedIds.add(eventId);
            lastMaxAttempts = maxAttempts;
        }

        @Override
        public void markPermanentlyFailed(String eventId, Exception exception) {
            permanentlyFailedIds.add(eventId);
        }
    }

    private static class FakeMessagePublisher implements OutboxMessagePublisher {
        private final boolean fail;
        private final List<OutboxEvent> publishedEvents = new ArrayList<>();

        FakeMessagePublisher(boolean fail) {
            this.fail = fail;
        }

        @Override
        public void publish(OutboxEvent event, Object payload) {
            if (fail) {
                throw new IllegalStateException("broker fora");
            }
            publishedEvents.add(event);
        }
    }
}
