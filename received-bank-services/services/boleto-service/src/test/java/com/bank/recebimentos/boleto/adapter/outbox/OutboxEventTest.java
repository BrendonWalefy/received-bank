package com.bank.recebimentos.boleto.adapter.outbox;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OutboxEventTest {
    @Test
    void deveControlarCicloDeVidaDoEvento() {
        OutboxEvent event = OutboxEvent.pending(
            "evt-1",
            "boleto-1",
            "boleto.gerado",
            "boleto.gerado",
            "boleto-1",
            "{}"
        );
        LocalDateTime now = LocalDateTime.of(2026, 5, 13, 10, 0);

        event.marcarProcessando(now);

        assertEquals(OutboxEventStatus.PROCESSING, event.getStatus());
        assertEquals(now, event.getProcessingStartedAt());

        event.marcarPublicado(now.plusSeconds(2));

        assertEquals(OutboxEventStatus.PUBLISHED, event.getStatus());
        assertEquals(now.plusSeconds(2), event.getPublishedAt());
        assertNull(event.getProcessingStartedAt());
        assertNull(event.getNextAttemptAt());
        assertNull(event.getLastError());
    }

    @Test
    void deveVoltarParaPendenteQuandoFalhaAntesDoMaximoDeTentativas() {
        OutboxEvent event = OutboxEvent.pending(
            "evt-1",
            "boleto-1",
            "boleto.gerado",
            "boleto.gerado",
            "boleto-1",
            "{}"
        );
        LocalDateTime now = LocalDateTime.of(2026, 5, 13, 10, 0);

        event.marcarProcessando(now);
        event.registrarFalha(new RuntimeException("kafka indisponivel"), 5, now.plusSeconds(5));

        assertEquals(OutboxEventStatus.PENDING, event.getStatus());
        assertEquals(1, event.getAttempts());
        assertEquals("kafka indisponivel", event.getLastError());
        assertEquals(now.plusSeconds(5), event.getNextAttemptAt());
        assertNull(event.getProcessingStartedAt());
    }

    @Test
    void deveMarcarComoFailedQuandoAtingeMaximoDeTentativas() {
        OutboxEvent event = OutboxEvent.pending(
            "evt-1",
            "boleto-1",
            "boleto.gerado",
            "boleto.gerado",
            "boleto-1",
            "{}"
        );
        LocalDateTime now = LocalDateTime.of(2026, 5, 13, 10, 0);

        event.registrarFalha(new RuntimeException("payload invalido"), 1, now.plusSeconds(5));

        assertEquals(OutboxEventStatus.FAILED, event.getStatus());
        assertEquals(1, event.getAttempts());
        assertNull(event.getNextAttemptAt());
    }

    @Test
    void deveMarcarFalhaDefinitivaSemNovoAgendamento() {
        OutboxEvent event = OutboxEvent.pending(
            "evt-1",
            "boleto-1",
            "boleto.gerado",
            "boleto.gerado",
            "boleto-1",
            "{}"
        );

        event.registrarFalhaDefinitiva(new RuntimeException("payload invalido"));

        assertEquals(OutboxEventStatus.FAILED, event.getStatus());
        assertEquals(1, event.getAttempts());
        assertEquals("payload invalido", event.getLastError());
        assertNull(event.getNextAttemptAt());
        assertNull(event.getProcessingStartedAt());
    }
}
