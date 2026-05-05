package com.bank.recebimentos.payment.application;

import com.bank.recebimentos.domain.event.BoletoGeradoIntegrationEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class EfetivarPagamentoUseCaseTest {
    @Test
    void deveGerarEventoDePagamentoEfetivado() {
        EfetivarPagamentoUseCase useCase = new EfetivarPagamentoUseCase();
        String boletoId = UUID.randomUUID().toString();

        var pagamento = useCase.efetivar(new BoletoGeradoIntegrationEvent(
            UUID.randomUUID().toString(),
            boletoId,
            "12345678901",
            "Empresa Beneficiaria",
            "341",
            "10987654321",
            "Cliente Pagador",
            new BigDecimal("120.50"),
            LocalDate.now().plusDays(1),
            "12345678901234567890123456789012345678901234567",
            "12345678901234567890123456789012345678901234567"
        ));

        assertEquals(boletoId, pagamento.boletoId());
        assertEquals(new BigDecimal("120.50"), pagamento.valorPago());
        assertEquals("SIMULADO", pagamento.canal());
        assertNotNull(pagamento.eventId());
        assertNotNull(pagamento.pagoEm());
    }
}
