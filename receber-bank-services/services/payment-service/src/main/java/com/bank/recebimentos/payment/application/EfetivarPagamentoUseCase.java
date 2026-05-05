package com.bank.recebimentos.payment.application;

import com.bank.recebimentos.domain.event.BoletoGeradoIntegrationEvent;
import com.bank.recebimentos.domain.event.PagamentoEfetivadoIntegrationEvent;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class EfetivarPagamentoUseCase {
    public PagamentoEfetivadoIntegrationEvent efetivar(BoletoGeradoIntegrationEvent evento) {
        return new PagamentoEfetivadoIntegrationEvent(
            UUID.randomUUID().toString(),
            evento.boletoId(),
            evento.valor(),
            Instant.now(),
            "SIMULADO"
        );
    }
}
