package com.bank.recebimentos.boleto.adapter.outbox;

public interface OutboxMessagePublisher {
    void publish(OutboxEvent event, Object payload) throws Exception;
}
