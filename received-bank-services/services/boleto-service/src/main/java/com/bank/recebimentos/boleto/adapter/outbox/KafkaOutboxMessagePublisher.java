package com.bank.recebimentos.boleto.adapter.outbox;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class KafkaOutboxMessagePublisher implements OutboxMessagePublisher {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaOutboxMessagePublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(OutboxEvent event, Object payload) throws Exception {
        kafkaTemplate.send(event.getTopic(), event.getMessageKey(), payload)
            .get(10, TimeUnit.SECONDS);
    }
}
