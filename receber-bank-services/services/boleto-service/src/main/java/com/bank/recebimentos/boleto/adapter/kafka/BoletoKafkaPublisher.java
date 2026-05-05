package com.bank.recebimentos.boleto.adapter.kafka;

import com.bank.recebimentos.boleto.application.port.BoletoEventPublisher;
import com.bank.recebimentos.boleto.domain.events.BoletoGeradoEvent;
import com.bank.recebimentos.domain.event.BoletoGeradoIntegrationEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class BoletoKafkaPublisher implements BoletoEventPublisher {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.boleto-gerado:boleto.gerado}")
    private String boletoGeradoTopic;

    public BoletoKafkaPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publicarBoletoGerado(BoletoGeradoEvent evento) {
        kafkaTemplate.send(boletoGeradoTopic, evento.getBoletoId(), toIntegrationEvent(evento));
    }

    private BoletoGeradoIntegrationEvent toIntegrationEvent(BoletoGeradoEvent evento) {
        return new BoletoGeradoIntegrationEvent(
            evento.getEventId(),
            evento.getBoletoId(),
            evento.getBeneficiarioCpfCnpj(),
            evento.getBeneficiarioNome(),
            evento.getBeneficiarioBanco(),
            evento.getPagadorCpfCnpj(),
            evento.getPagadorNome(),
            evento.getValor(),
            evento.getVencimento(),
            evento.getCodigoBarras(),
            evento.getLinhaDigitavel()
        );
    }
}
