package com.bank.recebimentos.payment.adapter.kafka;

import com.bank.recebimentos.domain.event.BoletoGeradoIntegrationEvent;
import com.bank.recebimentos.payment.application.EfetivarPagamentoUseCase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class BoletoGeradoConsumer {
    private final EfetivarPagamentoUseCase efetivarPagamentoUseCase;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String pagamentoEfetivadoTopic;

    public BoletoGeradoConsumer(EfetivarPagamentoUseCase efetivarPagamentoUseCase,
                                KafkaTemplate<String, Object> kafkaTemplate,
                                @Value("${app.kafka.topics.pagamento-efetivado:pagamento.efetivado}")
                                String pagamentoEfetivadoTopic) {
        this.efetivarPagamentoUseCase = efetivarPagamentoUseCase;
        this.kafkaTemplate = kafkaTemplate;
        this.pagamentoEfetivadoTopic = pagamentoEfetivadoTopic;
    }

    @KafkaListener(
        topics = "${app.kafka.topics.boleto-gerado:boleto.gerado}",
        groupId = "${spring.kafka.consumer.group-id:payment-service}"
    )
    public void consumir(@Payload BoletoGeradoIntegrationEvent evento) {
        var pagamento = efetivarPagamentoUseCase.efetivar(evento);
        kafkaTemplate.send(pagamentoEfetivadoTopic, pagamento.boletoId(), pagamento);
    }
}
