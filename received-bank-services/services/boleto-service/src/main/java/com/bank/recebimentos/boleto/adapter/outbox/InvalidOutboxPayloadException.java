package com.bank.recebimentos.boleto.adapter.outbox;

public class InvalidOutboxPayloadException extends RuntimeException {
    public InvalidOutboxPayloadException(String message) {
        super(message);
    }

    public InvalidOutboxPayloadException(String message, Throwable cause) {
        super(message, cause);
    }
}
