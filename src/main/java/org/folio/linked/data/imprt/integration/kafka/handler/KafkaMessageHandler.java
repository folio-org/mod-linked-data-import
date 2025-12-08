package org.folio.linked.data.imprt.integration.kafka.handler;

public interface KafkaMessageHandler<T> {

  void handle(T message);
}
