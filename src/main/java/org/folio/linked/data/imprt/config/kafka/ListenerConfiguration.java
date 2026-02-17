package org.folio.linked.data.imprt.config.kafka;

import static org.folio.linked.data.imprt.util.JsonUtil.JSON_MAPPER;

import java.util.HashMap;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.spring.tools.kafka.FolioKafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

@Configuration
public class ListenerConfiguration {

  @Bean
  @ConfigurationProperties("folio.kafka")
  public FolioKafkaProperties folioKafkaProperties() {
    return new FolioKafkaProperties();
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, ImportResultEvent> importResultEventListenerContainerFactory(
    ConsumerFactory<String, ImportResultEvent> importResultEventConsumerFactory
  ) {
    return concurrentKafkaBatchListenerContainerFactory(importResultEventConsumerFactory);
  }

  @Bean
  public ConsumerFactory<String, ImportResultEvent> importResultEventConsumerFactory(KafkaProperties properties) {
    return errorHandlingConsumerFactory(ImportResultEvent.class, properties);
  }


  private <V> ConcurrentKafkaListenerContainerFactory<String, V> concurrentKafkaBatchListenerContainerFactory(
    ConsumerFactory<String, V> consumerFactory) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, V>();
    factory.setBatchListener(true);
    factory.setConsumerFactory(consumerFactory);
    return factory;
  }


  private <V> ConsumerFactory<String, V> errorHandlingConsumerFactory(Class<V> clazz,
                                                                      KafkaProperties kafkaProperties) {
    var properties = new HashMap<>(kafkaProperties.buildConsumerProperties());
    Supplier<Deserializer<String>> keyDeserializer = StringDeserializer::new;
    Supplier<Deserializer<V>> valueDeserializer = () ->
      new ErrorHandlingDeserializer<>(new JacksonJsonDeserializer<>(clazz, JSON_MAPPER));
    return new DefaultKafkaConsumerFactory<>(properties, keyDeserializer, valueDeserializer);
  }
}
