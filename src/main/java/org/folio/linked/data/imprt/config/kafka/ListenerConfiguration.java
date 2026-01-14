package org.folio.linked.data.imprt.config.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.spring.tools.kafka.FolioKafkaProperties;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

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
    return concurrentKafkaBatchListenerContainerFactory(importResultEventConsumerFactory, true);
  }

  @Bean
  public ConsumerFactory<String, ImportResultEvent> importResultEventConsumerFactory(ObjectMapper mapper,
                                                                                     KafkaProperties properties) {
    return errorHandlingConsumerFactory(ImportResultEvent.class, mapper, properties);
  }


  private <V> ConcurrentKafkaListenerContainerFactory<String, V> concurrentKafkaBatchListenerContainerFactory(
    ConsumerFactory<String, V> consumerFactory, boolean batch) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, V>();
    factory.setBatchListener(batch);
    factory.setConsumerFactory(consumerFactory);
    return factory;
  }


  private <V> ConsumerFactory<String, V> errorHandlingConsumerFactory(Class<V> clazz,
                                                                      ObjectMapper mapper,
                                                                      KafkaProperties kafkaProperties) {
    var properties = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
    Supplier<Deserializer<String>> keyDeserializer = StringDeserializer::new;
    Supplier<Deserializer<V>> valueDeserializer = () ->
      new ErrorHandlingDeserializer<>(new JsonDeserializer<>(clazz, mapper));
    return new DefaultKafkaConsumerFactory<>(properties, keyDeserializer, valueDeserializer);
  }
}
