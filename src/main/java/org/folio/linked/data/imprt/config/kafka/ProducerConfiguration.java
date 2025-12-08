package org.folio.linked.data.imprt.config.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.folio.linked.data.imprt.domain.dto.ImportOutputEvent;
import org.folio.spring.tools.kafka.FolioMessageProducer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
@RequiredArgsConstructor
public class ProducerConfiguration {
  private final KafkaProperties kafkaProperties;
  private final TopicProperties topicProperties;

  @Bean
  public FolioMessageProducer<ImportOutputEvent> importOutputMessageProducer(
    KafkaTemplate<String, ImportOutputEvent> importOutputMessageTemplate) {
    var producer = new FolioMessageProducer<>(importOutputMessageTemplate, topicProperties::getLinkedDataImportOutput);
    producer.setKeyMapper(ImportOutputEvent::getTs);
    return producer;
  }

  @Bean
  public KafkaTemplate<String, ImportOutputEvent> importOutputMessageTemplate(
    ProducerFactory<String, ImportOutputEvent> importOutputMessageProducerFactory) {
    return new KafkaTemplate<>(importOutputMessageProducerFactory);
  }

  @Bean
  public ProducerFactory<String, ImportOutputEvent> importOutputMessageProducerFactory(ObjectMapper objectMapper) {
    var properties = new HashMap<>(kafkaProperties.buildProducerProperties(null));
    Supplier<Serializer<String>> keySerializer = StringSerializer::new;
    Supplier<Serializer<ImportOutputEvent>> valueSerializer = () -> new JsonSerializer<>(objectMapper);
    return new DefaultKafkaProducerFactory<>(properties, keySerializer, valueSerializer);
  }

}
