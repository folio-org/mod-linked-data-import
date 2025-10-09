package org.folio.linked.data.imprt.config.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.StringSerializer;
import org.folio.linked.data.imprt.domain.dto.ImportResult;
import org.folio.spring.tools.kafka.FolioMessageProducer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.lang.NonNull;

@Configuration
@RequiredArgsConstructor
public class ProducerConfiguration {
  private final KafkaProperties kafkaProperties;
  private final TopicProperties topicProperties;

  @Bean
  public FolioMessageProducer<ImportResult> importResultMessageProducer(
    KafkaTemplate<String, ImportResult> importResultMessageTemplate) {
    var producer = new FolioMessageProducer<>(importResultMessageTemplate, topicProperties::getLinkedDataImportOutput);
    producer.setKeyMapper(ImportResult::getTs);
    return producer;
  }

  @Bean
  public KafkaTemplate<String, ImportResult> importResultMessageTemplate(
    ProducerFactory<String, ImportResult> importResultMessageProducerFactory) {
    return new KafkaTemplate<>(importResultMessageProducerFactory);
  }

  @Bean
  public ProducerFactory<String, ImportResult> importResultMessageProducerFactory() {
    return new DefaultKafkaProducerFactory<>(getProducerProperties());
  }

  private @NonNull Map<String, Object> getProducerProperties() {
    var configProps = new HashMap<>(kafkaProperties.buildProducerProperties(null));
    configProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configProps.put(VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    return configProps;
  }
}
