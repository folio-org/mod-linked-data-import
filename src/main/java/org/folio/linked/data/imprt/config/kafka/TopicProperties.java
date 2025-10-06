package org.folio.linked.data.imprt.config.kafka;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties("mod-linked-data-import.kafka.topic")
public class TopicProperties {

  private String linkedDataImportResults;

}
