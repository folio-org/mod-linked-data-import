package org.folio.linked.data.imprt.test;

import static java.util.Comparator.comparing;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.folio.linked.data.imprt.test.TestUtil.OBJECT_MAPPER;
import static org.folio.linked.data.imprt.test.TestUtil.awaitAndAssert;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.folio.linked.data.imprt.domain.dto.ImportOutputEvent;
import org.folio.linked.data.imprt.domain.dto.ResourceWithLineNumber;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Getter
@Log4j2
@Component
@RequiredArgsConstructor
public class KafkaOutputTopicTestListener {
  private final List<String> messages = new CopyOnWriteArrayList<>();

  @KafkaListener(topics = "folio.test_tenant.linked_data_import.output")
  public void receive(ConsumerRecord<?, ?> consumerRecord) {
    log.info("received consumerRecord = [{}]", consumerRecord.toString());
    messages.add(consumerRecord.value().toString());
  }

  public List<ResourceWithLineNumber> getImportOutputMessagesResources(int expectedSize) {
    awaitAndAssert(() ->
      assertThat(getResourcesWithLineNumbers()).hasSize(expectedSize)
    );
    return getResourcesWithLineNumbers();
  }

  private List<ResourceWithLineNumber> getResourcesWithLineNumbers() {
    return messages.stream()
      .map(s -> {
        try {
          return OBJECT_MAPPER.readValue(s, ImportOutputEvent.class);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      })
      .flatMap(importOutput -> importOutput.getResourcesWithLineNumbers().stream())
      .sorted(comparing(ResourceWithLineNumber::getLineNumber))
      .toList();
  }

}
