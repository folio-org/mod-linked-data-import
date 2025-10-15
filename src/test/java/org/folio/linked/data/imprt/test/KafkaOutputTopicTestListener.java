package org.folio.linked.data.imprt.test;

import static java.util.Comparator.comparing;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.folio.linked.data.imprt.test.TestUtil.awaitAndAssert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.domain.dto.ImportResult;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Getter
@Log4j2
@Component
public class KafkaOutputTopicTestListener {
  private final List<String> messages = new CopyOnWriteArrayList<>();

  @KafkaListener(topics = "folio.test_tenant.linked_data_import.output")
  public void receive(ConsumerRecord<?, ?> consumerRecord) {
    log.info("received consumerRecord = [{}]", consumerRecord.toString());
    messages.add(consumerRecord.value().toString());
  }

  public List<Resource> getImportOutputMessagesResources(int expectedSize) {
    awaitAndAssert(() ->
      assertThat(getResourcesFromMessages()).hasSize(expectedSize)
    );
    return getResourcesFromMessages();
  }

  private List<Resource> getResourcesFromMessages() {
    var om = new ObjectMapper();
    return messages.stream()
      .map(s -> {
        try {
          return om.readValue(s, ImportResult.class);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      })
      .sorted(comparing(ImportResult::getTs))
      .map(ImportResult::getResources)
      .flatMap(List::stream)
      .toList();
  }

}
