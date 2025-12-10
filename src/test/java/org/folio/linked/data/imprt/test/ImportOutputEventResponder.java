package org.folio.linked.data.imprt.test;

import static org.folio.linked.data.imprt.test.TestUtil.OBJECT_MAPPER;
import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;
import static org.folio.linked.data.imprt.test.TestUtil.sendImportResultEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.OffsetDateTime;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.folio.linked.data.imprt.domain.dto.FailedResource;
import org.folio.linked.data.imprt.domain.dto.ImportOutputEvent;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Getter
@Log4j2
@Component
@RequiredArgsConstructor
public class ImportOutputEventResponder {

  @Autowired
  private KafkaTemplate<String, ImportResultEvent> importResultEventProducer;

  @KafkaListener(topics = "folio.test_tenant.linked_data_import.output", groupId = "test-import-output-responder")
  public void receive(ConsumerRecord<?, ?> consumerRecord) {
    log.info("received consumerRecord = [{}]", consumerRecord.toString());
    var messageValue = consumerRecord.value().toString();
    try {
      var outputEvent = OBJECT_MAPPER.readValue(messageValue, ImportOutputEvent.class);
      respondWithImportResultEvent(outputEvent);
    } catch (JsonProcessingException e) {
      log.error("Failed to parse ImportOutputEvent", e);
    }
  }

  private void respondWithImportResultEvent(ImportOutputEvent outputEvent) {
    var resourcesCount = outputEvent.getResourcesWithLineNumbers() != null
      ? outputEvent.getResourcesWithLineNumbers().size()
      : 0;

    var failedResources = outputEvent.getResourcesWithLineNumbers().stream()
      .filter(rwl -> rwl.getResource().getLabel().contains("FAIL_SAVING_LINE"))
      .map(rwl -> new FailedResource(
        rwl.getLineNumber(),
        rwl.getResource().toString(),
        "Failed because title = FAIL_SAVING_LINE")
      )
      .collect(Collectors.toSet());

    var resultEvent = new ImportResultEvent(
      outputEvent.getTs(),
      outputEvent.getJobInstanceId(),
      OffsetDateTime.now(),
      OffsetDateTime.now(),
      resourcesCount,
      resourcesCount - failedResources.size(),
      0
    );
    resultEvent.setTs(OffsetDateTime.now().toString());
    resultEvent.setTenant(TENANT_ID);
    resultEvent.setFailedResources(failedResources);

    sendImportResultEvent(resultEvent, importResultEventProducer);

    log.info("Sent ImportResultEvent for jobInstanceId={} with resourcesCount={}",
      outputEvent.getJobInstanceId(), resourcesCount);
  }

}
