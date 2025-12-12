package org.folio.linked.data.imprt.integration.kafka;

import static java.util.Optional.ofNullable;
import static org.folio.linked.data.imprt.util.KafkaUtils.handleForExistedTenant;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.Level;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.linked.data.imprt.integration.kafka.handler.KafkaMessageHandler;
import org.folio.linked.data.imprt.service.tenant.LinkedDataImportTenantService;
import org.folio.linked.data.imprt.service.tenant.TenantScopedExecutionService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.retry.RetryContext;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class ImportResultEventListener {

  private static final String LISTENER_ID = "mod-linked-data-import-import-result-event-listener";
  private static final String CONTAINER_FACTORY = "importResultEventListenerContainerFactory";
  private final LinkedDataImportTenantService linkedDataTenantService;
  private final TenantScopedExecutionService tenantScopedExecutionService;
  private final KafkaMessageHandler<ImportResultEvent> importResultEventHandler;

  @KafkaListener(
    id = LISTENER_ID,
    containerFactory = CONTAINER_FACTORY,
    groupId = "#{folioKafkaProperties.listener['import-result-event'].groupId}",
    concurrency = "#{folioKafkaProperties.listener['import-result-event'].concurrency}",
    topicPattern = "#{folioKafkaProperties.listener['import-result-event'].topicPattern}")
  public void handleImportOutputEvent(ConsumerRecord<String, ImportResultEvent> consumerRecord) {
    var event = consumerRecord.value();
    handleForExistedTenant(consumerRecord, event.getTs(), linkedDataTenantService, log, this::handleRecord);
  }

  private void handleRecord(ConsumerRecord<String, ImportResultEvent> consumerRecord) {
    log.info("Processing import result event with Job ID {} and ts {}",
      consumerRecord.value().getJobInstanceId(), consumerRecord.value().getTs());
    var event = consumerRecord.value();
    tenantScopedExecutionService.executeAsyncWithRetry(
      consumerRecord.headers(),
      retryContext -> runRetryableJob(event, retryContext),
      ex -> logFailedEvent(event, ex, false)
    );
  }

  private void runRetryableJob(ImportResultEvent event, RetryContext retryContext) {
    ofNullable(retryContext.getLastThrowable())
      .ifPresent(ex -> logFailedEvent(event, ex, true));
    importResultEventHandler.handle(event);
  }

  private void logFailedEvent(ImportResultEvent event, Throwable ex, boolean isRetrying) {
    var logLevel = isRetrying ? Level.INFO : Level.ERROR;
    log.log(logLevel, "Failed to handle import result event with id {}. Retrying: {}",
      event.getTs(), isRetrying, ex);
  }
}
