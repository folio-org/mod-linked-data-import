package org.folio.linked.data.imprt.integration.kafka;

import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;
import static org.folio.linked.data.imprt.test.TestUtil.createConsumerRecord;
import static org.folio.linked.data.imprt.test.TestUtil.createImportResultEventDto;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.linked.data.imprt.integration.kafka.handler.KafkaMessageHandler;
import org.folio.linked.data.imprt.service.tenant.LinkedDataImportTenantService;
import org.folio.linked.data.imprt.service.tenant.TenantScopedExecutionService;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.retry.RetryContext;

@UnitTest
@ExtendWith(MockitoExtension.class)
class ImportResultEventListenerTest {

  @InjectMocks
  private ImportResultEventListener listener;

  @Mock
  private LinkedDataImportTenantService linkedDataTenantService;

  @Mock
  private TenantScopedExecutionService tenantScopedExecutionService;

  @Mock
  private KafkaMessageHandler<ImportResultEvent> importResultEventHandler;

  @Test
  void handleImportOutputEvent_shouldProcessEventForExistingTenant() {
    // given
    var event = createImportResultEventDto(123L);
    var consumerRecord = createConsumerRecord(event);
    when(linkedDataTenantService.isTenantExists(TENANT_ID)).thenReturn(true);
    doAnswer(invocation -> {
      Consumer<RetryContext> retryJob = invocation.getArgument(1);
      var retryContext = mock(RetryContext.class);
      when(retryContext.getLastThrowable()).thenReturn(null);
      retryJob.accept(retryContext);
      return null;
    }).when(tenantScopedExecutionService).executeAsyncWithRetry(any(), any(), any());

    // when
    listener.handleImportOutputEvent(consumerRecord);

    // then
    verify(tenantScopedExecutionService).executeAsyncWithRetry(any(), any(), any());
    verify(importResultEventHandler).handle(event);
  }

  @Test
  void handleImportOutputEvent_shouldNotProcessEventForNonExistingTenant() {
    // given
    var event = createImportResultEventDto(123L);
    var consumerRecord = createConsumerRecord(event);
    when(linkedDataTenantService.isTenantExists(TENANT_ID)).thenReturn(false);

    // when
    listener.handleImportOutputEvent(consumerRecord);

    // then
    verify(tenantScopedExecutionService, never()).executeAsyncWithRetry(any(), any(), any());
    verify(importResultEventHandler, never()).handle(event);
  }

  @Test
  void runRetryableJob_shouldHandleEventWithRetryContext() {
    // given
    var event = createImportResultEventDto(123L);
    var consumerRecord = createConsumerRecord(event);
    when(linkedDataTenantService.isTenantExists(TENANT_ID)).thenReturn(true);
    doAnswer(invocation -> {
      Consumer<RetryContext> retryJob = invocation.getArgument(1);
      var retryContext = mock(RetryContext.class);
      when(retryContext.getLastThrowable()).thenReturn(null);
      retryJob.accept(retryContext);
      return null;
    }).when(tenantScopedExecutionService).executeAsyncWithRetry(any(), any(), any());

    // when
    listener.handleImportOutputEvent(consumerRecord);

    // then
    verify(importResultEventHandler).handle(event);
  }

  @Test
  void runRetryableJob_shouldLogFailedEventOnRetry() {
    // given
    var event = createImportResultEventDto(123L);
    var consumerRecord = createConsumerRecord(event);
    when(linkedDataTenantService.isTenantExists(TENANT_ID)).thenReturn(true);
    doAnswer(invocation -> {
      Consumer<RetryContext> retryJob = invocation.getArgument(1);
      var retryContext = mock(RetryContext.class);
      var exception = new RuntimeException("Test exception");
      when(retryContext.getLastThrowable()).thenReturn(exception);
      retryJob.accept(retryContext);
      return null;
    }).when(tenantScopedExecutionService).executeAsyncWithRetry(any(), any(), any());

    // when
    listener.handleImportOutputEvent(consumerRecord);

    // then
    verify(importResultEventHandler).handle(event);
  }

  @Test
  void handleImportOutputEvent_shouldInvokeErrorHandlerOnFailure() {
    // given
    var event = createImportResultEventDto(123L);
    var consumerRecord = createConsumerRecord(event);
    when(linkedDataTenantService.isTenantExists(TENANT_ID)).thenReturn(true);
    doAnswer(invocation -> {
      Consumer<Throwable> errorHandler = invocation.getArgument(2);
      var exception = new RuntimeException("Processing failed");
      errorHandler.accept(exception);
      return null;
    }).when(tenantScopedExecutionService).executeAsyncWithRetry(any(), any(), any());

    // when
    listener.handleImportOutputEvent(consumerRecord);

    // then
    verify(tenantScopedExecutionService).executeAsyncWithRetry(any(), any(), any());
  }
}

