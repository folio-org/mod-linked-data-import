package org.folio.linked.data.imprt.service.tenant;

import static java.util.stream.Collectors.toMap;
import static org.folio.spring.tools.config.RetryTemplateConfiguration.DEFAULT_KAFKA_RETRY_TEMPLATE_NAME;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.folio.spring.FolioExecutionContext;
import org.folio.spring.integration.XOkapiHeaders;
import org.folio.spring.scope.FolioExecutionContextSetter;
import org.folio.spring.tools.context.ExecutionContextBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.core.retry.Retryable;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class TenantScopedExecutionService {
  @Qualifier(DEFAULT_KAFKA_RETRY_TEMPLATE_NAME)
  private final RetryTemplate retryTemplate;
  private final ExecutionContextBuilder contextBuilder;
  @Value("${folio.okapi-url}")
  private String okapiUrl;

  @SneakyThrows
  public <T> T execute(String tenantId, Callable<T> job) {
    try (var fex = new FolioExecutionContextSetter(tenantContext(tenantId))) {
      return job.call();
    }
  }

  public <T> void executeWithRetry(Headers headers, Retryable<T> retryable, Consumer<Throwable> failureHandler) {
    try (var fex = new FolioExecutionContextSetter(kafkaFolioExecutionContext(headers))) {
      retryTemplate.execute(retryable);
    } catch (RetryException re) {
      failureHandler.accept(re.getLastException());
    }
  }

  private FolioExecutionContext kafkaFolioExecutionContext(Headers headers) {
    var headersMap = Arrays.stream(headers.toArray())
      .collect(toMap(Header::key, Header::value, (o, o2) -> o2, (Supplier<Map<String, Object>>) HashMap::new));
    return contextBuilder.forMessageHeaders(new MessageHeaders(headersMap));
  }

  private FolioExecutionContext tenantContext(String tenantId) {
    return contextBuilder.builder()
      .withTenantId(tenantId)
      .withOkapiUrl(okapiUrl)
      .withOkapiHeaders(Map.of(
          XOkapiHeaders.TENANT, List.of(tenantId),
          XOkapiHeaders.URL, List.of(okapiUrl)
        )
      )
      .build();
  }

}
