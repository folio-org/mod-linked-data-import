package org.folio.linked.data.imprt.test;

import static java.lang.System.getProperty;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.folio.spring.integration.XOkapiHeaders.TENANT;
import static org.folio.spring.integration.XOkapiHeaders.URL;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.awaitility.Durations.FIVE_SECONDS;
import static org.testcontainers.shaded.org.awaitility.Durations.ONE_HUNDRED_MILLISECONDS;
import static org.testcontainers.shaded.org.awaitility.Durations.TWO_MINUTES;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.linked.data.imprt.config.ObjectMapperConfig;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.linked.data.imprt.service.tenant.TenantScopedExecutionService;
import org.folio.s3.client.FolioS3Client;
import org.springframework.batch.core.BatchStatus;
import org.springframework.http.HttpHeaders;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;
import org.testcontainers.shaded.org.awaitility.core.ThrowingRunnable;

@UtilityClass
public class TestUtil {

  public static final String TENANT_ID = "test_tenant";
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapperConfig().objectMapper();
  private static final String FOLIO_OKAPI_URL = "folio.okapi-url";
  private static final String IMPORT_RESULT_TOPIC = "folio.test_tenant.linked_data_import.result";

  @SneakyThrows
  public static String asJsonString(Object value) {
    return OBJECT_MAPPER.writeValueAsString(value);
  }

  public static HttpHeaders defaultHeaders() {
    var httpHeaders = new HttpHeaders();
    httpHeaders.add(TENANT, TENANT_ID);
    httpHeaders.add(URL, getProperty(FOLIO_OKAPI_URL));
    return httpHeaders;
  }

  public static void cleanTables(JdbcTemplate jdbcTemplate) {
    JdbcTestUtils.deleteFromTables(jdbcTemplate, "batch_step_execution_context",
      "batch_step_execution",
      "batch_job_execution_params",
      "batch_job_execution_context",
      "batch_job_execution",
      "batch_job_instance",
      "failed_rdf_line",
      "import_result_event");
  }

  public static void awaitAndAssert(ThrowingRunnable throwingRunnable) {
    await().atMost(TWO_MINUTES)
      .pollDelay(FIVE_SECONDS)
      .pollInterval(ONE_HUNDRED_MILLISECONDS)
      .untilAsserted(throwingRunnable);
  }

  public static ImportResultEvent createImportResultEventDto(Long jobExecutionId) {
    var event = new ImportResultEvent(
      "original-ts",
      jobExecutionId,
      java.time.OffsetDateTime.now(),
      java.time.OffsetDateTime.now(),
      10,
      8,
      2
    );
    event.setTs("event-ts");
    event.setTenant(TENANT_ID);
    return event;
  }

  public static org.folio.linked.data.imprt.model.entity.ImportResultEvent createImportResultEvent(Long executionId) {
    return new org.folio.linked.data.imprt.model.entity.ImportResultEvent()
      .setJobExecutionId(executionId)
      .setResourcesCount(10)
      .setCreatedCount(8)
      .setUpdatedCount(2)
      .setStartDate(java.time.OffsetDateTime.now())
      .setEndDate(java.time.OffsetDateTime.now())
      .setOriginalEventTs("original-ts")
      .setEventTs("event-ts");
  }

  public static ConsumerRecord<String, ImportResultEvent> createConsumerRecord(ImportResultEvent event) {
    return createConsumerRecord(event, TENANT_ID);
  }

  public static ConsumerRecord<String, ImportResultEvent> createConsumerRecord(ImportResultEvent event, String tenant) {
    var consumerRecord = new ConsumerRecord<>("test-topic", 0, 0L, "key", event);
    consumerRecord.headers().add(new RecordHeader(TENANT, tenant.getBytes()));
    return consumerRecord;
  }

  public static void sendImportResultEvent(ImportResultEvent event,
                                           KafkaTemplate<String, ImportResultEvent> importResultEventProducer) {
    var producerRecord = new ProducerRecord<String, ImportResultEvent>(IMPORT_RESULT_TOPIC, event);
    producerRecord.headers().add(new RecordHeader(TENANT, TENANT_ID.getBytes()));
    importResultEventProducer.send(producerRecord);
  }

  public static void awaitJobCompletion(Long jobExecutionId, JdbcTemplate jdbcTemplate,
                                        TenantScopedExecutionService tenantScopedExecutionService) {
    awaitAndAssert(() -> {
      var status = tenantScopedExecutionService.execute(TENANT_ID, () ->
        jdbcTemplate.queryForObject(
          """
            SELECT e.status FROM batch_job_execution e
            WHERE e.job_execution_id = ?
            """,
          String.class,
          jobExecutionId
        )
      );
      assertThat(status).isIn(BatchStatus.COMPLETED.name(),
        BatchStatus.FAILED.name());
    });
  }

  public static void writeFileToS3(FolioS3Client s3Client, String fileName, InputStream is) {
    s3Client.write(TENANT_ID + "/" + fileName, is);
  }

}
