package org.folio.linked.data.imprt.integration.kafka.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_NAME;
import static org.folio.linked.data.imprt.test.TestUtil.STUB_DATE;
import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;
import static org.folio.linked.data.imprt.test.TestUtil.awaitAndAssert;
import static org.folio.linked.data.imprt.test.TestUtil.cleanTables;
import static org.folio.linked.data.imprt.test.TestUtil.createImportResultEventDto;
import static org.folio.linked.data.imprt.test.TestUtil.sendImportResultEvent;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.stream.Stream;
import org.folio.linked.data.imprt.domain.dto.FailedResource;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.linked.data.imprt.service.tenant.TenantScopedExecutionService;
import org.folio.linked.data.imprt.test.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

@IntegrationTest
class ImportResultEventHandlerIT {

  private static final String TEST_FILE_NAME = "test-rdf-file.txt";

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private KafkaTemplate<String, ImportResultEvent> importResultEventProducer;

  @Autowired
  private ImportResultEventRepo importResultEventRepo;

  @Autowired
  private TenantScopedExecutionService tenantScopedExecutionService;

  @BeforeEach
  void setUp() {
    tenantScopedExecutionService.execute(TENANT_ID, () -> {
      cleanTables(jdbcTemplate);
      return null;
    });
  }

  @Test
  void handleImportResultEvent_shouldSaveEntityWithFailedLines() {
    // given
    var jobExecutionId = 456L;
    var event = createImportResultEventDto(jobExecutionId);

    var failedResource1 = new FailedResource(2L, "Error 1");
    var failedResource2 = new FailedResource(5L, "Error 2");
    var failedResources = new LinkedHashSet<FailedResource>();
    failedResources.add(failedResource1);
    failedResources.add(failedResource2);
    event.setFailedResources(failedResources);

    createBatchJobExecutionParams(jobExecutionId);

    // when
    sendImportResultEvent(event, importResultEventProducer);

    // then
    awaitAndAssert(() -> {
      var savedEvents = tenantScopedExecutionService.execute(TENANT_ID,
        () -> importResultEventRepo.findAll());
      assertThat(savedEvents).hasSize(1);

      var savedEvent = savedEvents.getFirst();
      assertThat(savedEvent.getResourcesCount()).isEqualTo(10);
      assertThat(savedEvent.getCreatedCount()).isEqualTo(8);
      assertThat(savedEvent.getUpdatedCount()).isEqualTo(2);
      assertThat(savedEvent.getOriginalEventTs()).isEqualTo("original-ts");
      assertThat(savedEvent.getEventTs()).isEqualTo("event-ts");
      assertThat(savedEvent.getFailedRdfLines()).hasSize(2);

      var failedLines = savedEvent.getFailedRdfLines().stream()
        .sorted(Comparator.comparing(org.folio.linked.data.imprt.model.entity.FailedRdfLine::getLineNumber))
        .toList();

      assertThat(failedLines.getFirst().getLineNumber()).isEqualTo(2L);
      assertThat(failedLines.getFirst().getFailedRdfLine()).isEqualTo("Line 2 content");
      assertThat(failedLines.getFirst().getDescription()).isEqualTo("Error 1");
      assertThat(failedLines.getFirst().getJobExecutionId()).isEqualTo(456L);

      assertThat(failedLines.get(1).getLineNumber()).isEqualTo(5L);
      assertThat(failedLines.get(1).getFailedRdfLine()).isEqualTo("Line 5 content");
      assertThat(failedLines.get(1).getDescription()).isEqualTo("Error 2");
      assertThat(failedLines.get(1).getJobExecutionId()).isEqualTo(456L);
    });
  }

  @ParameterizedTest
  @MethodSource("countCombinations")
  void handleImportResultEvent_shouldSaveEntityWithCorrectCounts(int createdCount, int updatedCount) {
    // given
    var jobExecutionId = 789L;
    var event = new ImportResultEvent(
      "original-ts",
      jobExecutionId,
      FIXED_DATE,
      FIXED_DATE,
      createdCount + updatedCount,
      createdCount,
      updatedCount
    );
    event.setTs("event-ts");
    event.setTenant(TENANT_ID);

    createBatchJobExecutionParams(jobExecutionId);

    // when
    sendImportResultEvent(event, importResultEventProducer);

    // then
    awaitAndAssert(() -> {
      var savedEvents = tenantScopedExecutionService.execute(TENANT_ID,
        () -> importResultEventRepo.findAll());
      assertThat(savedEvents).hasSize(1);

      var savedEvent = savedEvents.getFirst();
      assertThat(savedEvent.getResourcesCount()).isEqualTo(createdCount + updatedCount);
      assertThat(savedEvent.getCreatedCount()).isEqualTo(createdCount);
      assertThat(savedEvent.getUpdatedCount()).isEqualTo(updatedCount);
      assertThat(savedEvent.getOriginalEventTs()).isEqualTo("original-ts");
      assertThat(savedEvent.getEventTs()).isEqualTo("event-ts");
      assertThat(savedEvent.getFailedRdfLines()).isEmpty();
    });
  }

  @Test
  void handleImportResultEvent_shouldSaveFallbackContent_givenMissingRdfFileLine() {
    // given
    var jobExecutionId = 111L;
    var event = new ImportResultEvent(
      "original-ts",
      jobExecutionId,
      FIXED_DATE,
      FIXED_DATE,
      5,
      4,
      0
    );
    event.setTs("event-ts");
    event.setTenant(TENANT_ID);

    var failedResources = new LinkedHashSet<FailedResource>();
    failedResources.add(new FailedResource(8L, "Line not in DB"));
    event.setFailedResources(failedResources);

    createBatchJobExecutionParams(jobExecutionId);

    // when
    sendImportResultEvent(event, importResultEventProducer);

    // then
    awaitAndAssert(() -> {
      var savedEvents = tenantScopedExecutionService.execute(TENANT_ID,
        () -> importResultEventRepo.findAll());
      assertThat(savedEvents).hasSize(1);

      var savedEvent = savedEvents.getFirst();
      assertThat(savedEvent.getFailedRdfLines()).hasSize(1);

      var failedLine = savedEvent.getFailedRdfLines().iterator().next();
      assertThat(failedLine.getLineNumber()).isEqualTo(8L);
      assertThat(failedLine.getDescription()).isEqualTo("Line not in DB");
      assertThat(failedLine.getFailedRdfLine())
        .isEqualTo("Line number 8 not found in database for jobExecutionId 111");
    });
  }

  @Test
  void handleImportResultEvent_shouldSaveMultipleEvents_givenSameJobExecutionId() {
    // given
    var jobExecutionId = 222L;
    createBatchJobExecutionParams(jobExecutionId);

    var event1 = new ImportResultEvent(
      "original-ts",
      jobExecutionId,
      FIXED_DATE,
      FIXED_DATE,
      3,
      3,
      0
    );
    event1.setTs("event-ts-1");
    event1.setTenant(TENANT_ID);

    var event2 = new ImportResultEvent(
      "original-ts",
      jobExecutionId,
      FIXED_DATE,
      FIXED_DATE,
      3,
      2,
      1
    );
    event2.setTs("event-ts-2");
    event2.setTenant(TENANT_ID);

    // when
    sendImportResultEvent(event1, importResultEventProducer);
    sendImportResultEvent(event2, importResultEventProducer);

    // then
    awaitAndAssert(() -> {
      var savedEvents = tenantScopedExecutionService.execute(TENANT_ID,
        () -> importResultEventRepo.findAll());
      assertThat(savedEvents).hasSize(2);

      var totalCreated = savedEvents.stream()
        .mapToInt(org.folio.linked.data.imprt.model.entity.ImportResultEvent::getCreatedCount)
        .sum();
      assertThat(totalCreated).isEqualTo(5);

      var totalUpdated = savedEvents.stream()
        .mapToInt(org.folio.linked.data.imprt.model.entity.ImportResultEvent::getUpdatedCount)
        .sum();
      assertThat(totalUpdated).isEqualTo(1);
    });
  }

  @ParameterizedTest
  @MethodSource("countCombinationsWithFailures")
  void handleImportResultEvent_shouldSaveEntityWithMixedSuccessAndFailure(
    long jobExecutionId, int createdCount, int updatedCount, int failedCount) {
    // given
    var event = new ImportResultEvent(
      "original-ts",
      jobExecutionId,
      FIXED_DATE,
      FIXED_DATE,
      createdCount + updatedCount + failedCount,
      createdCount,
      updatedCount
    );
    event.setTs("event-ts");
    event.setTenant(TENANT_ID);

    var failedResources = new LinkedHashSet<FailedResource>();
    for (int i = 1; i <= failedCount; i++) {
      failedResources.add(new FailedResource((long) i, "Error for line " + i));
    }
    event.setFailedResources(failedResources);

    createBatchJobExecutionParams(jobExecutionId);

    // when
    sendImportResultEvent(event, importResultEventProducer);

    // then
    awaitAndAssert(() -> {
      var savedEvents = tenantScopedExecutionService.execute(TENANT_ID,
        () -> importResultEventRepo.findAll());
      assertThat(savedEvents).hasSize(1);

      var savedEvent = savedEvents.getFirst();
      assertThat(savedEvent.getResourcesCount()).isEqualTo(createdCount + updatedCount + failedCount);
      assertThat(savedEvent.getCreatedCount()).isEqualTo(createdCount);
      assertThat(savedEvent.getUpdatedCount()).isEqualTo(updatedCount);
      assertThat(savedEvent.getOriginalEventTs()).isEqualTo("original-ts");
      assertThat(savedEvent.getEventTs()).isEqualTo("event-ts");
      assertThat(savedEvent.getFailedRdfLines()).hasSize(failedCount);

      var failedLines = savedEvent.getFailedRdfLines().stream()
        .sorted(Comparator.comparing(org.folio.linked.data.imprt.model.entity.FailedRdfLine::getLineNumber))
        .toList();

      for (int i = 0; i < failedCount; i++) {
        assertThat(failedLines.get(i).getLineNumber()).isEqualTo((long) (i + 1));
        assertThat(failedLines.get(i).getFailedRdfLine()).isEqualTo("Line " + (i + 1) + " content");
        assertThat(failedLines.get(i).getDescription()).isEqualTo("Error for line " + (i + 1));
        assertThat(failedLines.get(i).getJobExecutionId()).isEqualTo(jobExecutionId);
      }
    });
  }

  static Stream<Arguments> countCombinations() {
    return Stream.of(
      Arguments.of(10, 0),   // CREATE_INSTANCE only
      Arguments.of(0, 10),   // UPDATE_INSTANCE only
      Arguments.of(5, 5)     // mixed
    );
  }

  static Stream<Arguments> countCombinationsWithFailures() {
    return Stream.of(
      Arguments.of(333L, 2, 0, 1),  // create-dominant batch with failures
      Arguments.of(334L, 0, 2, 1),  // update-dominant batch with failures
      Arguments.of(335L, 1, 1, 1),  // mixed create+update with failures
      Arguments.of(336L, 0, 0, 2)   // all-failed batch
    );
  }

  private void createBatchJobExecutionParams(Long jobExecutionId) {
    var fileName = "s3://bucket/" + TEST_FILE_NAME;
    tenantScopedExecutionService.execute(TENANT_ID, () -> {
      jdbcTemplate.update("""
        INSERT INTO batch_job_instance (job_instance_id, job_name, job_key, version) \
        VALUES (?, 'testJob', 'testKey', 0)""", jobExecutionId
      );

      jdbcTemplate.update(
        """
          INSERT INTO batch_job_execution (job_execution_id, job_instance_id, create_time, start_time, status, \
          version) VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'STARTED', 0)""", jobExecutionId, jobExecutionId
      );

      jdbcTemplate.update(
        """
          INSERT INTO batch_job_execution_params (job_execution_id, parameter_name, parameter_type, parameter_value, \
          identifying) VALUES (?, ?, ?, ?, ?)""", jobExecutionId, FILE_NAME, "java.lang.String", fileName, "Y"
      );

      for (long i = 1; i <= 5; i++) {
        jdbcTemplate.update(
          "INSERT INTO rdf_file_line (id, job_execution_id, line_number, content) "
            + "VALUES (nextval('rdf_file_line_seq'), ?, ?, ?)", jobExecutionId, i, "Line " + i + " content"
        );
      }

      return null;
    });
  }
}

