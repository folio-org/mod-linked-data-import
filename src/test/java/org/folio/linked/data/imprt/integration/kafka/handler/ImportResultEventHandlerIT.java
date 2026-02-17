package org.folio.linked.data.imprt.integration.kafka.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_NAME;
import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;
import static org.folio.linked.data.imprt.test.TestUtil.awaitAndAssert;
import static org.folio.linked.data.imprt.test.TestUtil.cleanTables;
import static org.folio.linked.data.imprt.test.TestUtil.createImportResultEventDto;
import static org.folio.linked.data.imprt.test.TestUtil.sendImportResultEvent;

import java.util.Comparator;
import java.util.LinkedHashSet;
import org.folio.linked.data.imprt.domain.dto.FailedResource;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.linked.data.imprt.service.tenant.TenantScopedExecutionService;
import org.folio.linked.data.imprt.test.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
      assertThat(savedEvent.getFailedRdfLines()).hasSize(2);

      var failedLines = savedEvent.getFailedRdfLines().stream()
        .sorted(Comparator.comparing(org.folio.linked.data.imprt.model.entity.FailedRdfLine::getLineNumber))
        .toList();

      assertThat(failedLines.getFirst().getLineNumber()).isEqualTo(2L);
      assertThat(failedLines.getFirst().getFailedRdfLine()).isEqualTo("Line 2 content");
      assertThat(failedLines.getFirst().getDescription()).isEqualTo("Error 1");

      assertThat(failedLines.get(1).getLineNumber()).isEqualTo(5L);
      assertThat(failedLines.get(1).getFailedRdfLine()).isEqualTo("Line 5 content");
      assertThat(failedLines.get(1).getDescription()).isEqualTo("Error 2");
    });
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

