package org.folio.linked.data.imprt.integration.kafka.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;
import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;
import static org.folio.linked.data.imprt.test.TestUtil.awaitAndAssert;
import static org.folio.linked.data.imprt.test.TestUtil.cleanTables;
import static org.folio.linked.data.imprt.test.TestUtil.createImportResultEventDto;
import static org.folio.linked.data.imprt.test.TestUtil.sendImportResultEvent;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.LinkedHashSet;
import org.folio.linked.data.imprt.domain.dto.FailedResource;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.linked.data.imprt.service.tenant.TenantScopedExecutionService;
import org.folio.linked.data.imprt.test.IntegrationTest;
import org.junit.jupiter.api.AfterEach;
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

  private File testFile;

  @BeforeEach
  void setUp() throws IOException {
    tenantScopedExecutionService.execute(TENANT_ID, () -> cleanTables(jdbcTemplate));

    testFile = new File(TMP_DIR, TEST_FILE_NAME);
    Files.writeString(testFile.toPath(), """
      Line 1 content
      Line 2 content
      Line 3 content
      Line 4 content
      Line 5 content
      """);
  }

  @AfterEach
  void tearDown() {
    if (testFile != null && testFile.exists()) {
      var deleted = testFile.delete();
      if (!deleted) {
        testFile.deleteOnExit();
      }
    }
  }

  @Test
  void handleImportResultEvent_shouldSaveEntityWithFailedLines() {
    // given
    var jobExecutionId = 456L;
    var event = createImportResultEventDto(jobExecutionId);

    var failedResource1 = new FailedResource(2L, "{\"resource\": \"data1\"}", "Error 1");
    var failedResource2 = new FailedResource(5L, "{\"resource\": \"data2\"}", "Error 2");
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
      assertThat(failedLines.getFirst().getFailedMappedResource()).isEqualTo("{\"resource\": \"data1\"}");

      assertThat(failedLines.get(1).getLineNumber()).isEqualTo(5L);
      assertThat(failedLines.get(1).getFailedRdfLine()).isEqualTo("Line 5 content");
      assertThat(failedLines.get(1).getDescription()).isEqualTo("Error 2");
      assertThat(failedLines.get(1).getFailedMappedResource()).isEqualTo("{\"resource\": \"data2\"}");
    });
  }

  private void createBatchJobExecutionParams(Long jobExecutionId) {
    var fileUrl = "s3://bucket/" + TEST_FILE_NAME;
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
          identifying) VALUES (?, ?, ?, ?, ?)""", jobExecutionId, FILE_URL, "java.lang.String", fileUrl, "Y"
      );
    });
  }
}

