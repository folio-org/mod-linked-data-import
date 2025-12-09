package org.folio.linked.data.imprt.e2e;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;
import static org.folio.linked.data.imprt.rest.resource.ImportStartApi.PATH_START_IMPORT;
import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;
import static org.folio.linked.data.imprt.test.TestUtil.awaitAndAssert;
import static org.folio.linked.data.imprt.test.TestUtil.cleanTables;
import static org.folio.linked.data.imprt.test.TestUtil.defaultHeaders;
import static org.springframework.data.domain.Sort.by;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.File;
import org.folio.linked.data.imprt.model.entity.ImportResultEvent;
import org.folio.linked.data.imprt.repo.FailedRdfLineRepo;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.linked.data.imprt.service.tenant.TenantScopedExecutionService;
import org.folio.linked.data.imprt.test.IntegrationTest;
import org.folio.s3.client.FolioS3Client;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@IntegrationTest
class ImportIT {

  @Autowired
  protected JdbcTemplate jdbcTemplate;
  @Autowired
  private MockMvc mockMvc;
  @Autowired
  private FolioS3Client s3Client;
  @Autowired
  private TenantScopedExecutionService tenantScopedExecutionService;
  @Autowired
  private FailedRdfLineRepo failedRdfLineRepo;
  @Autowired
  private ImportResultEventRepo importResultEventRepo;

  @BeforeEach
  void clean() {
    tenantScopedExecutionService.execute(TENANT_ID, () -> cleanTables(jdbcTemplate));
  }

  @Test
  void checkSuccessfullyMappedLines_local() throws Exception {
    // given
    var fileName = "10_records_json.rdf";
    var input = this.getClass().getResourceAsStream("/" + fileName);
    s3Client.write(fileName, input);
    var requestBuilder = post(PATH_START_IMPORT)
      .param(FILE_URL, fileName)
      .headers(defaultHeaders());

    // when
    var resultActions = mockMvc.perform(requestBuilder);

    // then
    var result = resultActions.andExpect(status().isOk())
      .andReturn();
    var jobInstanceId = Long.parseLong(result.getResponse().getContentAsString());

    awaitJobCompletion(jobInstanceId);
    awaitAndAssert(() -> assertThat(new File(TMP_DIR, fileName)).doesNotExist());

    var importResultEvents = tenantScopedExecutionService.execute(TENANT_ID,
      () -> importResultEventRepo.findAll());

    var totalResourcesCount = importResultEvents.stream()
      .mapToInt(ImportResultEvent::getResourcesCount)
      .sum();
    assertThat(totalResourcesCount).isEqualTo(10);

    var totalCreatedCount = importResultEvents.stream()
      .mapToInt(ImportResultEvent::getCreatedCount)
      .sum();
    assertThat(totalCreatedCount).isEqualTo(10);
  }

  @Test
  void checkSuccessfullyMappedLines_usingSearchAndLinkedDataAndSrs() throws Exception {
    // given
    var fileName = "2_records_lccn_json.rdf";
    var input = this.getClass().getResourceAsStream("/" + fileName);
    s3Client.write(fileName, input);
    var requestBuilder = post(PATH_START_IMPORT)
      .param(FILE_URL, fileName)
      .headers(defaultHeaders());

    // when
    var resultActions = mockMvc.perform(requestBuilder);

    // then
    var result = resultActions.andExpect(status().isOk())
      .andReturn();
    var jobInstanceId = Long.parseLong(result.getResponse().getContentAsString());

    awaitJobCompletion(jobInstanceId);
    awaitAndAssert(() -> assertThat(new File(TMP_DIR, fileName)).doesNotExist());

    var importResultEvents = tenantScopedExecutionService.execute(TENANT_ID,
      () -> importResultEventRepo.findAll());

    var totalResourcesCount = importResultEvents.stream()
      .mapToInt(ImportResultEvent::getResourcesCount)
      .sum();
    assertThat(totalResourcesCount).isEqualTo(2);
  }

  @Test
  void checkPartiallyFailingMappingLines() throws Exception {
    // given
    var fileName = "failing_mapping_records_json.rdf";
    var input = this.getClass().getResourceAsStream("/" + fileName);
    s3Client.write(fileName, input);
    var requestBuilder = post(PATH_START_IMPORT)
      .param(FILE_URL, fileName)
      .headers(defaultHeaders());

    // when
    var resultActions = mockMvc.perform(requestBuilder);

    // then
    var result = resultActions.andExpect(status().isOk())
      .andReturn();
    var jobInstanceId = Long.parseLong(result.getResponse().getContentAsString());

    awaitJobCompletion(jobInstanceId);
    awaitAndAssert(() -> assertThat(new File(TMP_DIR, fileName)).doesNotExist());

    var importResultEvents = tenantScopedExecutionService.execute(TENANT_ID,
      () -> importResultEventRepo.findAll());

    var processedResourcesCount = importResultEvents.stream()
      .mapToInt(ImportResultEvent::getResourcesCount)
      .sum();
    assertThat(processedResourcesCount).isEqualTo(1);

    var failedRdfLines = tenantScopedExecutionService.execute(TENANT_ID,
      () -> failedRdfLineRepo.findAll(by("lineNumber")));
    assertThat(failedRdfLines).hasSize(4);

    for (int i = 1; i <= 5; i++) {
      if (i == 3) {
        continue;
      }
      var failedLine = failedRdfLines.get(i - (i < 3 ? 1 : 2));
      assertThat(failedLine.getLineNumber()).isEqualTo(i);
      assertThat(failedLine.getJobInstanceId()).isEqualTo(jobInstanceId);
      assertThat(failedLine.getFailedRdfLine()).isEqualTo("[{failing line " + i + "}]");
      assertThat(failedLine.getDescription()).isEqualTo("RDF parsing error");
    }
  }

  @Test
  void checkPartiallyFailingSavingLines() throws Exception {
    // given
    var fileName = "failing_saving_records_json.rdf";
    var input = this.getClass().getResourceAsStream("/" + fileName);
    s3Client.write(fileName, input);
    var requestBuilder = post(PATH_START_IMPORT)
      .param(FILE_URL, fileName)
      .headers(defaultHeaders());

    // when
    var resultActions = mockMvc.perform(requestBuilder);

    // then
    var result = resultActions.andExpect(status().isOk())
      .andReturn();
    var jobInstanceId = Long.parseLong(result.getResponse().getContentAsString());

    awaitJobCompletion(jobInstanceId);
    awaitAndAssert(() -> assertThat(new File(TMP_DIR, fileName)).doesNotExist());

    var importResultEvents = tenantScopedExecutionService.execute(TENANT_ID,
      () -> importResultEventRepo.findAll());

    var processedSavingResourcesCount = importResultEvents.stream()
      .mapToInt(ImportResultEvent::getResourcesCount)
      .sum();
    assertThat(processedSavingResourcesCount).isEqualTo(3);

    var failedSavingResourcesCount = importResultEvents.stream()
      .mapToInt(importResultEvent -> importResultEvent.getFailedRdfLines().size())
      .sum();
    assertThat(failedSavingResourcesCount).isEqualTo(1);

    var failedRdfLines = tenantScopedExecutionService.execute(TENANT_ID,
      () -> failedRdfLineRepo.findAll(by("lineNumber")));
    assertThat(failedRdfLines).hasSize(failedSavingResourcesCount);
    var failedRdfLine = failedRdfLines.getFirst();

    assertThat(failedRdfLine.getJobInstanceId()).isEqualTo(jobInstanceId);
    assertThat(failedRdfLine.getLineNumber()).isEqualTo(2L);
    assertThat(failedRdfLine.getDescription()).isEqualTo("Failed because title = FAIL_SAVING_LINE");
    assertThat(failedRdfLine.getLineNumber()).isEqualTo(2L);
    assertThat(failedRdfLine.getFailedRdfLine()).isEqualTo("""
      [{"@id":"http://test-tobe-changed.folio.com/resources/INSTANCE_ID",\
      "@type":["http://id.loc.gov/ontologies/bibframe/Instance"],\
      "http://id.loc.gov/ontologies/bibframe/title":\
      [{"@id":"http://test-tobe-changed.folio.com/resources/PRIMARY_TITLE_ID"}]},\
      {"@id":"http://test-tobe-changed.folio.com/resources/PRIMARY_TITLE_ID",\
      "@type":["http://id.loc.gov/ontologies/bibframe/Title"],\
      "http://id.loc.gov/ontologies/bibframe/mainTitle":[{"@value":"FAIL_SAVING_LINE"}]}]""");
    assertThat(failedRdfLine.getFailedMappedResource()).isNotNull();
  }

  private void awaitJobCompletion(Long jobInstanceId) {
    awaitAndAssert(() -> {
      var status = tenantScopedExecutionService.execute(TENANT_ID, () ->
        jdbcTemplate.queryForObject(
          """
            SELECT e.status FROM batch_job_execution e
            WHERE e.job_instance_id = ?
            ORDER BY e.create_time DESC
            LIMIT 1
            """,
          String.class,
          jobInstanceId
        )
      );
      assertThat(status).isIn(BatchStatus.COMPLETED.name(), BatchStatus.FAILED.name());
    });
  }

}
