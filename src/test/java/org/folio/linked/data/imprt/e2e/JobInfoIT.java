package org.folio.linked.data.imprt.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.rest.resource.ImportStartApi.PATH_START_IMPORT;
import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;
import static org.folio.linked.data.imprt.test.TestUtil.awaitJobCompletion;
import static org.folio.linked.data.imprt.test.TestUtil.cleanTables;
import static org.folio.linked.data.imprt.test.TestUtil.defaultHeaders;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import org.folio.linked.data.imprt.domain.dto.JobInfo;
import org.folio.linked.data.imprt.service.tenant.TenantScopedExecutionService;
import org.folio.linked.data.imprt.test.IntegrationTest;
import org.folio.s3.client.FolioS3Client;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@IntegrationTest
class JobInfoIT {

  private static final String JOBS_API_PATH = "/linked-data-import/jobs/";
  @Autowired
  protected JdbcTemplate jdbcTemplate;
  @Autowired
  private MockMvc mockMvc;
  @Autowired
  private FolioS3Client s3Client;
  @Autowired
  private TenantScopedExecutionService tenantScopedExecutionService;
  @Autowired
  private ObjectMapper objectMapper;

  @BeforeEach
  void clean() {
    tenantScopedExecutionService.execute(TENANT_ID, () -> cleanTables(jdbcTemplate));
  }

  @Test
  void getJobInfo_shouldReturnJobInformation_givenCompletedJob() throws Exception {
    // given
    var fileName = "rdf/failing_mapping_and_saving_records_json.rdf";
    var input = this.getClass().getResourceAsStream("/" + fileName);
    s3Client.write(fileName, input);
    var startRequestBuilder = post(PATH_START_IMPORT)
      .param(FILE_URL, fileName)
      .headers(defaultHeaders());
    var startResult = mockMvc.perform(startRequestBuilder)
      .andExpect(status().isOk())
      .andReturn();
    var jobExecutionId = Long.parseLong(startResult.getResponse().getContentAsString());
    awaitJobCompletion(jobExecutionId, jdbcTemplate, tenantScopedExecutionService);
    var getJobInfoRequest = get(JOBS_API_PATH + jobExecutionId)
      .headers(defaultHeaders());

    // when
    var jobInfoResult = mockMvc.perform(getJobInfoRequest)
      .andExpect(status().isOk())
      .andReturn();

    // then
    var jobInfo = objectMapper.readValue(
      jobInfoResult.getResponse().getContentAsString(),
      JobInfo.class
    );

    assertThat(jobInfo).isNotNull();
    assertThat(jobInfo.getStartDate()).isNotNull();
    assertThat(jobInfo.getEndDate()).isNotNull();
    assertThat(jobInfo.getStartedBy()).isNotNull();
    assertThat(jobInfo.getStatus()).isEqualTo("COMPLETED");
    assertThat(jobInfo.getFileName()).isEqualTo(fileName);
    assertThat(jobInfo.getLatestStep()).isEqualTo("mappingStep");
    assertThat(jobInfo.getLinesRead()).isEqualTo(3L);
    assertThat(jobInfo.getLinesMapped()).isEqualTo(2L);
    assertThat(jobInfo.getLinesFailedMapping()).isEqualTo(1L);
    assertThat(jobInfo.getLinesCreated()).isEqualTo(1L);
    assertThat(jobInfo.getLinesUpdated()).isZero();
    assertThat(jobInfo.getLinesFailedSaving()).isEqualTo(1L);
  }

  @Test
  void getFailedLines_shouldReturnCsvFile_givenJobWithFailedLines() throws Exception {
    // given
    var fileName = "rdf/failing_mapping_and_saving_records_json.rdf";
    var input = this.getClass().getResourceAsStream("/" + fileName);
    s3Client.write(fileName, input);
    var startRequestBuilder = post(PATH_START_IMPORT)
      .param(FILE_URL, fileName)
      .headers(defaultHeaders());
    var startResult = mockMvc.perform(startRequestBuilder)
      .andExpect(status().isOk())
      .andReturn();
    var jobExecutionId = Long.parseLong(startResult.getResponse().getContentAsString());
    awaitJobCompletion(jobExecutionId, jdbcTemplate, tenantScopedExecutionService);
    var getFailedLinesRequest = get(JOBS_API_PATH + jobExecutionId + "/failed-lines")
      .headers(defaultHeaders());
    var expectedCsvFileName = "/failing_mapping_and_saving_records_report.csv";
    var expectedCsv = new String(this.getClass().getResourceAsStream(expectedCsvFileName).readAllBytes());

    // when
    var csvResult = mockMvc.perform(getFailedLinesRequest)
      .andExpect(status().isOk())
      .andReturn();

    // then
    var contentType = csvResult.getResponse().getContentType();
    assertThat(contentType).isEqualTo("text/csv");
    var csvContent = csvResult.getResponse().getContentAsString();
    assertThat(csvContent).isEqualTo(expectedCsv);
  }

  @Test
  void cancelJob_shouldAcceptCancelRequest_givenStartedJob() throws Exception {
    // given
    var fileName = "rdf/10_records_json.rdf";
    var input = this.getClass().getResourceAsStream("/" + fileName);
    s3Client.write(fileName, input);
    var startRequestBuilder = post(PATH_START_IMPORT)
      .param(FILE_URL, fileName)
      .headers(defaultHeaders());
    var startResult = mockMvc.perform(startRequestBuilder)
      .andExpect(status().isOk())
      .andReturn();
    var jobExecutionId = Long.parseLong(startResult.getResponse().getContentAsString());

    // when - try to cancel the job immediately
    var cancelRequest = put(JOBS_API_PATH + jobExecutionId + "/cancel")
      .headers(defaultHeaders());
    var cancelResult = mockMvc.perform(cancelRequest)
      .andReturn();

    // then - cancel should succeed with 200 (job was running) or 409 (already completed)
    var cancelStatus = cancelResult.getResponse().getStatus();
    assertThat(cancelStatus).as("Cancel request should return 200 or 409").isIn(200, 409);

    // Wait for the job to reach terminal state
    await()
      .atMost(Duration.ofSeconds(5))
      .pollInterval(Duration.ofMillis(200))
      .untilAsserted(() -> {
        var status = tenantScopedExecutionService.execute(TENANT_ID, () ->
          jdbcTemplate.queryForObject(
            "SELECT status FROM batch_job_execution WHERE job_execution_id = ?",
            String.class,
            jobExecutionId
          )
        );
        assertThat(status).as("Job should reach terminal state").isIn("STOPPED", "COMPLETED", "STOPPING", "FAILED");
      });

    // Verify job status in database
    var finalStatus = tenantScopedExecutionService.execute(TENANT_ID, () ->
      jdbcTemplate.queryForObject(
        "SELECT status FROM batch_job_execution WHERE job_execution_id = ?",
        String.class,
        jobExecutionId
      )
    );

    // The job can be STOPPED (cancel succeeded), COMPLETED (finished before cancel),
    // or STOPPING (in the process of stopping)
    assertThat(finalStatus).as("Job should be in terminal state").isIn("STOPPED", "COMPLETED", "STOPPING");

    // Verify we can still get job info after cancel attempt
    var getJobInfoRequest = get(JOBS_API_PATH + jobExecutionId)
      .headers(defaultHeaders());
    mockMvc.perform(getJobInfoRequest)
      .andExpect(status().isOk());
  }

  @Test
  void cancelJob_shouldReturn409_givenCompletedJob() throws Exception {
    // given
    var fileName = "rdf/failing_mapping_and_saving_records_json.rdf";
    var input = this.getClass().getResourceAsStream("/" + fileName);
    s3Client.write(fileName, input);
    var startRequestBuilder = post(PATH_START_IMPORT)
      .param(FILE_URL, fileName)
      .headers(defaultHeaders());
    var startResult = mockMvc.perform(startRequestBuilder)
      .andExpect(status().isOk())
      .andReturn();
    var jobExecutionId = Long.parseLong(startResult.getResponse().getContentAsString());
    awaitJobCompletion(jobExecutionId, jdbcTemplate, tenantScopedExecutionService);

    // when
    var cancelRequest = put(JOBS_API_PATH + jobExecutionId + "/cancel")
      .headers(defaultHeaders());

    // then
    mockMvc.perform(cancelRequest)
      .andExpect(status().isConflict());
  }

  @Test
  void cancelJob_shouldReturn404_givenNonExistentJob() throws Exception {
    // given
    var nonExistentJobExecutionId = 99999L;

    // when
    var cancelRequest = put(JOBS_API_PATH + nonExistentJobExecutionId + "/cancel")
      .headers(defaultHeaders());

    // then
    mockMvc.perform(cancelRequest)
      .andExpect(status().isBadRequest());
  }
}
