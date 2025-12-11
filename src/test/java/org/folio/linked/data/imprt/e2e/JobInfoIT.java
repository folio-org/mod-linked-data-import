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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    var jobId = Long.parseLong(startResult.getResponse().getContentAsString());
    awaitJobCompletion(jobId, jdbcTemplate, tenantScopedExecutionService);
    var getJobInfoRequest = get(JOBS_API_PATH + jobId)
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
    assertThat(jobInfo.getCurrentStep()).isNull();
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
    var jobId = Long.parseLong(startResult.getResponse().getContentAsString());
    awaitJobCompletion(jobId, jdbcTemplate, tenantScopedExecutionService);
    var getFailedLinesRequest = get(JOBS_API_PATH + jobId + "/failed-lines")
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
}



