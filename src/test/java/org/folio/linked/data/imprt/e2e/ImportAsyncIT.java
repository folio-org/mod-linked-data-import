package org.folio.linked.data.imprt.e2e;

import static java.time.Duration.between;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.CONTENT_TYPE;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_NAME;
import static org.folio.linked.data.imprt.rest.resource.ImportStartApi.PATH_START_IMPORT;
import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;
import static org.folio.linked.data.imprt.test.TestUtil.awaitJobCompletion;
import static org.folio.linked.data.imprt.test.TestUtil.cleanTables;
import static org.folio.linked.data.imprt.test.TestUtil.defaultHeaders;
import static org.folio.linked.data.imprt.test.TestUtil.writeFileToS3;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.Set;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.batch.job.processor.Rdf2LdProcessor;
import org.folio.linked.data.imprt.domain.dto.ResourceWithLineNumber;
import org.folio.linked.data.imprt.model.RdfLineWithNumber;
import org.folio.linked.data.imprt.repo.FailedRdfLineRepo;
import org.folio.linked.data.imprt.service.tenant.TenantScopedExecutionService;
import org.folio.linked.data.imprt.test.IntegrationTest;
import org.folio.rdf4ld.service.Rdf4LdService;
import org.folio.s3.client.FolioS3Client;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@IntegrationTest
@Log4j2
class ImportAsyncIT {

  private static final long DELAY_MS = 3000L;

  @Autowired
  protected JdbcTemplate jdbcTemplate;
  @Autowired
  private MockMvc mockMvc;
  @Autowired
  private FolioS3Client s3Client;
  @Autowired
  private TenantScopedExecutionService tenantScopedExecutionService;

  @BeforeEach
  void clean() {
    tenantScopedExecutionService.execute(TENANT_ID, () -> {
      cleanTables(jdbcTemplate);
      return null;
    });
  }

  @Test
  void checkImportStartsAsynchronously() throws Exception {
    // given
    var fileName = "10_records_json.rdf";
    var input = this.getClass().getResourceAsStream("/rdf/" + fileName);
    writeFileToS3(s3Client, fileName, input);
    var requestBuilder = post(PATH_START_IMPORT)
      .param(FILE_NAME, fileName)
      .headers(defaultHeaders());

    // when
    var startTime = Instant.now();
    var resultActions = mockMvc.perform(requestBuilder);
    var responseTime = between(startTime, Instant.now());

    // then
    var result = resultActions.andExpect(status().isOk()).andReturn();
    assertThat(responseTime).isLessThan(ofSeconds(1));

    // wait for the async job to complete to avoid polluting other tests
    var jobExecutionId = Long.parseLong(result.getResponse().getContentAsString());
    awaitJobCompletion(jobExecutionId, jdbcTemplate, tenantScopedExecutionService);
  }

  @TestConfiguration
  static class AsyncTestConfig {

    @Bean
    @Primary
    @StepScope
    public Rdf2LdProcessor delayedRdf2LdProcessor(
      @Value("#{stepExecution.jobExecution.id}") Long jobExecutionId,
      @Value("#{jobParameters['" + CONTENT_TYPE + "']}") String contentType,
      Rdf4LdService rdf4LdService,
      FailedRdfLineRepo failedRdfLineRepo
    ) {
      return new DelayedRdf2LdProcessor(jobExecutionId, contentType, rdf4LdService, failedRdfLineRepo);
    }
  }

  @Log4j2
  static class DelayedRdf2LdProcessor extends Rdf2LdProcessor {

    DelayedRdf2LdProcessor(Long jobExecutionId,
                                  String contentType,
                                  Rdf4LdService rdf4LdService,
                                  FailedRdfLineRepo failedRdfLineRepo) {
      super(jobExecutionId, contentType, rdf4LdService, failedRdfLineRepo);
    }

    @Override
    @Nullable
    @SuppressWarnings("java:S2925")
    public Set<ResourceWithLineNumber> process(@NonNull RdfLineWithNumber rdfLineWithNumber) {
      try {
        log.debug("Processing with delay {}ms for line #{}", DELAY_MS, rdfLineWithNumber.getLineNumber());
        Thread.sleep(DELAY_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("Processing interrupted for line #{}", rdfLineWithNumber.getLineNumber(), e);
      }
      return super.process(rdfLineWithNumber);
    }
  }
}
