package org.folio.linked.data.imprt.batch.job.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;

import jakarta.persistence.EntityManagerFactory;
import org.folio.linked.data.imprt.model.entity.RdfFileLine;
import org.folio.linked.data.imprt.repo.RdfFileLineRepo;
import org.folio.linked.data.imprt.service.tenant.TenantScopedExecutionService;
import org.folio.linked.data.imprt.test.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

@IntegrationTest
class DatabaseRdfLineItemReaderTest {

  @Autowired
  private EntityManagerFactory entityManagerFactory;

  @Autowired
  private RdfFileLineRepo rdfFileLineRepo;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private TenantScopedExecutionService tenantScopedExecutionService;

  private Long jobExecutionId;

  @BeforeEach
  void setUp() {
    jobExecutionId = 999L;
    tenantScopedExecutionService.execute(TENANT_ID, () -> {
      jdbcTemplate.execute("DELETE FROM rdf_file_line");

      var line1 = new RdfFileLine();
      line1.setJobExecutionId(jobExecutionId);
      line1.setLineNumber(1L);
      line1.setContent("Test content 1");
      rdfFileLineRepo.save(line1);

      var line2 = new RdfFileLine();
      line2.setJobExecutionId(jobExecutionId);
      line2.setLineNumber(2L);
      line2.setContent("Test content 2");
      rdfFileLineRepo.save(line2);

      var line3 = new RdfFileLine();
      line3.setJobExecutionId(jobExecutionId);
      line3.setLineNumber(3L);
      line3.setContent("Test content 3");
      rdfFileLineRepo.save(line3);

      var otherJobLine = new RdfFileLine();
      otherJobLine.setJobExecutionId(888L);
      otherJobLine.setLineNumber(1L);
      otherJobLine.setContent("Other job content");
      rdfFileLineRepo.save(otherJobLine);
    });
  }

  @Test
  void read_shouldReturnRdfLinesForJobExecutionId() {
    // given
    var reader = new DatabaseRdfLineItemReader(jobExecutionId, entityManagerFactory, 1000);
    var executionContext = new ExecutionContext();

    // when
    tenantScopedExecutionService.execute(TENANT_ID, () -> {
      reader.open(executionContext);

      var result1 = reader.read();
      var result2 = reader.read();
      var result3 = reader.read();
      var result4 = reader.read();

      // then
      assertThat(result1).isNotNull();
      assertThat(result1.getLineNumber()).isEqualTo(1L);
      assertThat(result1.getContent()).isEqualTo("Test content 1");

      assertThat(result2).isNotNull();
      assertThat(result2.getLineNumber()).isEqualTo(2L);
      assertThat(result2.getContent()).isEqualTo("Test content 2");

      assertThat(result3).isNotNull();
      assertThat(result3.getLineNumber()).isEqualTo(3L);
      assertThat(result3.getContent()).isEqualTo("Test content 3");

      assertThat(result4).isNull();

      reader.close();
      return null;
    });
  }

  @Test
  void read_shouldReturnOnlyLinesForSpecificJobExecutionId() {
    // given
    var reader = new DatabaseRdfLineItemReader(jobExecutionId, entityManagerFactory, 1000);
    var executionContext = new ExecutionContext();

    // when
    tenantScopedExecutionService.execute(TENANT_ID, () -> {
      reader.open(executionContext);

      var result1 = reader.read();
      var result2 = reader.read();
      var result3 = reader.read();
      var result4 = reader.read();

      // then
      assertThat(result1).isNotNull();
      assertThat(result2).isNotNull();
      assertThat(result3).isNotNull();
      assertThat(result4).isNull();

      reader.close();
      return null;
    });
  }

  @Test
  void read_shouldReturnNullWhenNoLinesExist() {
    // given
    var emptyJobExecutionId = 777L;
    var reader = new DatabaseRdfLineItemReader(emptyJobExecutionId, entityManagerFactory, 1000);
    var executionContext = new ExecutionContext();

    // when
    tenantScopedExecutionService.execute(TENANT_ID, () -> {
      reader.open(executionContext);

      var result = reader.read();

      // then
      assertThat(result).isNull();

      reader.close();
      return null;
    });
  }
}
