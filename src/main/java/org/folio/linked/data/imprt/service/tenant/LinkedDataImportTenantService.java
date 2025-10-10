package org.folio.linked.data.imprt.service.tenant;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.folio.spring.FolioExecutionContext;
import org.folio.spring.exception.FolioContextExecutionException;
import org.folio.spring.liquibase.FolioSpringLiquibase;
import org.folio.spring.service.TenantService;
import org.folio.spring.tools.kafka.KafkaAdminService;
import org.folio.tenant.domain.dto.TenantAttributes;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Log4j2
@Primary
@Service
public class LinkedDataImportTenantService extends TenantService {

  private final KafkaAdminService kafkaAdminService;

  public LinkedDataImportTenantService(JdbcTemplate jdbcTemplate,
                                       FolioExecutionContext context,
                                       FolioSpringLiquibase folioSpringLiquibase,
                                       KafkaAdminService kafkaAdminService) {
    super(jdbcTemplate, context, folioSpringLiquibase);
    this.kafkaAdminService = kafkaAdminService;
  }

  @Override
  protected void afterTenantUpdate(TenantAttributes tenantAttributes) {
    log.info("Creating kafka topics for tenant {}", context.getTenantId());
    kafkaAdminService.createTopics(context.getTenantId());
    kafkaAdminService.restartEventListeners();
  }

  @Override
  protected void afterLiquibaseUpdate(TenantAttributes tenantAttributes) {
    log.info("Creating Spring Batch DB tables for tenant {}", context.getTenantId());
    jdbcTemplate.execute(readSpringBatchSchemaSql());
  }

  private String readSpringBatchSchemaSql() {
    var resource = new ClassPathResource("org/springframework/batch/core/schema-postgresql.sql");
    try (var reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
      return reader.lines().collect(Collectors.joining("\n"));
    } catch (Exception e) {
      throw new FolioContextExecutionException("Error reading Spring Batch SQL file", e);
    }
  }
}
