package org.folio.linked.data.imprt.service.tenant;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import org.folio.spring.FolioExecutionContext;
import org.folio.spring.exception.FolioContextExecutionException;
import org.folio.spring.liquibase.FolioSpringLiquibase;
import org.folio.spring.service.TenantService;
import org.folio.tenant.domain.dto.TenantAttributes;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Primary
@Service
public class LinkedDataImportTenantService extends TenantService {

  public LinkedDataImportTenantService(JdbcTemplate jdbcTemplate,
                                       FolioExecutionContext context,
                                       FolioSpringLiquibase folioSpringLiquibase) {
    super(jdbcTemplate, context, folioSpringLiquibase);
  }

  @Override
  protected void afterLiquibaseUpdate(TenantAttributes tenantAttributes) {
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
