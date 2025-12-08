package org.folio.linked.data.imprt.service.tenant;

import lombok.extern.log4j.Log4j2;
import org.folio.spring.FolioExecutionContext;
import org.folio.spring.liquibase.FolioSpringLiquibase;
import org.folio.spring.service.TenantService;
import org.folio.spring.tools.kafka.KafkaAdminService;
import org.folio.tenant.domain.dto.TenantAttributes;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Log4j2
@Primary
@Service
public class LinkedDataImportTenantService extends TenantService {

  private static final String MODULE_STATE = "module-state";
  private final KafkaAdminService kafkaAdminService;
  private final TenantScopedExecutionService tenantScopedExecutionService;

  public LinkedDataImportTenantService(JdbcTemplate jdbcTemplate,
                                       FolioExecutionContext context,
                                       FolioSpringLiquibase folioSpringLiquibase,
                                       KafkaAdminService kafkaAdminService,
                                       TenantScopedExecutionService tenantScopedExecutionService) {
    super(jdbcTemplate, context, folioSpringLiquibase);
    this.kafkaAdminService = kafkaAdminService;
    this.tenantScopedExecutionService = tenantScopedExecutionService;
  }

  @Override
  protected void afterTenantUpdate(TenantAttributes tenantAttributes) {
    log.info("Creating kafka topics for tenant {}", context.getTenantId());
    kafkaAdminService.createTopics(context.getTenantId());
    kafkaAdminService.restartEventListeners();
  }

  @Override
  protected void afterTenantDeletion(TenantAttributes tenantAttributes) {
    kafkaAdminService.deleteTopics(context.getTenantId());
  }

  @Cacheable(cacheNames = MODULE_STATE)
  public boolean isTenantExists(String tenantId) {
    return tenantScopedExecutionService.execute(tenantId, super::tenantExists);
  }
}
