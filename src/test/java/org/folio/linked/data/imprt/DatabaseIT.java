package org.folio.linked.data.imprt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;

import java.util.List;
import org.folio.linked.data.imprt.test.IntegrationTest;
import org.folio.spring.tools.kafka.KafkaAdminService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

@IntegrationTest
class DatabaseIT {

  private static final List<String> EXPECTED_TABLES = List.of(
    "databasechangelog",
    "databasechangeloglock",
    "batch_job_instance",
    "batch_job_execution",
    "batch_job_execution_params",
    "batch_step_execution",
    "batch_step_execution_context",
    "batch_job_execution_context",
    "failed_rdf_line",
    "import_result_event",
    "rdf_file_line"
  );
  private static final String LIST_TABLES_QUERY = """
          SELECT table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE' AND table_schema = ?
    """;
  @Value("${spring.application.name}")
  private String appName;

  @Autowired
  private JdbcTemplate jdbcTemplate;
  @MockitoSpyBean
  private KafkaAdminService kafkaAdminService;

  @Test
  void testTablesCreated() {
    // given
    var schema = TENANT_ID + "_" + appName.replace('-', '_');

    // when
    var tables = jdbcTemplate.queryForList(LIST_TABLES_QUERY, String.class, schema);

    // then
    assertThat(tables).containsOnly(EXPECTED_TABLES.toArray(new String[]{}));
  }

  @Test
  void testNoTablesCreatedInPublicSchema() {
    // when
    var tables = jdbcTemplate.queryForList(LIST_TABLES_QUERY, String.class, "public");

    // then
    assertThat(tables).isEmpty();
  }

}
