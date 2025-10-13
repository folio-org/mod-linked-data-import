package org.folio.linked.data.imprt.test;

import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;
import static org.folio.linked.data.imprt.test.TestUtil.asJsonString;
import static org.folio.linked.data.imprt.test.TestUtil.defaultHeaders;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.folio.tenant.domain.dto.TenantAttributes;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;

@Log4j2
public class TenantInstallationExtension implements Extension, BeforeEachCallback {

  private static final String TENANT_ENDPOINT_URI = "/_/tenant";
  private static boolean init;

  @SneakyThrows
  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    if (!init) {
      var context = SpringExtension.getApplicationContext(extensionContext);
      var env = context.getEnvironment();
      var mockMvc = context.getBean(MockMvc.class);
      mockMvc.perform(post(TENANT_ENDPOINT_URI, TENANT_ID)
          .content(asJsonString(new TenantAttributes().moduleTo(env.getProperty("spring.application.name"))))
          .headers(defaultHeaders(env))
          .contentType(APPLICATION_JSON))
        .andExpect(status().isNoContent());
      init = true;
    }
  }

}
