package org.folio.linked.data.imprt.test;

import static java.lang.System.getProperty;
import static org.folio.spring.integration.XOkapiHeaders.TENANT;
import static org.folio.spring.integration.XOkapiHeaders.URL;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;

@UtilityClass
public class TestUtil {

  public static final String TENANT_ID = "test_tenant";
  private static final String FOLIO_OKAPI_URL = "folio.okapi-url";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @SneakyThrows
  public static String asJsonString(Object value) {
    return OBJECT_MAPPER.writeValueAsString(value);
  }

  public static HttpHeaders defaultHeaders(Environment env) {
    var httpHeaders = new HttpHeaders();
    httpHeaders.add(TENANT, TENANT_ID);
    httpHeaders.add(URL, getProperty(FOLIO_OKAPI_URL));
    return httpHeaders;
  }

}
