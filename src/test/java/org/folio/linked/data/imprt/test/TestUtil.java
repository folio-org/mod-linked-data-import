package org.folio.linked.data.imprt.test;

import static java.lang.System.getProperty;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.folio.ld.dictionary.PropertyDictionary.DATE;
import static org.folio.ld.dictionary.PropertyDictionary.MAIN_TITLE;
import static org.folio.ld.dictionary.PropertyDictionary.NON_SORT_NUM;
import static org.folio.ld.dictionary.PropertyDictionary.NOTE;
import static org.folio.ld.dictionary.PropertyDictionary.PART_NAME;
import static org.folio.ld.dictionary.PropertyDictionary.PART_NUMBER;
import static org.folio.ld.dictionary.PropertyDictionary.SUBTITLE;
import static org.folio.ld.dictionary.PropertyDictionary.VARIANT_TYPE;
import static org.folio.ld.dictionary.ResourceTypeDictionary.PARALLEL_TITLE;
import static org.folio.ld.dictionary.ResourceTypeDictionary.VARIANT_TITLE;
import static org.folio.spring.integration.XOkapiHeaders.TENANT;
import static org.folio.spring.integration.XOkapiHeaders.URL;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.awaitility.Durations.FIVE_SECONDS;
import static org.testcontainers.shaded.org.awaitility.Durations.ONE_HUNDRED_MILLISECONDS;
import static org.testcontainers.shaded.org.awaitility.Durations.TWO_MINUTES;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.folio.ld.dictionary.PredicateDictionary;
import org.folio.ld.dictionary.PropertyDictionary;
import org.folio.ld.dictionary.ResourceTypeDictionary;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.config.ObjectMapperConfig;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.testcontainers.shaded.org.awaitility.core.ThrowingRunnable;

@UtilityClass
public class TestUtil {

  public static final String TENANT_ID = "test_tenant";
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapperConfig().objectMapper();
  private static final String FOLIO_OKAPI_URL = "folio.okapi-url";

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

  public static void awaitAndAssert(ThrowingRunnable throwingRunnable) {
    await().atMost(TWO_MINUTES)
      .pollDelay(FIVE_SECONDS)
      .pollInterval(ONE_HUNDRED_MILLISECONDS)
      .untilAsserted(throwingRunnable);
  }

  public static void validateInstanceWithTitles(Resource resource, int number) {
    assertThat(resource.getId()).isNotNull();
    assertThat(resource.getLabel()).isEqualTo(getTitleLabel("Title", number));
    assertThat(resource.getIncomingEdges()).isEmpty();
    validateTitleEdge(resource, Set.of(ResourceTypeDictionary.TITLE),
      Map.of(
        MAIN_TITLE, List.of("Title mainTitle 1" + number, "Title mainTitle 2" + number),
        PART_NAME, List.of("Title partName 1" + number, "Title partName 2" + number),
        PART_NUMBER, List.of("Title partNumber 1" + number, "Title partNumber 2" + number),
        SUBTITLE, List.of("Title subTitle 1" + number, "Title subTitle 2" + number),
        NON_SORT_NUM, List.of("Title nonSortNum 1" + number, "Title nonSortNum 2" + number)
      ), getTitleLabel("Title", number)
    );
    validateTitleEdge(resource, Set.of(PARALLEL_TITLE),
      Map.of(
        MAIN_TITLE, List.of("ParallelTitle mainTitle 1" + number, "ParallelTitle mainTitle 2" + number),
        PART_NAME, List.of("ParallelTitle partName 1" + number, "ParallelTitle partName 2" + number),
        PART_NUMBER, List.of("ParallelTitle partNumber 1" + number, "ParallelTitle partNumber 2" + number),
        SUBTITLE, List.of("ParallelTitle subTitle 1" + number, "ParallelTitle subTitle 2" + number),
        DATE, List.of("ParallelTitle date 1" + number, "ParallelTitle date 2" + number),
        NOTE, List.of("ParallelTitle note 1" + number, "ParallelTitle note 2" + number)
      ), getTitleLabel("ParallelTitle", number)
    );
    validateTitleEdge(resource, Set.of(VARIANT_TITLE),
      Map.of(
        MAIN_TITLE, List.of("VariantTitle mainTitle 1" + number, "VariantTitle mainTitle 2" + number),
        PART_NAME, List.of("VariantTitle partName 1" + number, "VariantTitle partName 2" + number),
        PART_NUMBER, List.of("VariantTitle partNumber 1" + number, "VariantTitle partNumber 2" + number),
        SUBTITLE, List.of("VariantTitle subTitle 1" + number, "VariantTitle subTitle 2" + number),
        DATE, List.of("VariantTitle date 1" + number, "VariantTitle date 2" + number),
        NOTE, List.of("VariantTitle note 1" + number, "VariantTitle note 2" + number),
        VARIANT_TYPE, List.of("0")
      ), getTitleLabel("VariantTitle", number)
    );
  }

  public static void validateTitleEdge(Resource parentResource,
                                       Set<ResourceTypeDictionary> expectedTypeSet,
                                       Map<PropertyDictionary, List<String>> expectedProperties,
                                       String expectedLabel) {
    var edges = parentResource.getOutgoingEdges()
      .stream()
      .filter(e -> PredicateDictionary.TITLE.equals(e.getPredicate())
        && expectedTypeSet.equals(e.getTarget().getTypes())
        && expectedLabel.equals(e.getTarget().getLabel()))
      .collect(Collectors.toSet());
    assertThat(edges).hasSize(1);
    var edge = edges.iterator().next();
    assertThat(edge.getId()).isNull();
    assertThat(edge.getSource()).isEqualTo(parentResource);
    var resource = edge.getTarget();
    assertThat(resource.getId()).isNotNull();
    expectedProperties.forEach((key, value) -> validateProperty(resource.getDoc(), key.getValue(), value));
  }

  public static void validateProperty(JsonNode doc, String property, List<String> expected) {
    assertThat(doc.has(property)).isTrue();
    assertThat(doc.get(property).size()).isEqualTo(expected.size());
    for (int i = 0; i < expected.size(); i++) {
      assertThat(doc.get(property).get(i).asText()).isEqualTo(expected.get(i));
    }
  }

  public static String getTitleLabel(String titleType, int number) {
    return titleType + " mainTitle 1" + number + ", " + titleType + " mainTitle 2" + number + ", "
      + titleType + " subTitle 1" + number + ", " + titleType + " subTitle 2" + number;
  }

}
