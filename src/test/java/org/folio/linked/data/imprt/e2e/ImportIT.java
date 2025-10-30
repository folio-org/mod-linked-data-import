package org.folio.linked.data.imprt.e2e;

import static org.assertj.core.api.AssertionsForClassTypes.fail;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.folio.ld.dictionary.PredicateDictionary.INSTANTIATES;
import static org.folio.ld.dictionary.PredicateDictionary.SUBJECT;
import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;
import static org.folio.linked.data.imprt.rest.resource.ImportStartApi.PATH_START_IMPORT;
import static org.folio.linked.data.imprt.test.TestUtil.awaitAndAssert;
import static org.folio.linked.data.imprt.test.TestUtil.defaultHeaders;
import static org.folio.linked.data.imprt.test.TestUtil.getTitleLabel;
import static org.folio.linked.data.imprt.test.TestUtil.validateInstanceWithTitles;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.File;
import java.util.List;
import java.util.stream.IntStream;
import org.folio.ld.dictionary.PredicateDictionary;
import org.folio.ld.dictionary.model.Resource;
import org.folio.ld.dictionary.model.ResourceEdge;
import org.folio.linked.data.imprt.test.IntegrationTest;
import org.folio.linked.data.imprt.test.KafkaOutputTopicTestListener;
import org.folio.s3.client.FolioS3Client;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.test.web.servlet.MockMvc;

@IntegrationTest
class ImportIT {

  @Autowired
  private Environment env;
  @Autowired
  private MockMvc mockMvc;
  @Autowired
  private FolioS3Client s3Client;
  @Autowired
  private KafkaOutputTopicTestListener outputTopicListener;

  @BeforeEach
  void clean() {
    outputTopicListener.getMessages().clear();
  }

  @Test
  void tenRecordsLocalImport_shouldProduce10Resources() throws Exception {
    // given
    var fileName = "10_records_json.rdf";
    var input = this.getClass().getResourceAsStream("/" + fileName);
    s3Client.write(fileName, input);
    var requestBuilder = post(PATH_START_IMPORT)
      .param("fileUrl", fileName)
      .headers(defaultHeaders(env));

    // when
    var resultActions = mockMvc.perform(requestBuilder);

    // then
    var result = resultActions.andExpect(status().isOk())
      .andReturn();
    assertThat(result.getResponse().getContentAsString()).isNotBlank();
    var allResources = outputTopicListener.getImportOutputMessagesResources(10);

    IntStream.range(0, 10)
      .forEach(i -> allResources.stream()
        .filter(r -> r.getLabel().equals(getTitleLabel("Title", i)))
        .findAny()
        .ifPresentOrElse(r -> validateInstanceWithTitles(r, i),
          () -> fail("Resource not found: " + getTitleLabel("Title", i)))
      );

    awaitAndAssert(() -> assertThat(new File(TMP_DIR, fileName)).doesNotExist());
  }

  @Test
  void recordsImportWithLccnResources_shouldProduceResourcesUsingSearchAndLinkedDataAndSrs() throws Exception {
    // given
    var fileName = "2_records_lccn_json.rdf";
    var input = this.getClass().getResourceAsStream("/" + fileName);
    s3Client.write(fileName, input);
    var requestBuilder = post(PATH_START_IMPORT)
      .param("fileUrl", fileName)
      .headers(defaultHeaders(env));

    // when
    var resultActions = mockMvc.perform(requestBuilder);

    // then
    var result = resultActions.andExpect(status().isOk())
      .andReturn();

    assertThat(result.getResponse().getContentAsString()).isNotBlank();
    var allResources = outputTopicListener.getImportOutputMessagesResources(2);

    IntStream.range(0, 1)
      .forEach(i -> allResources.stream()
        .filter(r -> r.getLabel().equals(getTitleLabel("Title", i)))
        .findAny()
        .ifPresentOrElse(r -> validateFetchedAuthority(r, i),
          () -> fail("Resource not found: " + getTitleLabel("Title", i)))
      );

    awaitAndAssert(() -> assertThat(new File(TMP_DIR, fileName)).doesNotExist());
  }

  private void validateFetchedAuthority(Resource instance, int number) {
    var works = getEdgeResources(instance, INSTANTIATES);
    assertThat(works).hasSize(1);
    var subjects = getEdgeResources(works.getFirst(), SUBJECT);
    assertThat(subjects).hasSize(1);
    assertThat(subjects.getFirst().getLabel())
      .isEqualTo(number == 0 ? "LCCN resource fetched from LD" : "Lccn resource fetched from SRS");
  }

  private List<Resource> getEdgeResources(Resource resource, PredicateDictionary predicate) {
    return resource.getOutgoingEdges()
      .stream()
      .filter(oe -> oe.getPredicate() == predicate)
      .map(ResourceEdge::getTarget)
      .toList();
  }

}
