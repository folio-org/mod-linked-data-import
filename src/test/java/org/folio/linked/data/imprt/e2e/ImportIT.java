package org.folio.linked.data.imprt.e2e;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.folio.linked.data.imprt.rest.resource.ImportStartApi.PATH_START_IMPORT;
import static org.folio.linked.data.imprt.test.TestUtil.defaultHeaders;
import static org.folio.linked.data.imprt.test.TestUtil.validateInstanceWithTitles;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.folio.linked.data.imprt.test.IntegrationTest;
import org.folio.linked.data.imprt.test.KafkaOutputTopicTestListener;
import org.folio.s3.client.FolioS3Client;
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

  @Test
  void tenRecordsLocalImport_shouldEndWith4MessagesWith3ResourcesChunks() throws Exception {
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
    resultActions
      .andExpect(status().isAccepted());
    var messages = outputTopicListener.readImportResultMessages(4);

    var message1 = messages.getFirst();
    assertThat(message1.getResources()).hasSize(3);
    validateInstanceWithTitles(message1.getResources().getFirst(), 0);
    validateInstanceWithTitles(message1.getResources().get(1), 1);
    validateInstanceWithTitles(message1.getResources().getLast(), 2);

    var message2 = messages.get(1);
    assertThat(message2.getResources()).hasSize(3);
    validateInstanceWithTitles(message2.getResources().getFirst(), 3);
    validateInstanceWithTitles(message2.getResources().get(1), 4);
    validateInstanceWithTitles(message2.getResources().getLast(), 5);

    var message3 = messages.get(2);
    assertThat(message3.getResources()).hasSize(3);
    validateInstanceWithTitles(message3.getResources().getFirst(), 6);
    validateInstanceWithTitles(message3.getResources().get(1), 7);
    validateInstanceWithTitles(message3.getResources().getLast(), 8);

    var message4 = messages.getLast();
    assertThat(message4.getResources()).hasSize(1);
    validateInstanceWithTitles(message4.getResources().getFirst(), 9);
  }

}
