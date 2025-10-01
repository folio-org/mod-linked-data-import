package org.folio.linked.data.imprt.integration.srs;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.folio.linked.data.imprt.integration.client.SrsClient;
import org.folio.marc4ld.service.marc2ld.authority.MarcAuthority2ldMapper;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@UnitTest
@ExtendWith(MockitoExtension.class)
class SrsServiceTest {

  @InjectMocks
  private SrsServiceImpl srsService;
  @Mock
  private SrsClient srsClient;
  @Mock
  private ObjectMapper objectMapper;
  @Mock
  private MarcAuthority2ldMapper marcAuthority2ldMapper;

  @Test
  void shouldReturnNoResult_ifSrsClientReturnsNoResult() {
    // given
    var inventoryId = "inventoryId";
    var srsResponse = new ResponseEntity<Record>(HttpStatus.OK);
    doReturn(srsResponse).when(srsClient).getAuthorityByInventoryId(inventoryId);

    // when
    var result = srsService.fetchAuthorityFromSrs(inventoryId);

    // then
    assertThat(result).isEmpty();
  }

  @Test
  void shouldReturnNoResult_ifSrsClientReturnsNotReadableResult() throws JsonProcessingException {
    // given
    var inventoryId = "inventoryId";
    var content = "content";
    var srsRecord = new Record().withParsedRecord(new ParsedRecord().withContent(content));
    var srsResponse = new ResponseEntity<>(srsRecord, HttpStatus.OK);
    doReturn(srsResponse).when(srsClient).getAuthorityByInventoryId(inventoryId);
    doThrow(new JsonParseException("error")).when(objectMapper).writeValueAsString(content);

    // when
    var result = srsService.fetchAuthorityFromSrs(inventoryId);

    // then
    assertThat(result).isEmpty();
  }

  @Test
  void shouldReturnNoResult_ifMarc2LdReturnsEmptyResult() throws JsonProcessingException {
    // given
    var inventoryId = "inventoryId";
    var content = "content";
    var srsRecord = new Record().withParsedRecord(new ParsedRecord().withContent(content));
    var srsResponse = new ResponseEntity<>(srsRecord, HttpStatus.OK);
    doReturn(srsResponse).when(srsClient).getAuthorityByInventoryId(inventoryId);
    var jsonContent = "jsonContent";
    doReturn(jsonContent).when(objectMapper).writeValueAsString(content);
    doReturn(emptyList()).when(marcAuthority2ldMapper).fromMarcJson(jsonContent);

    // when
    var result = srsService.fetchAuthorityFromSrs(inventoryId);

    // then
    assertThat(result).isEmpty();
  }

  @Test
  void shouldReturnResult_ifSrsResultMappedToLdResource() throws JsonProcessingException {
    // given
    var inventoryId = "inventoryId";
    var content = "content";
    var srsRecord = new Record().withParsedRecord(new ParsedRecord().withContent(content));
    var srsResponse = new ResponseEntity<>(srsRecord, HttpStatus.OK);
    doReturn(srsResponse).when(srsClient).getAuthorityByInventoryId(inventoryId);
    var jsonContent = "jsonContent";
    doReturn(jsonContent).when(objectMapper).writeValueAsString(content);
    var resource = new org.folio.ld.dictionary.model.Resource().setId(1L);
    doReturn(List.of(resource)).when(marcAuthority2ldMapper).fromMarcJson(jsonContent);

    // when
    var result = srsService.fetchAuthorityFromSrs(inventoryId);

    // then
    assertThat(result).contains(resource);
  }
}
