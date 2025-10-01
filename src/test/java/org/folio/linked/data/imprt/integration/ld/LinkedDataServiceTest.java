package org.folio.linked.data.imprt.integration.ld;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.domain.dto.SearchResourcesRequestDto;
import org.folio.linked.data.imprt.integration.client.LinkedDataClient;
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
class LinkedDataServiceTest {

  @InjectMocks
  private LinkedDataServiceImpl linkedDataService;
  @Mock
  private ObjectMapper objectMapper;
  @Mock
  private LinkedDataClient linkedDataClient;

  @Test
  void shouldReturnEmptyResult_ifLinkedDataClientReturnsEmptyResult() {
    // given
    var ldResponse = new ResponseEntity<Set<String>>(Set.of(), HttpStatus.OK);
    doReturn(ldResponse).when(linkedDataClient).searchResources(new SearchResourcesRequestDto(Set.of("inventoryId")));

    // when
    var result = linkedDataService.searchResources(Set.of("inventoryId"));

    // then
    assertThat(result).isEmpty();
  }

  @Test
  void shouldReturnEmptyResult_ifLinkedDataClientReturnsNotReadableResult() throws JsonProcessingException {
    // given
    var ldResponseBody = "abc";
    var ldResponse = new ResponseEntity<>(Set.of(ldResponseBody), HttpStatus.OK);
    doReturn(ldResponse).when(linkedDataClient).searchResources(new SearchResourcesRequestDto(Set.of("inventoryId")));
    doThrow(new JsonParseException("error")).when(objectMapper).readValue(ldResponseBody, Resource.class);

    // when
    var result = linkedDataService.searchResources(Set.of("inventoryId"));

    // then
    assertThat(result).isEmpty();
  }

  @Test
  void shouldReturnResult_ifLinkedDataClientReturnsResult() throws JsonProcessingException {
    // given
    var ldResponseBody = "abc";
    var ldResponse = new ResponseEntity<>(Set.of(ldResponseBody), HttpStatus.OK);
    doReturn(ldResponse).when(linkedDataClient).searchResources(new SearchResourcesRequestDto(Set.of("inventoryId")));
    var resource = new Resource().setId(1L);
    doReturn(resource).when(objectMapper).readValue(ldResponseBody, Resource.class);

    // when
    var result = linkedDataService.searchResources(Set.of("inventoryId"));

    // then
    assertThat(result).containsOnly(resource);
  }
}
