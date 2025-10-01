package org.folio.linked.data.imprt.integration.search;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.doReturn;

import java.util.List;
import org.folio.linked.data.imprt.domain.dto.AuthorityItem;
import org.folio.linked.data.imprt.domain.dto.AuthoritySearchResponse;
import org.folio.linked.data.imprt.integration.client.SearchClient;
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
class SearchServiceTest {

  @InjectMocks
  private SearchServiceImpl searchService;
  @Mock
  private SearchClient searchClient;

  @Test
  void shouldReturnEmptyResult_ifSearchClientReturnsEmptyResult() {
    // given
    var lccn = "lccn";
    var searchResponse = new ResponseEntity<>(new AuthoritySearchResponse(), HttpStatus.OK);
    doReturn(searchResponse).when(searchClient).searchAuthorities("lccn = " + lccn);

    // when
    var result = searchService.searchForInventoryId(lccn);

    // then
    assertThat(result).isEmpty();
  }

  @Test
  void shouldReturnInventoryId_ifSearchClientReturnsResult() {
    // given
    var lccn = "lccn";
    var inventoryId = "1";
    var authorities = List.of(new AuthorityItem().id(inventoryId).naturalId("2"));
    var searchResponse = new ResponseEntity<>(new AuthoritySearchResponse().authorities(authorities), HttpStatus.OK);
    doReturn(searchResponse).when(searchClient).searchAuthorities("lccn = " + lccn);

    // when
    var result = searchService.searchForInventoryId(lccn);

    // then
    assertThat(result).contains(inventoryId);
  }
}
