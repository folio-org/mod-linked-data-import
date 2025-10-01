package org.folio.linked.data.imprt.service.rdf;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.doReturn;

import java.util.Optional;
import java.util.Set;
import org.folio.linked.data.imprt.integration.ld.LinkedDataService;
import org.folio.linked.data.imprt.integration.search.SearchService;
import org.folio.linked.data.imprt.integration.srs.SrsService;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class LccnResourceProviderTest {

  @InjectMocks
  private LccnResourceProvider lccnResourceProvider;
  @Mock
  private SrsService srsService;
  @Mock
  private SearchService searchService;
  @Mock
  private LinkedDataService linkedDataService;

  @Test
  void shouldReturnEmptyResult_ifSearchReturnsNoResultForLccn() {
    // given
    var lccn = "lccn";
    doReturn(Optional.empty()).when(searchService).searchForInventoryId(lccn);

    // when
    var result = lccnResourceProvider.apply(lccn);

    // then
    assertThat(result).isEmpty();
  }

  @Test
  void shouldReturnEmptyResult_ifLinkedDataAndSrsReturnNoResultForLccn() {
    // given
    var lccn = "lccn";
    var inventoryId = "inventoryId";
    doReturn(Optional.of(inventoryId)).when(searchService).searchForInventoryId(lccn);
    doReturn(Set.of()).when(linkedDataService).searchResources(Set.of(inventoryId));
    doReturn(Optional.empty()).when(srsService).fetchAuthorityFromSrs(inventoryId);

    // when
    var result = lccnResourceProvider.apply(lccn);

    // then
    assertThat(result).isEmpty();
  }

  @Test
  void shouldReturnResourceReturnedByLinkedData() {
    // given
    var lccn = "lccn";
    var inventoryId = "inventoryId";
    doReturn(Optional.of(inventoryId)).when(searchService).searchForInventoryId(lccn);
    var resource = new org.folio.ld.dictionary.model.Resource().setId(1L);
    doReturn(Set.of(resource)).when(linkedDataService).searchResources(Set.of(inventoryId));

    // when
    var result = lccnResourceProvider.apply(lccn);

    // then
    assertThat(result).contains(resource);
  }

  @Test
  void shouldReturnResourceReturnedBySrs_ifLinkedDataReturnsNoResult() {
    // given
    var lccn = "lccn";
    var inventoryId = "inventoryId";
    doReturn(Optional.of(inventoryId)).when(searchService).searchForInventoryId(lccn);
    doReturn(Set.of()).when(linkedDataService).searchResources(Set.of(inventoryId));
    var resource = new org.folio.ld.dictionary.model.Resource().setId(1L);
    doReturn(Optional.of(resource)).when(srsService).fetchAuthorityFromSrs(inventoryId);

    // when
    var result = lccnResourceProvider.apply(lccn);

    // then
    assertThat(result).contains(resource);
  }
}
