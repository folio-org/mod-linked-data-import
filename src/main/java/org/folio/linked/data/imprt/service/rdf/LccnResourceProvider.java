package org.folio.linked.data.imprt.service.rdf;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.integration.ld.LinkedDataService;
import org.folio.linked.data.imprt.integration.search.SearchService;
import org.folio.linked.data.imprt.integration.srs.SrsService;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LccnResourceProvider implements Function<String, Optional<Resource>> {

  private final SrsService srsService;
  private final SearchService searchService;
  private final LinkedDataService linkedDataService;

  @Override
  public Optional<Resource> apply(String lccn) {
    return searchService.searchForInventoryId(lccn)
      .flatMap(inventoryId ->
        linkedDataService.searchResources(Set.of(inventoryId))
          .stream()
          .findFirst()
          .or(() -> srsService.fetchAuthorityFromSrs(inventoryId))
      );
  }

}
