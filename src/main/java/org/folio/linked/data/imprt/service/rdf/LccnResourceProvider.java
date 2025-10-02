package org.folio.linked.data.imprt.service.rdf;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.integration.ld.LinkedDataService;
import org.folio.linked.data.imprt.integration.search.SearchService;
import org.folio.linked.data.imprt.integration.srs.SrsService;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@RequiredArgsConstructor
public class LccnResourceProvider implements Function<String, Optional<Resource>> {

  private final SrsService srsService;
  private final SearchService searchService;
  private final LinkedDataService linkedDataService;

  @Override
  public Optional<Resource> apply(String lccn) {
    return searchService.searchForInventoryId(lccn).flatMap(inventoryId -> {
      log.info("InventoryId has been found for LCCN {}: {}", lccn, inventoryId);
      return linkedDataService.searchResources(Set.of(inventoryId)).stream().findFirst().or(() -> {
        log.info("Resource not found in LD for inventoryId {}, fetching from SRS", inventoryId);
        return srsService.fetchAuthorityFromSrs(inventoryId);
      });
    });
  }

}
