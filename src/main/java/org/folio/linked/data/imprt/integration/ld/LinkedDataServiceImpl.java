package org.folio.linked.data.imprt.integration.ld;

import static java.util.Optional.ofNullable;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.domain.dto.SearchResourcesRequestDto;
import org.folio.linked.data.imprt.integration.client.LinkedDataClient;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@RequiredArgsConstructor
public class LinkedDataServiceImpl implements LinkedDataService {

  private final ObjectMapper objectMapper;
  private final LinkedDataClient linkedDataClient;

  @Override
  public Set<Resource> searchResources(Set<String> inventoryIds) {
    return ofNullable(linkedDataClient.searchResources(new SearchResourcesRequestDto(inventoryIds))
      .getBody())
      .stream()
      .flatMap(Collection::stream)
      .collect(Collectors.toSet());
  }

}
